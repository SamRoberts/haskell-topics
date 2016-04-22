{-# LANGUAGE GADTs, RecordWildCards, TupleSections #-}

-- | An in-memory interpreter for Topics programs
module Network.Topics.Memory where
    -- FIXME: want a qualified export list at some point, but easier to play around without one
    -- maybe better to define non-exported items in an Internal module, rather than hide them?

import qualified Control.Monad.Operational as Op
import           Control.Monad.Trans.State.Strict (State)
import qualified Control.Monad.Trans.State.Strict as St

import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.Foldable (toList)
import qualified Data.List as L
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.Monoid ((<>))
import           Data.Sequence (Seq)
import qualified Data.Sequence as S
import qualified Data.Serialize.Get as Get
import qualified Data.Serialize.Put as Put

import qualified Network.Kafka.Protocol as KP

import           Network.Topics

newtype KafkaState = KafkaState { kafkaState :: (Map TopicName (Seq KafkaPartition)) }
data KafkaPartition = KafkaPartition
                    { partFirst :: Offset
                    , partItems :: Seq ByteString
                    }

partLast :: KafkaPartition -> Offset
partLast partition = partNext partition - 1

partNext :: KafkaPartition -> Offset
partNext KafkaPartition {..} = partFirst + fromIntegral (S.length partItems)

mkTopic :: TopicName -> Seq KafkaPartition -> Topic
mkTopic topicName partitions = Topic topicName (fromIntegral $ length partitions - 1)

runTopicsInMemory :: Topics a -> State KafkaState a
runTopicsInMemory = Op.interpretWithMonad eval
  where
    eval :: Instruction a -> State KafkaState a
    eval (GetTopic topicName) =
      St.gets (fmap (mkTopic topicName) . M.lookup topicName . kafkaState)

    eval (GetOffsets request) =
      withPartitions request (\() () oldPartition @ KafkaPartition {..} ->
        ( Right (partFirst, partLast oldPartition)
        , Nothing
        ))

    eval (Produce request) =
      withPartitions request (\_ values oldPartition @ KafkaPartition {..} ->
        ( Right (partNext oldPartition)
          -- TODO consider implementing a max number of messages in partition
        , Just (oldPartition { partItems = partItems <> S.fromList (map (Put.runPut . KP.serialize) values) })
        ))

    eval (Fetch request) =
      -- NOTE: The in-memory implementation of Fetch has some different behaviour to normal Kafka,
      --       well, in addition to the usual differences:
      --
      --       1. Kafka may wait for enough data to fill up min bytes. But this implementation is
      --          single-threaded and synchronous, so waiting does not good. We ignore min bytes.
      --       2. Fetch specifies the max number of bytes to send back in the response. But we don't serialize
      --          a proper Kafka response, so we don't know what this figure is. We use the total serialized
      --          size of each message as a reasonable proxy for this figure.
      withPartitions request (\FetchConfig {..} FetchInfo {..} oldPartition @ KafkaPartition {..} ->
        let serializedStream = toList (S.drop (fromIntegral (fetchOffset - partFirst)) partItems)
            totalSizes = scanl (\size serializedValue -> size + fromIntegral (B.length serializedValue)) 0 serializedStream
            serializedValues = (map snd . takeWhile ((<= fetchMaxBytes) . fst) . zip totalSizes) serializedStream
            deserializedValues = traverse (Get.runGet KP.deserialize) serializedValues
        in  ( case deserializedValues of
                Right values -> Right (FetchData (partLast oldPartition) (zip [fetchOffset..] values))
                Left  _      -> Left unknown
            , Nothing
            ))

withPartitions :: IncludesCommonErrors e => Request tc pc -> (tc -> pc -> KafkaPartition -> (Either e a, Maybe KafkaPartition)) -> State KafkaState (Response e a)
withPartitions Request {..} onPartition =
  St.state (\KafkaState {..} -> case M.lookup (topicName reqTopic) kafkaState of
    Nothing         -> (noTopicResponse, KafkaState kafkaState)
    Just partitions -> let (newPartitions, results) = L.mapAccumL partitionAction partitions reqParts
                       in  (Response reqTopic results, KafkaState (M.insert (topicName reqTopic) newPartitions kafkaState)))
  where
    noTopicResponse = Response reqTopic (map (\(ix,_) -> (ix, unknownTopicOrPartition)) reqParts)

    partitionAction partitions (partitionIx, partitionConf) =
      let ix = fromIntegral partitionIx in case seqLookup ix partitions of
        Nothing -> (partitions, (partitionIx, unknownTopicOrPartition))
        Just partition ->
          let (result, maybeNewPartition) = onPartition reqConf partitionConf partition
              newPartitions = maybe partitions (\newPartition -> S.update ix newPartition partitions) maybeNewPartition
          in  (newPartitions, (partitionIx, result))

class IncludesCommonErrors e where
  unknown :: e
  unknownTopicOrPartition :: e
  notLeaderForPartition :: e

instance IncludesCommonErrors CommonError where
  unknown = Unknown
  unknownTopicOrPartition = UnknownTopicOrPartition
  notLeaderForPartition = NotLeaderForPartition

instance IncludesCommonErrors l => IncludesCommonErrors (Either l r) where
  unknown = Left unknown
  unknownTopicOrPartition = Left unknownTopicOrPartition
  notLeaderForPartition = Left notLeaderForPartition

seqLookup :: Int -> Seq a -> Maybe a
seqLookup i xs =
  if i >= 0 && i < S.length xs then Just (S.index xs i) else Nothing
