{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}

-- | An in-memory synchronous interpreter for Topics programs
module Network.Topics.Sync where
    -- FIXME: want a qualified export list at some point, but easier to play around without one
    -- maybe better to define non-exported items in an Internal module, rather than hide them?

import           Control.Arrow ((&&&))
import           Control.Monad.State.Class (MonadState)
import qualified Control.Monad.State.Class as St
import           Control.Monad.Trans.State.Strict (State, runState)

import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.Foldable (toList)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.Maybe (catMaybes, fromJust)
import           Data.Monoid ((<>))
import           Data.Sequence (Seq)
import qualified Data.Sequence as S
import qualified Data.Serialize.Get as Get
import qualified Data.Serialize.Put as Put
import           Data.Typeable (TypeRep)
import qualified Data.Typeable as T

import qualified Network.Kafka.Protocol as KP

import           Network.Topics

runSyncTopics :: SyncTopics a -> KafkaState -> (a, KafkaState)
runSyncTopics = runState . syncTopics

newtype SyncTopics a = SyncTopics { syncTopics :: State KafkaState a } deriving (Functor, Applicative, Monad, MonadState KafkaState)
newtype SyncPartition v a = SyncPartition { syncPartition :: State KafkaPartition a } deriving (Functor, Applicative, Monad, MonadState KafkaPartition)

newtype KafkaState = KafkaState { kafkaState :: Map TopicName (TypeRep, Seq KafkaPartition) }
data KafkaPartition = KafkaPartition
                    { partFirst :: Offset
                    , partItems :: Seq ByteString
                    }

partLast :: KafkaPartition -> Offset
partLast partition = partNext partition - 1

partNext :: KafkaPartition -> Offset
partNext KafkaPartition {..} = partFirst + fromIntegral (S.length partItems)

instance Topics SyncTopics SyncPartition where
  getTopic :: forall v. Kafkaesque v => TopicName -> SyncTopics (Topic v)
  getTopic name =
    SyncTopics (St.gets (mkTopic . fromJust . M.lookup name . kafkaState))
    where
      mkTopic (typeRef, partitions) =
        if (typeRef /= T.typeOf (undefined :: v))
        then error "unexpected type"
        else Topic { topicName         = name
                   , topicMaxPartition = fromIntegral (S.length partitions - 1)
                   }

  withPartitions :: Kafkaesque v => Topic v -> [PartitionId] -> SyncPartition v a -> SyncTopics [a]
  withPartitions Topic {..} pids action = do
    -- for the moment, naively assume that our topic is still valid, as it must have been valid when created
    (typeRef, partitions) <- St.gets (fromJust . M.lookup topicName . kafkaState)
    let (results, newPartitions) = (unzip . toList . S.mapWithIndex perPartition) partitions
    St.modify (\KafkaState {..} -> KafkaState (M.insert topicName (typeRef, S.fromList newPartitions) kafkaState))
    return (catMaybes results)
    where
      perPartition i = if fromIntegral i `elem` pids
                       then (runState . syncPartition . fmap Just) action
                       else (Nothing,)

instance Kafkaesque v => Partition SyncPartition v where
  getOffsets :: SyncPartition v (Offset, Offset)
  getOffsets = fmap (partFirst &&& partLast) St.get

  produce :: [v] -> SyncPartition v Offset
  produce newItems = do
    partition <- St.get
    -- TODO consider implementing a max number of messages in partition
    let newMsgs = S.fromList (map (Put.runPut . KP.serialize) newItems)
    St.put (partition { partItems = partItems partition <> newMsgs })
    return (partNext partition)

  fetch :: Offset -> Size -> SyncPartition v (FetchData v)
  fetch offset maxBytes = do
    partition @ KafkaPartition {..} <- St.get
    let serializedStream   = toList (S.drop (fromIntegral (offset - partFirst)) partItems)
        totalSizes         = scanl (\size serializedValue -> size + fromIntegral (B.length serializedValue)) 0 serializedStream
        serializedValues   = (map snd . takeWhile ((<= maxBytes) . fst) . zip totalSizes) serializedStream
        deserializedValues = traverse (Get.runGet KP.deserialize) serializedValues
    case deserializedValues of
      Right values -> return (FetchData (partLast partition) (zip [offset..] values))
      Left _       -> error "error deserializing value from a partition"
