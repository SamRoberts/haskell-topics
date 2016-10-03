{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}

-- | A topics program which uses Kafka as the backend
module Network.Topics.Kafka where
    -- FIXME: want a qualified export list at some point, but easier to play around without one
    -- maybe better to define non-exported items in an Internal module, rather than hide them?

import           Control.Lens (Iso', (^.), (^..), each, filtered, from, iso, to)
import           Control.Monad.Except (ExceptT)
import           Control.Monad.Error.Class (MonadError, throwError)
import           Control.Monad.Free (Free)
import           Control.Monad.Free.Class (MonadFree, liftF)
--import           Control.Monad.IO.Class (MonadIO)
import           Control.Monad.Reader (ReaderT, runReaderT)
import           Control.Monad.Reader.Class (MonadReader, ask)
import           Control.Monad.Trans.Class (lift)

--import           Data.ByteString (ByteString)
--import qualified Data.ByteString as B
--import qualified Data.List as L
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
--import qualified Data.Serialize.Get as Get
--import qualified Data.Serialize.Put as Put
import qualified Data.Text.Encoding as T
import           Data.Typeable (TypeRep)
import qualified Data.Typeable as T

import qualified Network.Kafka as K
import qualified Network.Kafka.Protocol as KP

import           Network.Topics

-- A KafkaTopics action
--
-- The corresponding partition action is just a free monad on the list of kafka instructions,
-- so that we can inspect the actions to perform and merge them where possible. This means that
-- all the "real logic" is in KafkaTopics.
--
-- KafkaTopics keeps track of the Kafka config, can return kafka-specific errors, and can run Kafka actions.
-- Kafka actions themselves store some state, including:
--   * a list of broker ids -> host:port's,
--   * a list of host:port -> connection,
--   * a list of a list of partition -> current leader.
--
-- Kafka actions by default don't seem to automatically update leader metadata upon receiving invalid
-- leader errors. So looks like we need to take responses, partition into results, errors, and invalid leader errors
-- then update metadata for bad topics, and ty to redo the requests which responded with invalid leader errors.
--
-- Milena by default only seem to handle calling one doRequest' at a time on a given connection, and also
-- only supports one connection per broker. So initially we will do blocking IO, one request at a time to
-- any given broker. Would be nice to make doRequest' properly transactional, and submit a patch
-- to Milena for asynchronous requests, and another patch for supporting multiple connections
newtype KafkaTopics a =
    KafkaTopics { kafkaTopics :: ReaderT KafkaTopicsConfig (ExceptT KafkaTopicsError K.Kafka) a }
  deriving (Functor, Applicative, Monad, MonadReader KafkaTopicsConfig, MonadError KafkaTopicsError)

kafka :: K.Kafka a -> KafkaTopics a
kafka = KafkaTopics . lift . lift

newtype KafkaTopicsConfig = KafkaTopicsConfig { topicTypes :: Map TopicName TypeRep }

-- | An error type for topics specific errors
data KafkaTopicsError =
  -- TODO check that this is what we really want, after all,
  --      Milena throws a heap of errors into IO, so is a strongly typed Topics error really worth it?
  --      Or maybe, if we have this, we should double down on it and catch
  --      a bunch of Milena errors and turn them into strongly typed errors?
  --      Decisions decisions ...
    NoTopic TopicName                -- ^ The requested topic does not exist
  | NoTopicInKafka TopicName         -- ^ The requested topic should exist but cannot be found in Kafka
  | WrongTopicType TopicName TypeRep -- ^ The requested topic exists with a different type, returns the topic name and actual type
  | KafkaError KP.KafkaError         -- ^ Interaction with kafka returned a KafkaError
  | AmbiguousTopicMetadata [KP.TopicMetadata] -- ^ Kafka found multiple metadata entries for this topic

newtype KafkaPartition v a =
    KafkaPartition { kafkaPartition :: ReaderT K.TopicAndPartition (Free (KafkaStep v)) a }
  deriving (Functor, Applicative, Monad, MonadReader K.TopicAndPartition, MonadFree (KafkaStep v))

data KafkaStep v c =
    OStep (OffsetStep c)
  | PStep (ProduceStep v c)
  | FStep (FetchStep v c)
  deriving (Functor)

data OffsetStep c    = OffsetStep ((Offset, Offset) -> c) deriving Functor
data ProduceStep v c = ProduceStep [v] (Offset -> c) deriving Functor
data FetchStep v c   = FetchStep Offset Size (FetchData v -> c) deriving Functor


kafkaTopic :: Iso' TopicName KP.TopicName
kafkaTopic = iso forward backward
  where
    forward (TopicName name) = name ^. to T.encodeUtf8 . from KP.kString . from KP.tName
    backward name = name ^. KP.tName . KP.kString . to T.decodeUtf8 . to TopicName

kafkaPId :: Iso' PartitionId KP.Partition
kafkaPId = iso forward backward
  where
    forward (PartitionId pid) = KP.Partition pid
    backward (KP.Partition pid) = PartitionId pid

ensure :: Bool -> KafkaTopicsError -> KafkaTopics ()
ensure ok err =
    if ok then return () else throwError err

partitionMetadata :: TopicName -> KafkaTopics [KP.PartitionMetadata]
partitionMetadata name = do
    response <- kafka (K.metadata (KP.MetadataReq [nameK]))
    let mdAnswers = response ^.. KP.topicsMetadata . each . filtered (^. KP.topicMetadataName . to (== nameK))
    case mdAnswers of
      [metadata] -> do
        let err = metadata ^. KP.topicMetadataKafkaError
        ensure (err == KP.NoError) (KafkaError err)
        return (metadata ^. KP.partitionsMetadata)

      [] ->
        throwError (NoTopicInKafka name)

      _ ->
        throwError (AmbiguousTopicMetadata mdAnswers)
  where
    nameK = name ^. kafkaTopic

instance Topics KafkaTopics KafkaPartition where
    getTopic :: forall v. Kafkaesque v => TopicName -> KafkaTopics (Topic v)
    getTopic name = do
        KafkaTopicsConfig {..} <- ask

        typeRef <- maybe (throwError (NoTopic name)) return (M.lookup name topicTypes)
        ensure (typeRef == T.typeOf (undefined :: v)) (WrongTopicType name typeRef)

        partitions <- partitionMetadata name
        -- TODO cache partitionMetadata?
        -- the partitions are numbered from 0 to N-1
        return (Topic name (PartitionId (fromIntegral (length partitions - 1))))

    withPartitions :: Kafkaesque v => Topic v -> [PartitionId] -> KafkaPartition v a -> KafkaTopics [a]
    withPartitions Topic {..} pids action =
        -- TODO throw error on bad partition ids
        let taps = map (^. kafkaPId . to (K.TopicAndPartition (topicName ^. kafkaTopic))) pids
        -- NOTE I can't help feel like the algorithm here should be pretty super bloody simple
        --      1. start with action
        --      2. get steps from applying taps
        --      3. collate steps
        --      4. carry out each collated step, possibly in parallel as they must all be independant
        --      5. call the continuations
        --      6. loop back to step 2
            steps = map (runReaderT (kafkaPartition action)) taps
        in  undefined


instance Kafkaesque v => Partition KafkaPartition v where
    partitionId :: KafkaPartition v PartitionId
    partitionId = fmap (^. to K._tapPartition . from kafkaPId) ask

    getOffsets :: KafkaPartition v (Offset, Offset)
    getOffsets = liftF (OStep (OffsetStep id))

    produce :: [v] -> KafkaPartition v Offset
    produce newItems = liftF (PStep (ProduceStep newItems id))

    fetch :: Offset -> Size -> KafkaPartition v (FetchData v)
    fetch offset maxBytes = liftF (FStep (FetchStep offset maxBytes id))

{-

Giving up on this approach for the moment, don't really like synchronous behaviour of Milena

data SafeReq m c = forall resp. SafeReq (KP.ReqResp (m resp)) (resp -> [(PartitionId, c)])

-- TODO replace list of SafeRep with a function parameterised on resp
--      which actually does the request, runs the continuations, and
--      returns the results, then just call this function three times for
--      Offset, Produce, and Fetch, rather than glomming all the requests
--      together into the one list
--
-- TODO also, take inspiration from Milena producer, which groups messages together by leader
--      might be able to re-use some of that logic
requests :: forall m v c. (MonadIO m, MonadReader KafkaTopicsConfig m, Kafkaesque v)
         => Topic v
         -> [(PartitionId, KafkaStep v c)]
         -> Kafka (M.Map Leader (Maybe KP.OffsetRequest, Maybe KP.ProduceRequest, Maybe KP.FetchRequest))
requests Topic {..} steps KafkaTopicsConfig {..} =
    [ runReq oRR oCont | not (null oSteps) ] ++
    [ runReq pRR pCont | not (null pSteps) ] ++
    [ runReq fRR fCont | not (null fsteps) ]
  where
    oRR = let body  = (repId, [(kName, parts)])
              repId = -1 -- only to be used by kafka replicas
              parts = [ (pid ^. kafkaPId, undefined, undefined)
                      | (pid, _) <- oSteps ]
          in KP.OffsetRR (KP.OffsetReq body)

    pRR = let body  = (undefined, undefined, [(kName, parts)])
              parts = [ (pid ^. kafkaPId, undefined)
                      | (pid, _) <- pSteps ]
          in KP.ProduceRR (KP.ProduceReq body)

    fRR = let body  = (undefined, undefined, undefined, [(kName, parts)])
              parts = [ (pid ^. kafkaPId, undefined, undefined)
                      | (pid, _) <- pSteps ]
          in KP.FetchRR (KP.FetchReq body)

    oCont oResp = undefined
    pCont oResp = undefined
    fCont oResp = undefined

    --mkOCont :: [(PartitionId, OffsetStep c)] -> KP.OffsetResponse -> [(PartitionId, c)]
    mkOCont = undefined

    --mkPCont :: [(PartitionId, ProduceStep v c)] -> KP.ProduceResponse -> [(PartitionId, c)]
    mkPCont = undefined

    --mkFCont :: [(PartitionId, FetchStep v c)] -> KP.FetchResponse -> [(PartitionId, c)]
    mkFCont = undefined

    oSteps = [(pid, oStep) | (pid, OStep oStep) <- steps ]
    pSteps = [(pid, pStep) | (pid, PStep pStep) <- steps ]
    fSteps = [(pid, fStep) | (pid, FStep fStep) <- steps ]

    kName = topicName ^. kafkaTopic
-}
