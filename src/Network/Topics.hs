{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards #-}

-- | A module for interacting with Kafka, either for real or with a dummy implementation.
module Network.Topics where
    -- FIXME: want a qualified export list at some point, but easier to play around without one
    -- maybe better to define non-exported items in an Internal module, rather than hide them?

import           Data.Int (Int16, Int32, Int64)
import           Data.Ix (Ix)
import           Data.String (IsString)
import           Data.Text (Text)
import           Data.Typeable (Typeable)

import           Network.Kafka.Protocol (Deserializable, Serializable)

-- | syntax for interacting with kafka
--
-- FIXME: introduce a way to work with successfull responses only, treating each partition
-- independantly of the others ... is it possible to do that and still send responses together?
-- Run monad up to next request along each branch, than gather requests together, send them all
-- out, rinse, repeat?
--
-- FIXME: will need a consistent way to handle errors if we do this, thinking of having a global error type
-- and the ability to recover specific error types form that, but no tracking of what errors are possible
--
-- FIXME: default settings for config and fetch objects, with the ability to override on a per-request basis
-- kinda seems like I want a reader monad for config
--
-- FIXME: allow offset response to return something sensible on an emtpy queue. Might just use maybe?
--
-- FIXME: consider the suitability of (first, last) offset vs (first, next) or (first, length)
class Topics ts t | ts -> t where
  getTopic :: Typeable v => TopicName -> ts (Maybe (Topic v))
  withPartitions :: (Partition t v) => Topic v -> [PartitionId] -> t v a -> ts [a]

  withAllPartitions :: Partition t v => Topic v -> t v a -> ts [a]
  withAllPartitions topic = withPartitions topic (allPartitions topic)

class Partition t v where
  getOffsets :: t v (Offset, Offset)
  produce :: Kafkaesque v => [v] -> t v Offset
  fetch :: Kafkaesque v => Offset -> Size -> t v (FetchData v)

-- | Kafka configuration which must be the same for every partition in a single request
data KafkaConfig = KafkaConfig
                 { kafkaClientId :: ClientId -- ^ A string Kafka will include when logging any error relating to this request
                 , kafkaRequiredAcks :: RequiredAcks -- ^ The number of akcnowledgements the Kafka leader will wait for before responding to the client
                 , kafkaProduceTimeout :: Timeout -- ^ The maximum time the Kafka leader should spend waiting for the required number of acknowledgments
                 , kafkaMinBytes :: Size -- ^ The minimum number of bytes that Kafka should respond with. Typical values would be 0 to always return immediately, or 1 to wait for at least one message.
                 , kafkaFetchTimeout :: Timeout -- ^ The maximum time the Kafka leader should spend waiting for the required amount of bytes of data to be available to send back to the client.
                 }
                 deriving (Show, Eq)

-- | A topic
--
-- A topic has a name and a range of partitions from minBound to _topicMaxPartition.
data Topic v = Topic
             { topicName :: TopicName -- ^ The topic name
             , topicMaxPartition :: PartitionId -- ^ The largest partition for this topic. The topic has partitions ranging from minBound to this partition.
             }
             deriving (Show, Eq)

data FetchData v = FetchData
                 { fetchHighwaterMark :: Offset -- ^ The last available offset in the partition. Can be used to determine if there is still more data to be fetched
                 , fetchMessages :: [(Offset, v)] -- ^ The messages retrieved from Kafka, with their corresponding offsets
                  }
                  deriving (Show, Eq, Functor)

allPartitions :: Topic v -> [PartitionId]
allPartitions Topic {..} = [minBound .. topicMaxPartition]

-- | Fetch request fields for each partition
data FetchInfo = FetchInfo
               { fetchOffset :: Offset -- ^ The offset to start fetching messages from
               , fetchMaxBytes :: Size -- ^ The maximum number of message bytes in the response
               }
               deriving (Show, Eq)

-- | Common errors shared by many Kafka responses
data CommonError = Unknown -- ^ Unknown error, no further information
                 | UnknownTopicOrPartition -- ^ The topic or partition does not exist
                 deriving (Show, Eq)

-- | Errors for Produce requests
data ProduceError = ProduceErrorTODO deriving (Show, Eq)
-- FIXME err ... actually fill these in

-- | Errors for Fetch requests
data FetchError = OffsetOutOfRange -- ^ The offset is outside the range of offsets stored on the server for that partition
                | ReplicaNotAvailable -- ^ Kafka server expected to find a replica that did not exist
                deriving (Show, Eq)
-- FIXME but *surely* things like timeout errors should be included here!
-- FIXME unclear when ReplicaNotAvailable is sent to the client and why the client cares

-- | The name for a topic
newtype TopicName = TopicName Text deriving (Show, Eq, Ord, IsString)

-- | A partition for a topic
newtype PartitionId = PartitionId Int32 deriving (Show, Eq, Ord, Num, Real, Integral, Enum, Ix)

-- | An identifying string that the client can send to Kafka, Kafka will use this string when logging any errors
--
-- This allows the client to correlate Kafka errors with the request that caused those errors.
newtype ClientId = ClientId Text deriving (Show, Eq, IsString)

-- | The offset of a message inside a partition
newtype Offset = Offset Int64 deriving (Show, Eq, Ord, Num, Real, Integral, Enum, Ix)

-- | A timeout value, in milliseconds
newtype Timeout = Timeout { timeoutMillis :: Int32 } deriving (Show, Eq, Ord, Num, Real, Integral, Enum, Ix)

-- | The size of some data, in bytes
newtype Size = Size Int32 deriving (Show, Eq, Ord, Num, Real, Integral, Enum, Ix)

-- | The number of acknowledgments the Kafka leader should receive before responding to a Produce request
--
-- The Kafka leader applies produce requests to itself, as well as sending them to other brokers. The other
-- brokers will respond with an Ack when they have successfully applied the produce request. This setting
-- controls the number of Acks that the Kafka leader will wait for before responding to the client.
data RequiredAcks = NoResponse -- ^ Never respond to the client, this means the client gets no confirmation that the produce succeeded
                  | WaitForLeader -- ^ Respond as soon as the Kafka leader write the data to it's own log
                  | WaitForAll -- ^ Respond when all in-sync replicas write the data to their own logs
                  | WaitForAtLeast Int16 -- ^ Respond either when all in-sync replicas write the data, or when at least N in-sync replicas have written the data to their own logs
                  deriving (Show, Eq)

instance Bounded PartitionId where
  minBound = 0
  maxBound = PartitionId maxBound

-- | A data type that can be serialized and deserialized to and from Kafka
class (Serializable a, Deserializable a) => Kafkaesque a

instance Kafkaesque Int16
instance Kafkaesque Int32
instance Kafkaesque Int64
