{-# LANGUAGE DeriveFunctor, FlexibleInstances, FunctionalDependencies, GADTs, GeneralizedNewtypeDeriving, MultiParamTypeClasses, RankNTypes, ScopedTypeVariables, TemplateHaskell #-}

-- | A module for interacting with Kafka, either for real or with a dummy implementation.
module Network.Topics where
    -- FIXME: want a qualified export list at some point, but easier to play around without one
    -- maybe better to define non-exported items in an Internal module, rather than hide them?

import           Control.Monad.Operational (Program, ProgramT)
import qualified Control.Monad.Operational as Op

import           Data.Int (Int16, Int32, Int64)
import           Data.Ix (Ix)
import           Data.String (IsString)
import           Data.Text (Text)

import Data.Typeable (Typeable)

import           Network.Kafka.Protocol (Deserializable, Serializable)

-- | A monad transformer for Kafka interactions
type TopicsT m a = ProgramT Instruction m a

-- | A monad for interacting with Kafka
type Topics a = Program Instruction a

getTopic :: TopicName -> ProgramT Instruction m (Maybe Topic)
getTopic = Op.singleton . GetTopic

getOffsets :: OffsetsRequest -> ProgramT Instruction m OffsetsResponse
getOffsets = Op.singleton . GetOffsets

produce :: Kafkaesque v => ProduceRequest v -> ProgramT Instruction m ProduceResponse
produce = Op.singleton . Produce

fetch :: Kafkaesque v => FetchRequest -> ProgramT Instruction m (FetchResponse v)
fetch = Op.singleton . Fetch

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
data Instruction a where
  GetTopic :: TopicName -> Instruction (Maybe Topic) -- ^ Retrieve a topic reference and metadata for a given topic name
  GetOffsets :: OffsetsRequest -> Instruction OffsetsResponse -- ^ Retrieve the first and last available offsets in a given topic
  Produce :: forall v. Kafkaesque v => ProduceRequest v -> Instruction ProduceResponse -- ^ Send messages to a topic
  Fetch :: Kafkaesque v => FetchRequest -> Instruction (FetchResponse v) -- ^ Read messages from a topic

data TempTopic v = TempTopic v -- really should be Topic, once we've ironed it out

class Monad ts => ClassyTopics ts t | ts -> t where
  cGetTopic :: Typeable v => TopicName -> ts (Maybe (TempTopic v))
  cOnTopic :: ClassyTopic t v => TempTopic v -> t v a -> ts a

class Monad (t v) => ClassyTopic t v where
  cGetOffsets :: OffsetsRequest -> t v OffsetsResponse
  cProduce :: Kafkaesque v => ProduceRequest v -> t v ProduceResponse
  cFetch :: Kafkaesque v => FetchRequest -> t v (FetchResponse v)

newtype IOTopic v a = IOTopic (IO a) deriving (Functor, Applicative, Monad)

instance ClassyTopics IO IOTopic where
  cGetTopic _ = error "called cGetTopic"
  cOnTopic _ _ = error "called cOnTopic"

instance ClassyTopic IOTopic v where
  cGetOffsets _ = error "called cGetOffsets"
  cProduce _ = error "called cProduce"
  cFetch _ = error "called cFetch"

-- | A request to get the first and last offset available for a given topic
type OffsetsRequest = Request () ()
-- TODO should offset request allow asking for all topics in one request? That seems reasonable to me.

-- | The response giving the first and last offsets available for a given topic
type OffsetsResponse = Response CommonError (Offset, Offset)

-- | A request to send a list of messages to Kaka
type ProduceRequest v = Request ProduceConfig [v]

-- | The response after sending a list of messages to Kafka
--
-- The offset returned is the offset of the first message sent to Kafka.
type ProduceResponse = Response (Either CommonError FetchError) Offset
-- TODO Is zip [responseOffset..] requestMessageList a valid way of determining offset for each message?
--      or is it possible that some other produce request's messages could be interleaved with mine?

-- | A request to fetch some messages from Kafka
type FetchRequest = Request FetchConfig FetchInfo

-- | The response coming back from a fetch request to Kafka
type FetchResponse v = Response (Either CommonError FetchError) (FetchData v)

-- | The common structure for all Kafka requests
data Request c a = Request
                 { reqClientId :: ClientId -- ^ A string Kafka will include when logging any errors relating to this request
                 , reqTopic :: Topic -- ^ The topic that the request applies to
                 , reqConf :: c -- ^ Request fields that apply to all partitions
                 , reqParts :: [(Partition, a)] -- ^ The partitions that the request applies to, along with per-partition request fields
                 }
                 deriving (Show, Eq, Functor)

-- | The common structure for all Kafka responses
data Response e a = Response
                  { respTopic :: Topic -- ^ The topic that the response relates to
                  , respParts :: [(Partition, Either e a)] -- ^ Each partition returns either an error or the response
                  }
                  deriving (Show, Eq, Functor)

-- | A topic
--
-- A topic has a name and a range of partitions from minBound to _topicMaxPartition.
data Topic = Topic
           { topicName :: TopicName -- ^ The topic name
           , topicMaxPartition :: Partition -- ^ The largest partition for this topic. The topic has partitions ranging from minBound to this partition.
           }
           deriving (Show, Eq)

-- | Produce request fields that apply to all partitions
data ProduceConfig = ProduceConfig
                   { produceRequiredAcks :: RequiredAcks -- ^ The number of acknowledgements the Kafka leader will wait for before responding to the client
                   , produceTimeout :: Timeout -- ^ The maximum time the Kafka leader should spent waiting for the required number of acknowledgements
                   }
                   deriving (Show, Eq)

-- | Fetch request fields that apply to all partitions
data FetchConfig = FetchConfig
                 { fetchMaxWaitTime :: Timeout -- ^ The maximum time the Kafka leader should spent waiting for more than _fetchMinBytes to be available to respond with
                 , fetchMinBytes :: Size -- ^ The minimum number of bytes that Kafka should respond with, Kafka will wait up to _fetchMaxWaitTime if not enough bytes are available. Typical values would be 0 to always return immediately, or 1 to wait for at least one message.
                 }
                 deriving (Show, Eq)

-- | Fetch request fields for each partition
data FetchInfo = FetchInfo
               { fetchOffset :: Offset -- ^ The offset to start fetching messages from
               , fetchMaxBytes :: Size -- ^ The maximum number of message bytes in the response
               }
               deriving (Show, Eq)

-- | The fetch response for each partition
data FetchData v = FetchData
                 { fetchHighwaterMark :: Offset -- ^ The last available offset in the partition. Can be used to determine if there is still more data to be fetched
                 , fetchMessages :: [(Offset, v)] -- ^ The messages retrieved from Kafka, with their corresponding offsets
                 }
                 deriving (Show, Eq, Functor)

-- | Common errors shared by many Kafka responses
data CommonError = Unknown -- ^ Unknown error, no further information
                 | UnknownTopicOrPartition -- ^ The topic or partition does not exist
                 | NotLeaderForPartition -- ^ The request was not sent to the current leader for this partition
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
newtype Partition = Partition Int32 deriving (Show, Eq, Ord, Num, Real, Integral, Enum, Ix)

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

instance Bounded Partition where
  minBound = 0
  maxBound = Partition maxBound

-- | A data type that can be serialized and deserialized to and from Kafka
class (Serializable a, Deserializable a) => Kafkaesque a

instance Kafkaesque Int16
instance Kafkaesque Int32
instance Kafkaesque Int64
