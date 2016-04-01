{-# LANGUAGE DeriveFunctor, GADTs, GeneralizedNewtypeDeriving, RankNTypes, ScopedTypeVariables #-}

-- | A module for interacting with Kafka, either for real or with a dummy implementation.
module Network.Topics where
    -- FIXME: want a qualified export list at some point, but easier to play around without one
    -- maybe better to define non-exported items in an Internal module, rather than hide them?

import           Data.ByteString (ByteString)
import           Data.Int (Int16, Int32, Int64)
import           Data.Ix (Ix)
import           Data.String (IsString)
import           Data.Text (Text)

import           Network.Kafka.Protocol (Deserializable, Serializable)
import qualified Network.Kafka.Protocol as KP

-- FIXME: change request and partition to allow multiple topic * partition pairs per request / response cycle
-- FIXME: need some way of representing commonalities between different errors

data Request c a = Request
                 { _reqConf :: c
                 , _reqTopic :: Topic
                 , _reqParts :: [(Partition, a)]
                 }
                 deriving (Show, Eq, Functor)

data Response e a = Response
                  { _respTopic :: Topic
                  , _respParts :: [(Partition, Either e a)]
                  }
                  deriving (Show, Eq, Functor)

class (Show a, Serializable a, Deserializable a) => Kafkaesque a

instance Kafkaesque Int16
instance Kafkaesque Int32
instance Kafkaesque Int64

data MyType = MyType Int16 Int32 deriving (Show, Eq)

instance Serializable MyType where
  serialize (MyType i j) = do
    KP.serialize i
    KP.serialize j

instance Deserializable MyType where
  deserialize = do
    i :: Int16 <- KP.deserialize
    j :: Int32 <- KP.deserialize
    return $ MyType i j

instance Kafkaesque MyType

type OffsetsRequest = Request ReplicaId ()
type OffsetsResponse = Response OffsetError (Offset, Offset)

type ProduceRequest v = Request ProduceConfig [v]
type ProduceResponse = Response ProduceError Offset

type FetchRequest = Request FetchConfig FetchInfo
type FetchResponse v = Response FetchError (FetchData v)

-- currently thinking of using the operational monad here
data Instruction a where
  GetTopic :: TopicName -> Instruction Topic
  GetOffsets :: OffsetsRequest -> Instruction OffsetsResponse
  Produce :: forall v. Kafkaesque v => ProduceRequest v -> Instruction ProduceResponse
  Fetch :: Kafkaesque v => FetchRequest -> Instruction (FetchResponse v)

-- Note on GetOffsets:
--
-- Retrieving both offsets works by setting time to the latest and setting max number of offsets to the
-- maximum possible number.
--
-- Might be nice to be able to just retrieve latest or earliest with a targetted request to just retrieve
-- the one offset. Leaving it out of first cut though as I doubt it really makes much difference.

instance Show (Instruction Topic) where
  show (GetTopic name) = "GetTopic " ++ show name

instance Show (Instruction OffsetsResponse) where
  show (GetOffsets request) = "GetOffsets " ++ show request

instance Show (Instruction ProduceResponse) where
  show (Produce request) = "Produce " ++ show request

instance Show (Instruction (FetchResponse v)) where
  show (Fetch request) = "Fetch " ++ show request

newtype TopicName = TopicName Text deriving (Show, Eq, IsString)
newtype Partition = Partition Int32 deriving (Show, Eq, Ord, Num, Enum, Ix)
newtype Offset = Offset Int64 deriving (Show, Eq, Ord, Num, Enum, Ix)
newtype ReplicaId = ReplicaId Int32 deriving (Show, Eq, Ord, Num, Enum, Ix, Bounded)
newtype Timeout = Timeout Int32 deriving (Show, Eq, Ord, Num, Enum, Ix)
newtype Size = Size Int32 deriving (Show, Eq, Ord, Num, Enum, Ix)

data Topic = Topic
           { _topicName :: TopicName
           , _topicMaxPartition :: Partition
           }
           deriving (Show, Eq)

instance Bounded Partition where
  minBound = 0
  maxBound = Partition maxBound

data OffsetError = OffsetUnknown
                 | OffsetUnknownTopicOrPartition
                 | OffsetNotLeaderForPartition
                 deriving (Show, Eq)

data ProduceConfig = ProduceConfig
                   { _requiredAcks :: RequiredAcks
                   , _timeout :: Timeout
                   }
                   deriving (Show, Eq)

data RequiredAcks = WaitForNone
                  | WaitForLeader
                  | WaitForAll
                  | WaitForAtLeast Int16
                  deriving (Show, Eq)

data ProduceError = ProduceErrorTODO deriving (Show, Eq)

data FetchConfig = FetchConfig
                 { _replicaId :: ReplicaId
                 , _maxWaitTime :: Timeout
                 , _minBytes :: Size
                 }
                 deriving (Show, Eq)

data FetchInfo = FetchInfo
               { _fetchOffset :: Offset
               , _maxBytes :: Size
               }
               deriving (Show, Eq)

data FetchError = FetchUnknown
                | FetchUnknownTopicOrPartition
                | FetchNotLeaderForPartition
                | FetchOffsetOutOfRange
                | FetchReplicaNotAvailable
                deriving (Show, Eq)

data FetchData v = FetchData
                 { _highwaterMark :: Offset
                 , _messages :: [(Offset, v)]
                 }
                 deriving (Show, Eq, Functor)
