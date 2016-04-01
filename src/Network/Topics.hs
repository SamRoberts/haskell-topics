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

-- FIXME: need some way of representing commonalities between different errors
import           Text.Show (showParen, showString)

-- | syntax for interacting with kafka
data Instruction a where
  GetTopic :: TopicName -> Instruction Topic
  GetOffsets :: OffsetsRequest -> Instruction OffsetsResponse
  Produce :: forall v. Kafkaesque v => ProduceRequest v -> Instruction ProduceResponse
  Fetch :: Kafkaesque v => FetchRequest -> Instruction (FetchResponse v)

instance Show (Instruction Topic) where
  showsPrec fixity (GetTopic name) = showConstructor fixity "GetTopic" name

instance Show (Instruction OffsetsResponse) where
  showsPrec fixity (GetOffsets request) = showConstructor fixity "GetOffsets" request

instance Show (Instruction ProduceResponse) where
  showsPrec fixity (Produce request) = showConstructor fixity "Produce" request

instance Show (Instruction (FetchResponse v)) where
  showsPrec fixity (Fetch request) = showConstructor fixity "Fetch" request

showConstructor outerFixity constructor child =
  showParen (outerFixity >= 10) (showString (constructor ++ " ") . showsPrec 10 child)

type OffsetsRequest = Request ReplicaId ()
type OffsetsResponse = Response CommonError (Offset, Offset)

type ProduceRequest v = Request ProduceConfig [v]
type ProduceResponse = Response (Either CommonError FetchError) Offset

type FetchRequest = Request FetchConfig FetchInfo
type FetchResponse v = Response (Either CommonError FetchError) (FetchData v)

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

data Topic = Topic
           { _topicName :: TopicName
           , _topicMaxPartition :: Partition
           }
           deriving (Show, Eq)

data ProduceConfig = ProduceConfig
                   { _requiredAcks :: RequiredAcks
                   , _timeout :: Timeout
                   }
                   deriving (Show, Eq)

data FetchConfig = FetchConfig
                 { _fetchReplicaId :: ReplicaId
                 , _fetchMaxWaitTime :: Timeout
                 , _fetchMinBytes :: Size
                 }
                 deriving (Show, Eq)

data FetchInfo = FetchInfo
               { _fetchOffset :: Offset
               , _fetchMaxBytes :: Size
               }
               deriving (Show, Eq)

data FetchData v = FetchData
                 { _fetchHighwaterMark :: Offset
                 , _messages :: [(Offset, v)]
                 }
                 deriving (Show, Eq, Functor)

data CommonError = Unknown
                 | UnknownTopicOrPartition
                 | NotLeaderForPartition
                 deriving (Show, Eq)

data ProduceError = ProduceErrorTODO deriving (Show, Eq)

data FetchError = OffsetOutOfRange
                | ReplicaNotAvailable
                deriving (Show, Eq)

newtype TopicName = TopicName Text deriving (Show, Eq, IsString)
newtype Partition = Partition Int32 deriving (Show, Eq, Ord, Num, Enum, Ix)
newtype Offset = Offset Int64 deriving (Show, Eq, Ord, Num, Enum, Ix)
newtype ReplicaId = ReplicaId Int32 deriving (Show, Eq, Ord, Num, Enum, Ix, Bounded)
newtype Timeout = Timeout Int32 deriving (Show, Eq, Ord, Num, Enum, Ix)
newtype Size = Size Int32 deriving (Show, Eq, Ord, Num, Enum, Ix)

instance Bounded Partition where
  minBound = 0
  maxBound = Partition maxBound

data RequiredAcks = WaitForNone
                  | WaitForLeader
                  | WaitForAll
                  | WaitForAtLeast Int16
                  deriving (Show, Eq)

class (Show a, Serializable a, Deserializable a) => Kafkaesque a

instance Kafkaesque Int16
instance Kafkaesque Int32
instance Kafkaesque Int64
