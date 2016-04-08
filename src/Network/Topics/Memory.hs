{-# LANGUAGE GADTs #-}

-- | An in-memory interpreter for Topics programs
module Network.Topics.Memory where
    -- FIXME: want a qualified export list at some point, but easier to play around without one
    -- maybe better to define non-exported items in an Internal module, rather than hide them?

import           Control.Monad ((<=<))
import           Control.Monad.Operational(ProgramView, ProgramViewT(Return, (:>>=)))
import qualified Control.Monad.Operational as Op
import           Control.Monad.Trans.State.Strict (State)
import qualified Control.Monad.Trans.State.Strict as St

import           Data.ByteString (ByteString)
import           Data.Int (Int64)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.Sequence (Seq)

import qualified Network.Kafka.Protocol as KP

import           Network.Topics

data KafkaState = KafkaState { kafkaState :: (Map TopicName (Seq KafkaPartition)) }
data KafkaPartition = KafkaPartition
                    { partFirst :: Offset
                    , partItems :: Seq ByteString
                    }

mkTopic :: TopicName -> Seq KafkaPartition -> Topic
mkTopic topicName partitions = Topic topicName (fromIntegral $ length partitions - 1)

runTopicsInMemory :: Topics a -> State KafkaState a
runTopicsInMemory = Op.interpretWithMonad eval
  where
    eval :: Instruction a -> State KafkaState a
    eval (GetTopic topicName) = St.gets (fmap (mkTopic topicName) . M.lookup topicName . kafkaState)
    eval (GetOffsets request) = error "not implemented"
    eval (Produce    request) = error "not implemented"
    eval (Fetch      request) = error "not implemented"

