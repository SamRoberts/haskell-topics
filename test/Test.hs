{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}

import qualified Control.Monad.Trans.State.Strict as St

import           Data.Int (Int32)
import qualified Data.Map as M
import           Data.Maybe (fromJust)
import qualified Data.Sequence as S

import qualified Test.Tasty as T
import           Test.Tasty (TestTree)
import           Test.Tasty.QuickCheck as QC

import           Network.Topics
import           Network.Topics.Memory

main :: IO ()
main = T.defaultMain tests

tests :: TestTree
tests = T.testGroup "produce/offset properties"
  [ QC.testProperty "getOffsets <=< produce ~= length" $ \(messages :: [Int32]) ->
      let topicAction = do
                          topic   <- fmap fromJust (getTopic "test")
                          _       <- produce (Request "produce" topic (ProduceConfig WaitForAll 1000) [(0, messages)])
                          offsets <- getOffsets (Request "offsets" topic () [(0, ())])
                          return offsets
          stateAction = runTopicsInMemory topicAction
          response = St.evalState stateAction emptyTestState
          emptyTestState = KafkaState
                         { kafkaState = M.fromList [("test", S.singleton (KafkaPartition 0 S.empty))]
                         }
          expectedParts = [(0, Right (0, fromIntegral (length messages) - 1))]
          actualParts = respParts response
      in  expectedParts == actualParts
  ]
