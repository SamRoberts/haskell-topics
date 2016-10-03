{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

import           Data.Int (Int32)
import qualified Data.Map as M
import qualified Data.Sequence as S
import           Data.Typeable (typeOf)

import qualified Test.Tasty as T
import           Test.Tasty (TestTree)
import           Test.Tasty.QuickCheck as QC

import           Network.Topics
import           Network.Topics.Sync

main :: IO ()
main = T.defaultMain tests

tests :: TestTree
tests = T.testGroup "produce/offset properties"
  [ QC.testProperty "getOffsets <=< produce ~= length" $ \(messages :: [Int32]) ->
      let topicAction = do
            topic   <- getTopic "test"
            withAllPartitions topic $ produce messages >> getOffsets
          response       =  fst (runSyncTopics topicAction emptyTestState)
          emptyTestState =  KafkaState (M.fromList [("test", (typeOf (undefined :: Int32), S.singleton (KafkaPartition 0 S.empty)))])
          expectedParts  =  [(0, fromIntegral (length messages) - 1)]
      in  response == expectedParts
  ]
