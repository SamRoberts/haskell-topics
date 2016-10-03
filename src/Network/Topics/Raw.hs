{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

-- | A topics program which uses does it's own TCP interactions, not relying on a Kafka library
module Network.Topics.Raw where
    -- FIXME: want a qualified export list at some point, but easier to play around without one
    -- maybe better to define non-exported items in an Internal module, rather than hide them?

import           Control.Applicative (liftA2)
import           Control.Monad.Free (Free(Free))
import           Control.Monad.Free.Class (MonadFree)
import           Control.Monad.Reader (ReaderT)
import           Control.Monad.Reader.Class (MonadReader)

-- thoughts:
--
-- I like the idea of having a free construct for partition, and running everything from topic monad
--
-- Kafka guarantees it will send responses back in the order the requests are received, one at a time,
-- but it encourages users to send requests while waiting for the result of the previous request,
-- so that we don't have to wait a network round trip between finishing one request and starting the next.
--
-- So it seems like we want to have separate Monadic and Applicative implementations, the monadic
-- waiting for responses before sending the next request, and the applicative just spamming requests
-- and then waiting.
--
-- So we need to open up one connection per broker. Can be done lazily? Why do it lazily if we just have one per broker?
-- Because we might need to re-open it in mid-flight anyway, so we need that functionality.
--
-- Ok, so anatomy of a request cycle (one step in monadic code, the whole shebang in applicative code):
--   there are a bunch of request instructions
--   there may be cached metadata depending on topic
--   there may be cached broker connections
--   we figure out brokers for each request instruction:
--     group instructions by topic
--     if we don't have cached topic metadata for all topics of interest, fetch it for all topics of interest
--     use metadata to group instructions by broker
--   process requests:
--     for each broker:
--       group instructions by request type
--       form combined requests, one request per request type per broker
--       send all the requests asynchronously, relying on Kafka guarantees to sort out responses
--       wait for responses
--   handle responses:
--     group responses by status: success, outdated metadata, or failure
--     if any failure, return failure
--     if any outdated, flush metadata for all topics of interest, then repeat process on requests with outdated responses, starting from "figure out brokers for each request instruction"
--     once we have responses for all request instructions, return the responses

newtype Raw a =
    Raw (ReaderT RawConf RawProg a)
    deriving (Functor, Applicative, Monad, MonadFree RawReq, MonadReader RawConf)

data RawConf = RawCong
             { -- TODO config fields
             }

newtype RawProg a =
  RawProg (Free RawReq a)
  deriving (Functor, Monad, MonadFree RawReq)

instance Applicative RawProg where
  pure = return
  (RawProg freeF) <*> (RawProg freeA) =
    let freeB = case (freeF, freeA) of
                  (Free rrpf, Free rrpa) -> Free (liftA2 (<*>) rrpf rrpa)
                  _                      -> freeF <*> freeA
    in  RawProg freeB

-- TODO have this in it's own module, with protected constructors maintaining type safety
data RawReq a = RawReq { requests :: [KafkaReq], respond :: [KafkaResp] -> a }
     deriving Functor

instance Applicative RawReq where
  pure a = RawReq [] (\_ -> a)
  (RawReq lreqs lcont) <*> (RawReq rreqs rcont) =
    RawReq (lreqs ++ rreqs) (\resps ->
      -- TODO check number of responses
      let (lresps, rresps) = splitAt (length lreqs) resps
      in  (lcont lresps) (rcont rresps))

data KafkaReq = PlaceholderKafkaReq
data KafkaResp = PlaceholderKafkaResp

{-
runRaw :: Raw a -> RawConf -> EitherT RawError IO a
runRaw = undefined

data RawError = PlaceholderRawError

data RawVars = RawVars
             { currentTopics :: TVar (Set TopicName)
             , cachedMetadata :: TVar Metadata
             , cachedConnections :: TVar (Map Broker Connection)
             }
-}
