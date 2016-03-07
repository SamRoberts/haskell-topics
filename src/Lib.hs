{-# LANGUAGE DeriveFunctor, ScopedTypeVariables, GeneralizedNewtypeDeriving, LambdaCase #-}

module Lib where
    -- FIXME: want a qualified export list at some point, but easier to play around without one
    -- maybe better to define non-exported items in an Internal module, rather than hide them?
    -- ( someFunc
    -- ) where

import           Control.Applicative (liftA2)
import           Data.Binary (Binary, get, put)
import           Data.Binary.Get (Get, getByteString)
import           Data.Binary.Put (putByteString, putLazyByteString, runPut)

import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL

import           Data.Digest.CRC32 (crc32)

import           Data.Int (Int8, Int16, Int32, Int64)

import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import           Data.Vector (Vector)
import qualified Data.Vector as V

import           Data.Word (Word32)

type Size t = Prop t Int32
data Prop t a = Const a | Variable (t -> a) deriving (Functor)

instance Applicative (Prop t) where
  pure = Const
  (Const f) <*> (Const a) = Const (f a)
  (Const f) <*> (Variable fa) = Variable (\x -> f (fa x))
  (Variable ff) <*> (Const a) = Variable (\x -> (ff x) a)
  (Variable ff) <*> (Variable fa) = Variable (\x -> (ff x) (fa x))

prop :: Prop t a -> t -> a
prop (Const a) _ = a
prop (Variable f) t = f t

var :: Integral a => (t -> a) -> Size t
var f = Variable (fromIntegral . f)

class (Binary t) => Kafka t where
  -- encodedSize = BS.length . encode
  -- kafka serialization needs to know the length of the encoded message before putting the message itself
  encodedSize :: Prop t Int32

  -- FIXME: seeing as how we already know the length
  --        it seems like we should be able to skip the Binary builder framework
  --        and just directly allocate into the final buffer

-- integers
-- binary already encodes integers in big endian format

instance Kafka Int8 where
  encodedSize = Const 1

instance Kafka Int16 where
  encodedSize = Const 2

instance Kafka Int32 where
  encodedSize = Const 4

instance Kafka Int64 where
  encodedSize = Const 8

instance Kafka Word32 where
  encodedSize = Const 4

-- Strings
kString :: Text -> KString
kString = KString . T.encodeUtf8

unKString :: KString -> Text
unKString (KString bs) = T.decodeUtf8 bs

-- FIXME: hide constructor to KString
newtype KString = KString ByteString deriving Eq

instance Show KString where
  show (KString bs) = show $ T.decodeUtf8 bs

instance Binary KString where
  put (KString utf8) = do
    put ((fromIntegral (BS.length utf8)) :: Int16)
    putByteString utf8

  get = do
    length <- fmap fromIntegral (get :: Get Int16)
    fmap KString (getByteString length)

instance Kafka KString where
  encodedSize = var $ \(KString utf8) ->
    let len :: Int16 = fromIntegral $ BS.length utf8
    in  prop encodedSize len + fromIntegral len

-- Bytes
kBytes :: ByteString -> KBytes
kBytes = KBytes

newtype KBytes = KBytes { unKBytes :: ByteString } deriving (Show, Eq)

instance Binary KBytes where
  put (KBytes bs) = do
    put ((fromIntegral (BS.length bs)) :: Int32)
    putByteString bs

  get = do
    length <- fmap fromIntegral (get :: Get Int32)
    fmap kBytes (getByteString length)

instance Kafka KBytes where
  encodedSize = var $ \(KBytes bs) ->
    let len :: Int32 = fromIntegral $ BS.length bs
    in  prop encodedSize len + len

-- Nullable Bytes
kNullBytes :: Maybe ByteString -> KNullBytes
kNullBytes = KNullBytes

newtype KNullBytes = KNullBytes { unKNullBytes :: Maybe ByteString } deriving (Show, Eq)

instance Binary KNullBytes where
  put (KNullBytes Nothing) =
    put (-1 :: Int32)

  put (KNullBytes (Just bs)) = do
    put ((fromIntegral (BS.length bs)) :: Int32)
    putByteString bs

  get = do
    length <- fmap fromIntegral (get :: Get Int32)
    case length of
      -1 -> return (kNullBytes Nothing)
      n  -> fmap (kNullBytes . Just) (getByteString length)

instance Kafka KNullBytes where
  encodedSize = var $ \case
    KNullBytes Nothing -> prop encodedSize (-1 :: Int32)
    KNullBytes (Just bs) -> let len :: Int32 = fromIntegral $ BS.length bs
                            in  prop encodedSize len + len

-- Arrays
kArray :: Vector a -> KArray a
kArray = KArray

newtype KArray a = KArray { unKArray :: Vector a } deriving (Show, Eq)

instance Binary a => Binary (KArray a) where
  put (KArray elems) = do
    put ((fromIntegral (V.length elems)) :: Int32)
    mapM_ put elems

  get = do
    length <- fmap fromIntegral (get :: Get Int32)
    fmap kArray (V.replicateM length get)

instance (Kafka a) => Kafka (KArray a) where
  encodedSize = var $ \(KArray elems) ->
    let len :: Int32 = fromIntegral $ V.length elems
    in  prop encodedSize len + case encodedSize :: Size a of
      Const elemSize -> len * elemSize
      Variable getSz -> sum (fmap getSz elems)

-- the Message type
data KMessage = KMessage
             -- FIXME: support compression, currently assumes compression is off
             -- FIXME: make message typed, to give compiler information about
             --        underlying types (e.g. are they constant size?)
             { _key :: KNullBytes
             , _value :: KNullBytes
             }
             deriving (Show, Eq)

instance Binary KMessage where
  put (KMessage key value) = do
    let body = runPut $ do
                 put (0 :: Int8) -- magic byte, current version is 0
                 put (0 :: Int8) -- compression flags, FIXME support compression
                 put key
                 put value
    put (crc32 body)
    putLazyByteString body

  get = do
    crc <- fmap fromIntegral (get :: Get Word32)
    expect (0 :: Int8) "haskell-topics only supports a message magic byte of 0"
    expect (0 :: Int8) "haskell-topics does not support message encryption"
    key <- get :: Get KNullBytes
    value <- get :: Get KNullBytes
    -- FIXME: check the crc of the remainder of the message
    --        kinda difficult to see how to do this efficiently with the get
    --        framework, might have to wait for lower level buffer manipulation
    --        what we want is a parser that will return the underlying bytes parsed
    --        as well as the return value. If we used get framework would probably
    --        use some combination of look ahead and bytesRead, but then would have
    --        to run parser twice, once to get span of bytes, and once to parse
    --        message
    return (KMessage key value)

instance Kafka KMessage where
  encodedSize = var $ \(KMessage key value) ->
      prop encodedSize (0 :: Word32) -- crc
    + prop encodedSize (0 :: Int8)   -- magic byte
    + prop encodedSize (0 :: Int8)   -- compression flags
    + prop encodedSize key
    + prop encodedSize value


-- the Message Set type
--newtype Offset = Offset Int64 deriving (Show, Eq, Ord, Num, Binary, Kafka)
--
--newtype MessageSet = MessgeSet (Vector (Offset, Message)) deriving (Show, Eq)
--
--instance (Kafka a) => Binary (TArray a) where
--  put (array @ (TArray elems)) = do
--    put (encodedSize array)
--    mapM_ put elems
--
--
---- request --
--
--newtype ApiKey = ApiKey Int16 deriving (Show, Eq, Ord, Num, Binary, Kafka)
--newtype ApiVersion = ApiVersion Int16 deriving (Show, Eq, Ord, Num, Binary, Kafka)
--newtype CorrelationId = CorrelationId Int32 deriving (Show, Eq, Ord, Num, Binary, Kafka)
--newtype ClientId = ClientId TString deriving (Show, Eq, Binary, Kafka)
--
--newtype TopicName = TopicName TString deriving (Show, Eq, Binary, Kafka)
--newtype RequiredAcks = RequiredAcks Int16 deriving (Show, Eq, Ord, Num, Binary, Kafka)
--newtype Timeout = Timeout Int32 deriving (Show, Eq, Ord, Num, Binary, Kafka)
--newtype Partition = Partition Int32 deriving (Show, Eq, Ord, Num, Binary, Kafka)
--
--data TopicMetadataRequest = TODO
--
--data ProduceRequest = ProduceRequest
--                    { _requiredAcks :: RequiredAcks
--                    , _timeout :: Timeout
--                    , _topics :: TArray (TopicName, TArray (Partition, MessageSet))
--                    }
--
expect :: (Eq a, Binary a) => a -> String -> Get ()
expect expected msg = do
  actual <- get
  if (actual == expected)
    then return ()
    else fail msg
--
--data Request = Request
--             { _apiKey :: ApiKey
--             , _apiVersion :: ApiVersion
--             , _reqCorrelationId :: CorrelationId
--             , _clientId :: ClientId
--             , _requestBody :: RequestBody
--             }
--             deriving (Show, Eq)
