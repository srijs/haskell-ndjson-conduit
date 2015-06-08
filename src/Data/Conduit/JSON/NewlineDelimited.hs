{-# LANGUAGE Trustworthy #-}

-- |
-- Module     : Data.Conduit.JSON.NewlineDelimited
-- License    : MIT
-- Maintainer : Sam Rijs <srijs@airpost.net>

module Data.Conduit.JSON.NewlineDelimited
  ( -- * Serialization
    serializer
  , valueSerializer
    -- * Parsing
  , parser
  , maybeParser
  , eitherParser
  , valueParser
  , maybeValueParser
  , eitherValueParser
  , resultValueParser
  ) where

import qualified Data.Aeson as A
import qualified Data.Aeson.Types as A
import qualified Data.Aeson.Encode as A
import Data.Attoparsec.ByteString
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as B (toChunks)
import qualified Data.ByteString.Builder as B
import Data.Conduit
import Data.Monoid

carriage = fromIntegral $ fromEnum '\r'
newline = fromIntegral $ fromEnum '\n'

await' f = await >>= maybe (return ()) f

-- | Consumes a stream of serializable values, and provides a stream of bytestrings.
serializer :: (Monad m, A.ToJSON a) => Conduit a m B.ByteString
serializer = mapInput A.toJSON (const Nothing) valueSerializer

-- | Consumes a stream of aeson values, and provides a stream of bytestrings.
valueSerializer :: Monad m => Conduit A.Value m B.ByteString
valueSerializer = await' $ (>> valueSerializer) . yieldBuilder . build
  where yieldBuilder = mapM_ yield . B.toChunks . B.toLazyByteString
        build a = A.encodeToByteStringBuilder a <> B.word8 carriage <> B.word8 newline

-- | Consumes a stream of bytestrings, and provides a stream of parsed values,
--   ignoring all parse errors.
parser :: (Monad m, A.FromJSON a) => Conduit B.ByteString m a
parser = mapOutputMaybe (>>= A.parseMaybe A.parseJSON) maybeValueParser

-- | Consumes a stream of bytestrings, and provides a stream of just parsed values,
--   or nothing on parse error.
maybeParser :: (Monad m, A.FromJSON a) => Conduit B.ByteString m (Maybe a)
maybeParser = mapOutput (>>= A.parseMaybe A.parseJSON) maybeValueParser

-- | Consumes a stream of bytestrings, and provides a stream of right parsed values,
--   or left strings describing the parse error.
eitherParser :: (Monad m, A.FromJSON a) => Conduit B.ByteString m (Either String a)
eitherParser = mapOutput (>>= A.parseEither A.parseJSON) eitherValueParser

-- | Consumes a stream of bytestrings, and provides a stream of aeson values,
--   ignoring all parse errors.
valueParser :: Monad m => Conduit B.ByteString m A.Value
valueParser = mapOutputMaybe maybeResult resultValueParser

-- | Consumes a stream of bytestrings, and provides a stream of just aeson values,
--   or nothing on parse error.
maybeValueParser :: Monad m => Conduit B.ByteString m (Maybe A.Value)
maybeValueParser = mapOutput maybeResult resultValueParser

-- | Consumes a stream of bytestrings, and provides a stream of right aeson values,
--   or left strings describing the parse error.
eitherValueParser :: Monad m => Conduit B.ByteString m (Either String A.Value)
eitherValueParser = mapOutput eitherResult resultValueParser

-- | Consumes a stream of bytestrings, and provides a stream of parse results.
resultValueParser :: Monad m => Conduit B.ByteString m (Result A.Value)
resultValueParser = go (parse A.json B.empty)
  where go r = await' $ (>>= go) . proc . split
          where proc [] = return r
                proc [c] = return (feed r c)
                proc (c:cs) = do
                  yield (feed r c)
                  mapM_ yield (map (parse A.json) (init cs))
                  return (parse A.json (last cs))
        split = B.split newline . B.filter (/=carriage)
