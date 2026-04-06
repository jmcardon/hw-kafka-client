{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE LambdaCase                 #-}

-----------------------------------------------------------------------------
-- |
-- Module to produce messages to Kafka topics.
--
-- Here's an example of code to produce messages to a topic:
--
-- @
-- import Control.Exception (bracket)
-- import Control.Monad (forM_)
-- import Data.ByteString (ByteString)
-- import Kafka.Producer
--
-- -- Global producer properties
-- producerProps :: 'ProducerProperties'
-- producerProps = 'brokersList' ["localhost:9092"]
--              <> 'logLevel' 'KafkaLogDebug'
--
-- -- Topic to send messages to
-- targetTopic :: 'TopicName'
-- targetTopic = 'TopicName' "kafka-client-example-topic"
--
-- -- Run an example
-- runProducerExample :: IO ()
-- runProducerExample =
--     bracket mkProducer clProducer runHandler >>= print
--     where
--       mkProducer = 'newProducer' 'NoDeliveryCallback' producerProps
--       clProducer (Left _)     = pure ()
--       clProducer (Right prod) = 'closeProducer' prod
--       runHandler (Left err)   = pure $ Left err
--       runHandler (Right prod) = sendMessages prod
--
-- -- Example sending 2 messages and printing the response from Kafka
-- sendMessages :: 'KafkaProducer' -> IO (Either 'KafkaError' ())
-- sendMessages prod = do
--   err1 <- 'produceMessage' prod (mkMessage Nothing (Just "test from producer") )
--   forM_ err1 print
--
--   err2 <- 'produceMessage' prod (mkMessage (Just "key") (Just "test from producer (with key)"))
--   forM_ err2 print
--
--   pure $ Right ()
--
-- mkMessage :: Maybe ByteString -> Maybe ByteString -> 'ProducerRecord'
-- mkMessage k v = 'ProducerRecord'
--                   { 'prTopic' = targetTopic
--                   , 'prPartition' = 'UnassignedPartition'
--                   , 'prKey' = k
--                   , 'prValue' = v
--                   }
-- @
-----------------------------------------------------------------------------
module Kafka.Producer
( KafkaProducer
, module X
, runProducer
, newProducer
, produceMessage
, produceMessage'
, produceMessageNoPoll
, produceMessageNoPoll'
, flushProducer
, closeProducer
, pollEvents
, outboundQueueLength
, flushProducerWithTimeout
, RdKafkaRespErrT (..)
)
where

import           Control.Exception        (bracket)
import           Control.Monad            (forM_)
import           Control.Monad.IO.Class   (MonadIO (liftIO))
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.Text                as Text
import           Foreign.C.String         (withCString)
import           Foreign.ForeignPtr       (newForeignPtr_, withForeignPtr)
import           Foreign.Marshal.Alloc    (allocaBytes)
import           Foreign.Ptr              (Ptr, nullPtr, plusPtr)
import           Foreign.StablePtr        (newStablePtr, castStablePtrToPtr, freeStablePtr)
import           Foreign.Storable         (Storable(sizeOf), pokeElemOff)
import           Kafka.Internal.RdKafka   (RdKafkaRespErrT (..), RdKafkaTypeT (..), RdKafkaVuT(..), newRdKafkaT, rdKafkaErrorCode
                                          , rdKafkaErrorDestroy, rdKafkaOutqLen, rdKafkaMessageProduceVa', rdKafkaSetLogLevel
                                          , rdKafkaFlush)
import           Kafka.Internal.Setup     (Kafka (..), KafkaConf (..), KafkaProps (..), TopicProps (..), kafkaConf, topicConf, Callback(..))
import           Kafka.Internal.Shared    (pollEvents)
import           Kafka.Producer.Convert   (copyMsgFlags, handleProduceErrT, producePartitionCInt)
import           Kafka.Producer.Types     (KafkaProducer (..))


import Kafka.Producer.ProducerProperties as X
import Kafka.Producer.Types              as X hiding (KafkaProducer)
import Kafka.Types                       as X
import Control.Exception.Base (bracketOnError)

-- | Runs Kafka Producer.
-- The callback provided is expected to call 'produceMessage'
-- to send messages to Kafka.
{-# DEPRECATED runProducer "Use 'newProducer'/'closeProducer' instead" #-}
runProducer :: ProducerProperties
            -> (KafkaProducer 'NoCallbacks -> IO (Either KafkaError a))
            -> IO (Either KafkaError a)
runProducer props f =
  bracket mkProducer clProducer runHandler
  where
    mkProducer = newProducer NoDeliveryCallback props

    clProducer (Left _)     = return ()
    clProducer (Right prod) = closeProducer prod

    runHandler (Left err)   = return $ Left err
    runHandler (Right prod) = f prod

-- | Creates a new kafka producer.
-- A newly created producer must be closed with 'closeProducer' function.
--
-- The 'DeliveryCallback' argument determines whether the producer supports
-- per-message delivery report callbacks:
--
--   * 'NoDeliveryCallback' — no delivery reports, lighter weight
--   * 'WithDeliveryCallback' @cb@ — installs @cb@ as the global delivery callback
newProducer :: MonadIO m => DeliveryCallback s -> ProducerProperties -> m (Either KafkaError (KafkaProducer s))
newProducer dcb pps = liftIO $ do
  kc@(KafkaConf kc' _ _) <- kafkaConf (KafkaProps $ (ppKafkaProps pps))
  tc <- topicConf (TopicProps $ (ppTopicProps pps))

  -- install delivery callback only when HasCallbacks
  case dcb of
    NoDeliveryCallback     -> pure ()
    WithDeliveryCallback _ -> installDeliveryCallback dcb kc

  -- set other callbacks
  forM_ (ppCallbacks pps) (\(Callback setCb) -> setCb kc)

  mbKafka <- newRdKafkaT RdKafkaProducer kc'
  case mbKafka of
    Left err    -> return . Left $ KafkaError err
    Right kafka -> do
      forM_ (ppLogLevel pps) (rdKafkaSetLogLevel kafka . fromEnum)
      let prod = KafkaProducer (Kafka kafka) kc tc
      return (Right prod)

-- | Sends a single message.
-- Since librdkafka is backed by a queue, this function can return before messages are sent. See
-- 'flushProducer' to wait for queue to empty.
produceMessage :: MonadIO m
               => KafkaProducer s
               -> ProducerRecord
               -> m (Maybe KafkaError)
produceMessage kp m = liftIO $ do
  pollEvents kp . Just . Timeout $ 0
  produceMessageNoPoll kp m

-- | Sends a single message with a registered callback.
--
--   The callback can be a long running process, as it is forked by the thread
--   that handles the delivery reports.
produceMessage' :: MonadIO m
                => KafkaProducer 'HasCallbacks
                -> ProducerRecord
                -> (DeliveryReport -> IO ())
                -> m (Either ImmediateError ())
produceMessage' kp msg cb = liftIO $ do
  pollEvents kp . Just . Timeout $ 0
  produceMessageNoPoll' kp msg cb

-- | Like 'produceMessage' but does NOT call 'pollEvents'.
-- Use this with a dedicated polling thread for high-throughput production.
-- Without polling, delivery report callbacks will not fire until
-- 'pollEvents' or 'flushProducer' is called (e.g. from a poller thread).
produceMessageNoPoll :: MonadIO m
                     => KafkaProducer s
                     -> ProducerRecord
                     -> m (Maybe KafkaError)
produceMessageNoPoll kp m =
  produceMessageImpl kp m Nothing >>= \case
    Right () -> pure Nothing
    Left (ImmediateError err) -> pure (Just err)

-- | Like 'produceMessage'' but does NOT call 'pollEvents'.
-- Use this with a dedicated polling thread for high-throughput production.
produceMessageNoPoll' :: MonadIO m
                      => KafkaProducer 'HasCallbacks
                      -> ProducerRecord
                      -> (DeliveryReport -> IO ())
                      -> m (Either ImmediateError ())
produceMessageNoPoll' kp msg cb = produceMessageImpl kp msg (Just cb)

produceMessageImpl :: MonadIO m
                   => KafkaProducer s
                   -> ProducerRecord
                   -> Maybe (DeliveryReport -> IO ())
                   -> m (Either ImmediateError ())
produceMessageImpl (KafkaProducer (Kafka k) _ _) msg mcb = liftIO $
  withBS (prValue msg) $ \payloadPtr payloadLength ->
    withBS (prKey msg) $ \keyPtr keyLength ->
      withCString (Text.unpack . unTopicName . prTopic $ msg) $ \topicName ->
        withMaybeCallback mcb $ \opaquePtr -> do
          let hdrs = headersToList (prHeaders msg)
              nHeaders = length hdrs
              hasOpaque = opaquePtr /= nullPtr
              nTotal = (if hasOpaque then 6 else 5) + nHeaders
          allocaBytes (nTotal * sizeOf (undefined :: RdKafkaVuT)) $ \arrPtr -> do
            pokeElemOff arrPtr 0 $ Topic'RdKafkaVu topicName
            pokeElemOff arrPtr 1 $ Partition'RdKafkaVu (producePartitionCInt $ prPartition msg)
            pokeElemOff arrPtr 2 $ MsgFlags'RdKafkaVu (fromIntegral copyMsgFlags)
            pokeElemOff arrPtr 3 $ Value'RdKafkaVu payloadPtr (fromIntegral payloadLength)
            pokeElemOff arrPtr 4 $ Key'RdKafkaVu keyPtr (fromIntegral keyLength)
            let hdrStart
                  | hasOpaque = do
                      pokeElemOff arrPtr 5 $ Opaque'RdKafkaVu opaquePtr
                      pure 6
                  | otherwise = pure 5
            idx <- hdrStart
            pokeHeaders hdrs arrPtr idx $ do
              fptr <- newForeignPtr_ arrPtr
              code <- bracket (rdKafkaMessageProduceVa' k fptr (fromIntegral nTotal)) rdKafkaErrorDestroy rdKafkaErrorCode
              handleProduceErrT code >>= \case
                Just err -> pure . Left . ImmediateError $ err
                Nothing  -> pure . Right $ ()
  where
    pokeHeaders [] _ _ action = action
    pokeHeaders ((nm, val):rest) arrPtr idx action =
      BS.useAsCString nm $ \cnm ->
        withBS (Just val) $ \vp vl -> do
          pokeElemOff arrPtr idx $ Header'RdKafkaVu cnm vp (fromIntegral vl)
          pokeHeaders rest arrPtr (idx + 1) action

    withMaybeCallback Nothing f = f nullPtr
    withMaybeCallback (Just cb) f =
      bracketOnError (newStablePtr cb) freeStablePtr $ \callbackPtr -> do
        res <- f (castStablePtrToPtr callbackPtr)
        case res of
          Left _ -> freeStablePtr callbackPtr >> pure res
          Right _ -> pure res
{-# INLINABLE produceMessageImpl #-}

-- | Closes the producer.
-- Will wait until the outbound queue is drained before returning the control.
closeProducer :: MonadIO m => KafkaProducer s -> m ()
closeProducer = flushProducer

-- | Drains the outbound queue for a producer.
--  This function is also called automatically when the producer is closed
-- with 'closeProducer' to ensure that all queued messages make it to Kafka.
flushProducer :: MonadIO m => KafkaProducer s -> m ()
flushProducer kp = liftIO $ do
    pollEvents kp (Just $ Timeout 100)
    l <- outboundQueueLength (kpKafkaPtr kp)
    if l == 0
      then pollEvents kp (Just $ Timeout 0) -- to be sure that all the delivery reports are fired
      else flushProducer kp

flushProducerWithTimeout :: MonadIO m => KafkaProducer s -> Timeout -> m KafkaFlushResult
flushProducerWithTimeout (KafkaProducer (Kafka k) _ _) (Timeout timeout) =
  liftIO (rdKafkaFlush k timeout) >>= \case
    RdKafkaRespErrTimedOut -> pure KafkaFlushTimedOut
    _ -> pure KafkaFlushOk
------------------------------------------------------------------------------------


withBS :: Maybe BS.ByteString -> (Ptr a -> Int -> IO b) -> IO b
withBS Nothing f = f nullPtr 0
withBS (Just bs) f =
    let (d, o, l) = BSI.toForeignPtr bs
    in  withForeignPtr d $ \p -> f (p `plusPtr` o) l

outboundQueueLength :: Kafka -> IO Int
outboundQueueLength (Kafka k) = rdKafkaOutqLen k
