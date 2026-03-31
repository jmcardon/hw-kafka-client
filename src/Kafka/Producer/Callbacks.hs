{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE LambdaCase #-}
module Kafka.Producer.Callbacks
( deliveryCallback
, module X
)
where

import           Control.Exception      (bracket)
import           Foreign.C.Error        (getErrno)
import           Foreign.Ptr            (Ptr, nullPtr)
import           Foreign.Storable       (Storable(peek))
import           Foreign.StablePtr      (castPtrToStablePtr, deRefStablePtr, freeStablePtr)
import           Control.Concurrent     (myThreadId)
import           Kafka.Callbacks        as X
import           Kafka.Consumer.Types   (Offset(..))
import           Kafka.Internal.RdKafka (RdKafkaMessageT(..), RdKafkaRespErrT(..), rdKafkaConfSetDrMsgCb)
import           Kafka.Internal.Setup   (getRdKafkaConf, Callback(..))
import           Kafka.Internal.Shared  (kafkaRespErr, readTopic, readKey, readPayload, readHeaders)
import           Kafka.Producer.Types   (ProducerRecord(..), DeliveryReport(..), ProducePartition(..))
import           Kafka.Types            (KafkaError(..), TopicName(..))
import Data.Either (fromRight)
import Debug.Trace

-- | Sets the callback for delivery reports.
--
--   /Note: A callback should not be a long-running process as it blocks
--   librdkafka from continuing on the thread that handles the delivery
--   callbacks. For callbacks to individual messsages see
--   'Kafka.Producer.produceMessage\''./
--
deliveryCallback :: (DeliveryReport -> IO ()) -> Callback
deliveryCallback callback = Callback $ \kc -> rdKafkaConfSetDrMsgCb (getRdKafkaConf kc) realCb
  where
    realCb :: t -> Ptr RdKafkaMessageT -> IO ()
    realCb _ mptr = do
      tid <- myThreadId
      if mptr == nullPtr
        then do
          traceIO $ "[dr_msg_cb] called with NULL mptr on thread " ++ show tid
          getErrno >>= (callback . NoMessageError . kafkaRespErr)
        else do
          s <- peek mptr
          prodRec <- mkProdRec mptr
          let cbPtr = opaque'RdKafkaMessageT s
          traceIO $ "[dr_msg_cb] called on thread " ++ show tid
            ++ " cbPtr=" ++ show cbPtr
            ++ " err=" ++ show (err'RdKafkaMessageT s)
          callbacks cbPtr $
            if err'RdKafkaMessageT s /= RdKafkaRespErrNoError
              then mkErrorReport s prodRec
              else mkSuccessReport s prodRec

    callbacks cbPtr rep = do
      callback rep
      if cbPtr == nullPtr then do
        traceIO "[dr_msg_cb] cbPtr is NULL — skipping per-message callback"
        pure ()
      else do
        traceIO "[dr_msg_cb] invoking per-message callback via StablePtr"
        bracket (pure $ castPtrToStablePtr cbPtr) freeStablePtr $ \stablePtr -> do
          msgCb <- deRefStablePtr @(DeliveryReport -> IO ()) stablePtr
          msgCb rep

mkErrorReport :: RdKafkaMessageT -> ProducerRecord -> DeliveryReport
mkErrorReport msg prodRec = DeliveryFailure prodRec (KafkaResponseError (err'RdKafkaMessageT msg))

mkSuccessReport :: RdKafkaMessageT -> ProducerRecord -> DeliveryReport
mkSuccessReport msg prodRec = DeliverySuccess prodRec (Offset $ offset'RdKafkaMessageT msg)

mkProdRec :: Ptr RdKafkaMessageT -> IO ProducerRecord
mkProdRec pmsg = do
  msg         <- peek pmsg
  topic       <- readTopic msg
  key         <- readKey msg
  payload     <- readPayload msg
  flip fmap (fromRight mempty <$> readHeaders pmsg) $ \headers ->
    ProducerRecord
      { prTopic = TopicName topic
      , prPartition = SpecifiedPartition (partition'RdKafkaMessageT msg)
      , prKey = key
      , prValue = payload
      , prHeaders = headers
      }
