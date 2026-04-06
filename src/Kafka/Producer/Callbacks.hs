{-# LANGUAGE DataKinds        #-}
{-# LANGUAGE GADTs            #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE LambdaCase #-}
module Kafka.Producer.Callbacks
( deliveryCallback
, installDeliveryCallback
, module X
)
where

import           Control.Exception      (bracket)
import           Foreign.C.Error        (getErrno)
import           Foreign.Ptr            (Ptr, nullPtr)
import           Foreign.Storable       (Storable(peek))
import           Foreign.StablePtr      (castPtrToStablePtr, deRefStablePtr, freeStablePtr)
import           Kafka.Callbacks        as X
import           Kafka.Consumer.Types   (Offset(..))
import           Kafka.Internal.RdKafka (RdKafkaMessageT(..), RdKafkaRespErrT(..), rdKafkaConfSetDrMsgCb)
import           Kafka.Internal.Setup   (getRdKafkaConf, KafkaConf)
import           Kafka.Internal.Shared  (kafkaRespErr, readTopic, readKey, readPayload, readHeaders)
import           Kafka.Producer.Types   (ProducerRecord(..), DeliveryReport(..), DeliveryCallback(..), SupportsCallback(..), ProducePartition(..))
import           Kafka.Types            (KafkaError(..), TopicName(..))
import Data.Either (fromRight)

-- | Creates a 'DeliveryCallback' value that tags the producer as supporting callbacks.
deliveryCallback :: (DeliveryReport -> IO ()) -> DeliveryCallback 'HasCallbacks
deliveryCallback = WithDeliveryCallback

-- | Install a delivery callback on the kafka conf. Only called when 'HasCallbacks'.
installDeliveryCallback :: DeliveryCallback 'HasCallbacks -> KafkaConf -> IO ()
installDeliveryCallback (WithDeliveryCallback callback) kc =
  rdKafkaConfSetDrMsgCb (getRdKafkaConf kc) realCb
  where
    realCb :: t -> Ptr RdKafkaMessageT -> IO ()
    realCb _ mptr =
      if mptr == nullPtr
        then getErrno >>= (callback . NoMessageError . kafkaRespErr)
        else do
          s <- peek mptr
          prodRec <- mkProdRec mptr
          let cbPtr = opaque'RdKafkaMessageT s
          callbacks cbPtr $
            if err'RdKafkaMessageT s /= RdKafkaRespErrNoError
              then mkErrorReport s prodRec
              else mkSuccessReport s prodRec

    callbacks cbPtr rep = do
      callback rep
      if cbPtr == nullPtr then
        pure ()
      else bracket (pure $ castPtrToStablePtr cbPtr) freeStablePtr $ \stablePtr -> do
        msgCb <- deRefStablePtr @(DeliveryReport -> IO ()) stablePtr
        -- Note: if this callback blocks, then librdkafka is essentially blocked.
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
