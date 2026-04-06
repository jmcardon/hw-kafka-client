{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
module Kafka.MockClusterSpec
( spec
) where

import Control.Exception (bracket)
import Data.ByteString (ByteString)
import Data.List (sortOn)
import qualified Data.Map as Map
import qualified Data.Text as Text

import Kafka.Consumer as C
import Kafka.Internal.RdKafka
    ( RdKafkaMockClusterTPtr
    , withHandleMockCluster
    , rdKafkaMockClusterBootstraps
    , rdKafkaMockBrokerSetDown
    , rdKafkaMockBrokerSetUp
    )
import Kafka.Internal.Setup (Kafka(..))
import Kafka.Producer as P
import Kafka.Producer.Types (KafkaProducer(..))

import Test.Hspec

mockTopic :: TopicName
mockTopic = TopicName "mock-test-topic"

mockProducerProps :: ProducerProperties
mockProducerProps =
     P.brokersList ["localhost:9092"]
  <> P.extraProps (Map.singleton "test.mock.num.brokers" "3")

withMockProducer :: (KafkaProducer 'NoCallbacks -> IO a) -> IO a
withMockProducer = bracket acquire closeProducer
  where
    acquire =
      newProducer NoDeliveryCallback mockProducerProps >>= either (error . ("Mock producer: " <>) . show) pure

mkMessage :: Maybe ByteString -> Maybe ByteString -> ProducerRecord
mkMessage k v = ProducerRecord
  { prTopic     = mockTopic
  , prPartition = UnassignedPartition
  , prKey       = k
  , prValue     = v
  , prHeaders   = mempty
  }

mkMessageWithHeaders :: Maybe ByteString -> Maybe ByteString -> Headers -> ProducerRecord
mkMessageWithHeaders k v h = ProducerRecord
  { prTopic     = mockTopic
  , prPartition = UnassignedPartition
  , prKey       = k
  , prValue     = v
  , prHeaders   = h
  }

getMockBootstraps :: KafkaProducer s -> IO String
getMockBootstraps (KafkaProducer (Kafka rk) _ _) =
  withHandleMockCluster rk rdKafkaMockClusterBootstraps >>= \case
    Nothing -> error "No mock cluster on this rd_kafka_t"
    Just bs -> pure bs

withMockConsumer :: KafkaProducer s -> (KafkaConsumer -> IO a) -> IO a
withMockConsumer producer f = do
  bootstraps <- getMockBootstraps producer
  let props = C.brokersList [BrokerAddress (Text.pack bootstraps)]
           <> groupId (ConsumerGroupId "mock-test-group")
           <> noAutoCommit
      sub = topics [mockTopic] <> offsetReset Earliest
  bracket
    (newConsumer props sub >>= either (error . ("Mock consumer: " <>) . show) pure)
    (\c -> closeConsumer c >> pure ())
    f

-- | Run an action against the mock cluster owned by a producer.
withProducerMockCluster :: KafkaProducer s -> (RdKafkaMockClusterTPtr -> IO a) -> IO a
withProducerMockCluster (KafkaProducer (Kafka rk) _ _) f =
  withHandleMockCluster rk f >>= \case
    Nothing -> error "No mock cluster on this rd_kafka_t"
    Just a  -> pure a

-- | Poll up to @n@ messages, retrying until we get them all or timeout.
pollMessages :: KafkaConsumer -> Int -> IO [ConsumerRecord (Maybe ByteString) (Maybe ByteString)]
pollMessages consumer n = go n []
  where
    go 0 acc = pure (reverse acc)
    go remaining acc = do
      msg <- pollMessage consumer (Timeout 5000)
      case msg of
        Left _   -> pure (reverse acc)  -- no more messages
        Right cr -> go (remaining - 1) (cr : acc)

spec :: Spec
spec = describe "Mock Cluster" $ do

  -- Basic produce tests
  it "can produce a message without error" $
    withMockProducer $ \producer -> do
      err <- produceMessage producer (mkMessage Nothing (Just "hello mock"))
      err `shouldBe` Nothing

  it "can produce and flush without error" $
    withMockProducer $ \producer -> do
      _ <- produceMessage producer (mkMessage (Just "key") (Just "value"))
      res <- flushProducerWithTimeout producer (Timeout 5000)
      res `shouldBe` KafkaFlushOk

  it "can produce multiple messages and flush" $
    withMockProducer $ \producer -> do
      e1 <- produceMessage producer (mkMessage (Just "k1") (Just "v1"))
      e2 <- produceMessage producer (mkMessage (Just "k2") (Just "v2"))
      e3 <- produceMessage producer (mkMessage Nothing (Just "v3"))
      e1 `shouldBe` Nothing
      e2 `shouldBe` Nothing
      e3 `shouldBe` Nothing
      res <- flushProducerWithTimeout producer (Timeout 5000)
      res `shouldBe` KafkaFlushOk

  -- Produce + consume roundtrip
  it "can produce and consume via shared mock cluster" $
    withMockProducer $ \producer -> do
      _ <- produceMessage producer (mkMessage (Just "test-key") (Just "test-value"))
      _ <- flushProducerWithTimeout producer (Timeout 5000)

      withMockConsumer producer $ \consumer -> do
        msg <- pollMessage consumer (Timeout 10000)
        case msg of
          Left err -> expectationFailure $ "Expected message, got: " <> show err
          Right cr -> do
            crKey cr   `shouldBe` Just "test-key"
            crValue cr `shouldBe` Just "test-value"

  -- Headers roundtrip
  it "preserves headers through produce and consume" $
    withMockProducer $ \producer -> do
      let hdrs = headersFromList [("x-trace-id", "abc123"), ("x-source", "test")]
          msg  = mkMessageWithHeaders (Just "hdr-key") (Just "hdr-val") hdrs
      _ <- produceMessage producer msg
      _ <- flushProducerWithTimeout producer (Timeout 5000)

      withMockConsumer producer $ \consumer -> do
        result <- pollMessage consumer (Timeout 10000)
        case result of
          Left err -> expectationFailure $ "Expected message, got: " <> show err
          Right cr -> do
            crKey cr   `shouldBe` Just "hdr-key"
            crValue cr `shouldBe` Just "hdr-val"
            let receivedHeaders = sortOn fst (headersToList (crHeaders cr))
            receivedHeaders `shouldBe` sortOn fst [("x-trace-id", "abc123"), ("x-source", "test")]

  -- Multiple message consumption
  it "can consume multiple messages" $
    withMockProducer $ \producer -> do
      _ <- produceMessage producer (mkMessage (Just "k1") (Just "v1"))
      _ <- produceMessage producer (mkMessage (Just "k2") (Just "v2"))
      _ <- produceMessage producer (mkMessage (Just "k3") (Just "v3"))
      _ <- flushProducerWithTimeout producer (Timeout 5000)

      withMockConsumer producer $ \consumer -> do
        msgs <- pollMessages consumer 3
        length msgs `shouldBe` 3
        let values = sortOn id [v | cr <- msgs, Just v <- [crValue cr]]
        values `shouldBe` ["v1", "v2", "v3"]

  -- Consumer poll timeout (no messages)
  it "returns timeout when no messages available" $
    withMockProducer $ \producer ->
      withMockConsumer producer $ \consumer -> do
        result <- pollMessage consumer (Timeout 500)
        case result of
          Left (KafkaResponseError RdKafkaRespErrTimedOut) -> pure ()
          Left err -> expectationFailure $ "Expected timeout, got: " <> show err
          Right _  -> expectationFailure "Expected timeout, got a message"

  -- Flush timeout with brokers down
  it "returns flush timeout when all brokers are down" $
    withMockProducer $ \producer -> do
      withProducerMockCluster producer $ \mc -> do
        -- Take all 3 brokers down
        _ <- rdKafkaMockBrokerSetDown mc 1
        _ <- rdKafkaMockBrokerSetDown mc 2
        _ <- rdKafkaMockBrokerSetDown mc 3

        -- Produce a message (queued locally, can't reach brokers)
        _ <- produceMessage producer (mkMessage (Just "lost") (Just "message"))

        -- Flush should time out since no brokers are reachable
        res <- flushProducerWithTimeout producer (Timeout 1000)
        res `shouldBe` KafkaFlushTimedOut

        -- Bring brokers back up for clean shutdown
        _ <- rdKafkaMockBrokerSetUp mc 1
        _ <- rdKafkaMockBrokerSetUp mc 2
        _ <- rdKafkaMockBrokerSetUp mc 3
        pure ()
