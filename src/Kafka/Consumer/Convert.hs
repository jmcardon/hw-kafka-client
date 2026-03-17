module Kafka.Consumer.Convert
( offsetSyncToInt
, offsetToInt64
, int64ToOffset
, fromNativeTopicPartitionList''
, fromNativeTopicPartitionList'
, fromNativeTopicPartitionList
, toNativeTopicPartitionList
, toNativeTopicPartitionListNoDispose
, toNativeTopicPartitionList'
, topicPartitionFromMessage
, topicPartitionFromMessageForCommit
, toMap
, fromMessagePtr
, fromMessagePtrZeroCopy
, offsetCommitToBool
)
where

import           Control.Monad          ((>=>))
import qualified Data.ByteString        as BS
import           Data.Either            (fromRight)
import           Data.Int               (Int64)
import           Data.Map.Strict        (Map, fromListWith)
import qualified Data.Set               as S
import qualified Data.Text              as Text
import           Foreign.Ptr            (Ptr, nullPtr)
import           Foreign.ForeignPtr     (withForeignPtr)
import qualified Foreign.Concurrent    as FC
import qualified Data.ByteString.Internal as BSI
import           Foreign.Storable       (Storable(..))
import           Foreign.C.Error        (getErrno)
import           Kafka.Consumer.Types   (ConsumerRecord(..), TopicPartition(..), Offset(..), OffsetCommit(..), PartitionOffset(..), OffsetStoreSync(..))
import           Kafka.Internal.RdKafka
  ( RdKafkaRespErrT(..)
  , RdKafkaMessageT(..)
  , RdKafkaTopicPartitionListTPtr
  , RdKafkaTopicPartitionListT(..)
  , RdKafkaMessageTPtr
  , RdKafkaTopicPartitionT(..)
  , rdKafkaTopicPartitionListAdd
  , newRdKafkaTopicPartitionListT
  , rdKafkaMessageDestroy
  , rdKafkaTopicPartitionListSetOffset
  , rdKafkaTopicPartitionListNew
  , peekCText
  )
import           Kafka.Internal.Shared  (kafkaRespErr, readHeaders, readTopic, readKey, readPayload, readTimestamp)
import           Kafka.Types            (KafkaError(..), PartitionId(..), TopicName(..))

-- | Converts offsets sync policy to integer (the way Kafka understands it):
--
--     * @OffsetSyncDisable == -1@
--
--     * @OffsetSyncImmediate == 0@
--
--     * @OffsetSyncInterval ms == ms@
offsetSyncToInt :: OffsetStoreSync -> Int
offsetSyncToInt sync =
    case sync of
        OffsetSyncDisable     -> -1
        OffsetSyncImmediate   -> 0
        OffsetSyncInterval ms -> ms
{-# INLINE offsetSyncToInt #-}

offsetToInt64 :: PartitionOffset -> Int64
offsetToInt64 o = case o of
    PartitionOffsetBeginning -> -2
    PartitionOffsetEnd       -> -1
    PartitionOffset off      -> off
    PartitionOffsetStored    -> -1000
    PartitionOffsetInvalid   -> -1001
{-# INLINE offsetToInt64 #-}

int64ToOffset :: Int64 -> PartitionOffset
int64ToOffset o
    | o == -2    = PartitionOffsetBeginning
    | o == -1    = PartitionOffsetEnd
    | o == -1000 = PartitionOffsetStored
    | o >= 0     = PartitionOffset o
    | otherwise  = PartitionOffsetInvalid
{-# INLINE int64ToOffset #-}

fromNativeTopicPartitionList'' :: RdKafkaTopicPartitionListTPtr -> IO [TopicPartition]
fromNativeTopicPartitionList'' ptr =
    withForeignPtr ptr $ \fptr -> fromNativeTopicPartitionList' fptr

fromNativeTopicPartitionList' :: Ptr RdKafkaTopicPartitionListT -> IO [TopicPartition]
fromNativeTopicPartitionList' ppl =
    if ppl == nullPtr
        then return []
        else peek ppl >>= fromNativeTopicPartitionList

fromNativeTopicPartitionList :: RdKafkaTopicPartitionListT -> IO [TopicPartition]
fromNativeTopicPartitionList pl =
    let count = cnt'RdKafkaTopicPartitionListT pl
        elems = elems'RdKafkaTopicPartitionListT pl
    in mapM (peekElemOff elems >=> toPart) [0..(fromIntegral count - 1)]
    where
        toPart :: RdKafkaTopicPartitionT -> IO TopicPartition
        toPart p = do
            topic <- peekCText $ topic'RdKafkaTopicPartitionT p
            return TopicPartition {
                tpTopicName = TopicName topic,
                tpPartition = PartitionId $ partition'RdKafkaTopicPartitionT p,
                tpOffset    = int64ToOffset $ offset'RdKafkaTopicPartitionT p
            }

toNativeTopicPartitionList :: [TopicPartition] -> IO RdKafkaTopicPartitionListTPtr
toNativeTopicPartitionList ps = do
    pl <- newRdKafkaTopicPartitionListT (length ps)
    mapM_ (\p -> do
        let TopicName tn = tpTopicName p
            (PartitionId tp) = tpPartition p
            to = offsetToInt64 $ tpOffset p
            tnS = Text.unpack tn
        _ <- rdKafkaTopicPartitionListAdd pl tnS tp
        rdKafkaTopicPartitionListSetOffset pl tnS tp to) ps
    return pl

toNativeTopicPartitionListNoDispose :: [TopicPartition] -> IO RdKafkaTopicPartitionListTPtr
toNativeTopicPartitionListNoDispose ps = do
    pl <- rdKafkaTopicPartitionListNew (length ps)
    mapM_ (\p -> do
        let TopicName tn = tpTopicName p
            (PartitionId tp) = tpPartition p
            to = offsetToInt64 $ tpOffset p
            tnS = Text.unpack tn
        _ <- rdKafkaTopicPartitionListAdd pl tnS tp
        rdKafkaTopicPartitionListSetOffset pl tnS tp to) ps
    return pl

toNativeTopicPartitionList' :: [(TopicName, PartitionId)] -> IO RdKafkaTopicPartitionListTPtr
toNativeTopicPartitionList' tps = do
    let utps = S.toList . S.fromList $ tps
    pl <- newRdKafkaTopicPartitionListT (length utps)
    mapM_ (\(TopicName t, PartitionId p) -> rdKafkaTopicPartitionListAdd pl (Text.unpack t) p) utps
    return pl

topicPartitionFromMessage :: ConsumerRecord k v -> TopicPartition
topicPartitionFromMessage m =
  let (Offset moff) = crOffset m
   in TopicPartition (crTopic m) (crPartition m) (PartitionOffset moff)

-- | Creates a topic partition message for use with the offset commit message.
-- We increment the offset by 1 here because when we commit, the offset is the position
-- the consumer reads from to process the next message.
topicPartitionFromMessageForCommit :: ConsumerRecord k v -> TopicPartition
topicPartitionFromMessageForCommit m =
  case topicPartitionFromMessage m of
    (TopicPartition t p (PartitionOffset moff)) -> TopicPartition t p (PartitionOffset $ moff + 1)
    other                                       -> other

toMap :: Ord k => [(k, v)] -> Map k [v]
toMap kvs = fromListWith (++) [(k, [v]) | (k, v) <- kvs]

fromMessagePtr :: RdKafkaMessageTPtr -> IO (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
fromMessagePtr ptr =
    withForeignPtr ptr $ \realPtr ->
    if realPtr == nullPtr then Left . kafkaRespErr <$> getErrno
    else do
        s <- peek realPtr
        msg <- if err'RdKafkaMessageT s /= RdKafkaRespErrNoError
                then return . Left . KafkaResponseError $ err'RdKafkaMessageT s
                else Right <$> mkRecord s realPtr
        rdKafkaMessageDestroy realPtr
        return msg
    where
        mkRecord msg rptr = do
            topic     <- readTopic msg
            key       <- readKey msg
            payload   <- readPayload msg
            timestamp <- readTimestamp ptr
            headers   <- fromRight mempty <$> readHeaders rptr
            return ConsumerRecord
                { crTopic     = TopicName topic
                , crPartition = PartitionId $ partition'RdKafkaMessageT msg
                , crOffset    = Offset $ offset'RdKafkaMessageT msg
                , crTimestamp = timestamp
                , crHeaders   = headers
                , crKey       = key
                , crValue     = payload
                }

-- | Like 'fromMessagePtr' but avoids copying the message payload.
-- The payload 'BS.ByteString' points directly into librdkafka's buffer.
-- The C message is destroyed when the payload ByteString is garbage collected.
--
-- Small fields (key, topic, headers) are still copied.
--
-- WARNING: Any slice of the payload ByteString (e.g. from protobuf
-- decoding) keeps the entire C message alive. Callers should 'BS.copy'
-- fields they intend to store long-term.
fromMessagePtrZeroCopy :: RdKafkaMessageTPtr -> IO (Either KafkaError (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)))
fromMessagePtrZeroCopy ptr =
    withForeignPtr ptr $ \realPtr ->
    if realPtr == nullPtr then Left . kafkaRespErr <$> getErrno
    else do
        s <- peek realPtr
        if err'RdKafkaMessageT s /= RdKafkaRespErrNoError
            then do
                rdKafkaMessageDestroy realPtr
                return . Left . KafkaResponseError $ err'RdKafkaMessageT s
            else do
                -- Read small fields first (copied — cheap, independent of C message lifetime)
                topic     <- readTopic s
                key       <- readKey s
                timestamp <- readTimestamp ptr
                headers   <- fromRight mempty <$> readHeaders realPtr

                -- Zero-copy payload: wrap C buffer, destroy message when GC'd
                payload <- zeroCopyPayload realPtr s

                return . Right $ ConsumerRecord
                    { crTopic     = TopicName topic
                    , crPartition = PartitionId $ partition'RdKafkaMessageT s
                    , crOffset    = Offset $ offset'RdKafkaMessageT s
                    , crTimestamp = timestamp
                    , crHeaders   = headers
                    , crKey       = key
                    , crValue     = payload
                    }
    where
        zeroCopyPayload :: Ptr RdKafkaMessageT -> RdKafkaMessageT -> IO (Maybe BS.ByteString)
        zeroCopyPayload realPtr s
            | payload'RdKafkaMessageT s == nullPtr = do
                rdKafkaMessageDestroy realPtr
                pure Nothing
            | otherwise = do
                let !payloadPtr = payload'RdKafkaMessageT s
                    !payloadLen = len'RdKafkaMessageT s
                fptr <- FC.newForeignPtr payloadPtr (rdKafkaMessageDestroy realPtr)
                pure . Just $! BSI.BS fptr payloadLen

offsetCommitToBool :: OffsetCommit -> Bool
offsetCommitToBool OffsetCommit      = False
offsetCommitToBool OffsetCommitAsync = True
