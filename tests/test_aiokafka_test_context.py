import asyncio
import unittest
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, TopicPartition, ConsumerRecord

from aiokafka_test.src.aiokafka_test import (
    aiokafka_test_context,
    MessageBroker,
    message_broker,
)


class TestAiokafkaTestContext(unittest.IsolatedAsyncioTestCase):

    # ==================== 1. Basic scenarios ====================

    async def test_context_manager_yields_broker(self):
        """Context manager yields a MessageBroker instance."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            self.assertIsInstance(broker, MessageBroker)
            await producer.stop()

    async def test_context_manager_patches_send(self):
        """AIOKafkaProducer.send is replaced with mock_send inside context."""
        original_send = AIOKafkaProducer.send
        with aiokafka_test_context():
            producer = AIOKafkaProducer()
            self.assertIsNot(AIOKafkaProducer.send, original_send)
            await producer.stop()
        self.assertIs(AIOKafkaProducer.send, original_send)

    async def test_context_manager_patches_start(self):
        """AIOKafkaProducer.start is replaced with mock_start inside context."""
        original_start = AIOKafkaProducer.start
        with aiokafka_test_context():
            producer = AIOKafkaProducer()
            self.assertIsNot(AIOKafkaProducer.start, original_start)
            await producer.stop()
        self.assertIs(AIOKafkaProducer.start, original_start)

    # ==================== 2. Message sending ====================

    async def test_send_message_with_value(self):
        """Sending a message with value - message appears in the broker."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            await producer.send("test-topic", value=b"hello")
            messages = broker.messages["test-topic"]
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0].value, b"hello")
            await producer.stop()

    async def test_send_message_with_key_and_value(self):
        """Sending with key and value - both fields are preserved."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            await producer.send("test-topic", value=b"hello", key=b"key1")
            messages = broker.messages["test-topic"]
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0].value, b"hello")
            self.assertEqual(messages[0].key, b"key1")
            await producer.stop()

    async def test_send_message_with_headers(self):
        """Sending with headers - headers are preserved."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            headers = [("content-type", b"json"), ("trace-id", b"abc123")]
            await producer.send("test-topic", value=b"data", headers=headers)
            messages = broker.messages["test-topic"]
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0].headers, headers)
            await producer.stop()

    async def test_send_message_with_partition(self):
        """Sending with partition - partition number is preserved."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            await producer.send("test-topic", value=b"data", partition=2)
            messages = broker.messages["test-topic"]
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0].partition, 2)
            await producer.stop()

    async def test_send_message_with_timestamp(self):
        """Sending with timestamp - timestamp is preserved."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            ts = 1700000000000
            await producer.send("test-topic", value=b"data", timestamp=ts)
            messages = broker.messages["test-topic"]
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0].timestamp_ms, ts)
            await producer.stop()

    async def test_send_message_all_fields(self):
        """Sending with all fields at once."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            headers = [("key", b"value")]
            await producer.send(
                "test-topic",
                value=b"full-data",
                key=b"key1",
                partition=1,
                timestamp=1700000000000,
                headers=headers,
            )
            messages = broker.messages["test-topic"]
            self.assertEqual(len(messages), 1)
            msg = messages[0]
            self.assertEqual(msg.topic, "test-topic")
            self.assertEqual(msg.value, b"full-data")
            self.assertEqual(msg.key, b"key1")
            self.assertEqual(msg.partition, 1)
            self.assertEqual(msg.timestamp_ms, 1700000000000)
            self.assertEqual(msg.headers, headers)
            await producer.stop()

    # ==================== 3. Broker management ====================

    async def test_broker_topics_after_send(self):
        """broker.topics() returns the list of topics after sending."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            await producer.send("topic-a", value=b"data")
            await producer.send("topic-b", value=b"data")
            topics = broker.topics()
            self.assertIn("topic-a", topics)
            self.assertIn("topic-b", topics)
            await producer.stop()

    async def test_broker_topics_empty_initially(self):
        """broker.topics() is empty before sending messages."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            topics = broker.topics()
            self.assertEqual(topics, [])
            await producer.stop()

    async def test_multiple_messages_same_topic(self):
        """Multiple messages to the same topic accumulate."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            for i in range(5):
                await producer.send("counter", value=f"msg-{i}".encode())
            messages = broker.messages["counter"]
            self.assertEqual(len(messages), 5)
            for i, msg in enumerate(messages):
                self.assertEqual(msg.value, f"msg-{i}".encode())
            await producer.stop()

    async def test_messages_different_topics(self):
        """Messages to different topics are distributed correctly."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            await producer.send("orders", value=b"order-1")
            await producer.send("orders", value=b"order-2")
            await producer.send("payments", value=b"pay-1")
            self.assertEqual(len(broker.messages["orders"]), 2)
            self.assertEqual(len(broker.messages["payments"]), 1)
            await producer.stop()

    async def test_broker_reset_after_context_exit(self):
        """Broker is reset after exiting the context."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            await producer.send("test-topic", value=b"data")
            self.assertEqual(len(broker.messages["test-topic"]), 1)
            await producer.stop()

        self.assertEqual(message_broker.topics(), [])

    async def test_original_methods_restored_after_exit(self):
        """Original send/start are restored after exiting the context."""
        original_send = AIOKafkaProducer.send
        original_start = AIOKafkaProducer.start
        self.assertIsNot(original_send, None)
        self.assertIsNot(original_start, None)

        with aiokafka_test_context():
            producer = AIOKafkaProducer()
            await producer.stop()

        self.assertIs(AIOKafkaProducer.send, original_send)
        self.assertIs(AIOKafkaProducer.start, original_start)

    # ==================== 4. Reuse ====================

    async def test_reenter_context_fresh_broker(self):
        """Re-entering the context gives a fresh broker."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            await producer.send("topic", value=b"first")
            self.assertEqual(len(broker.messages["topic"]), 1)
            await producer.stop()

        with aiokafka_test_context() as broker:
            self.assertEqual(broker.topics(), [])
            producer = AIOKafkaProducer()
            await producer.send("topic", value=b"second")
            self.assertEqual(len(broker.messages["topic"]), 1)
            self.assertEqual(broker.messages["topic"][0].value, b"second")
            await producer.stop()

    async def test_multiple_contexts_independent(self):
        """Multiple sequential contexts do not affect each other."""
        for iteration in range(3):
            with aiokafka_test_context() as broker:
                producer = AIOKafkaProducer()
                await producer.send(
                    f"topic-{iteration}", value=f"iter-{iteration}".encode()
                )
                topics = broker.topics()
                self.assertEqual(len(topics), 1)
                self.assertEqual(f"topic-{iteration}", topics[0])
                await producer.stop()

    # ==================== 5. Edge cases ====================

    async def test_send_none_value(self):
        """Sending value=None does not raise errors."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            await producer.send("test-topic", value=None)
            messages = broker.messages["test-topic"]
            self.assertEqual(len(messages), 1)
            self.assertIsNone(messages[0].value)
            await producer.stop()

    async def test_send_none_key(self):
        """Sending key=None does not raise errors."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            await producer.send("test-topic", value=b"data", key=None)
            messages = broker.messages["test-topic"]
            self.assertEqual(len(messages), 1)
            self.assertIsNone(messages[0].key)
            await producer.stop()

    async def test_send_empty_headers(self):
        """Sending with empty headers."""
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            await producer.send("test-topic", value=b"data", headers=[])
            messages = broker.messages["test-topic"]
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0].headers, [])
            await producer.stop()

    # ==================== 6. Producer created before context manager ====================

    async def test_send_with_producer_created_before_context(self):
        """Producer created before context - send works correctly."""
        producer = AIOKafkaProducer()
        try:
            with aiokafka_test_context() as broker:
                await producer.send("test-topic", value=b"hello")
                messages = broker.messages["test-topic"]
                self.assertEqual(len(messages), 1)
                self.assertEqual(messages[0].value, b"hello")
        finally:
            await producer.stop()

    async def test_send_with_producer_created_before_context_key(self):
        """Producer before context - sending with key."""
        producer = AIOKafkaProducer()
        try:
            with aiokafka_test_context() as broker:
                await producer.send("test-topic", value=b"data", key=b"key1")
                messages = broker.messages["test-topic"]
                self.assertEqual(len(messages), 1)
                self.assertEqual(messages[0].key, b"key1")
        finally:
            await producer.stop()

    async def test_send_with_producer_created_before_context_headers(self):
        """Producer before context - sending with headers."""
        producer = AIOKafkaProducer()
        try:
            with aiokafka_test_context() as broker:
                headers = [("content-type", b"json")]
                await producer.send("test-topic", value=b"data", headers=headers)
                messages = broker.messages["test-topic"]
                self.assertEqual(len(messages), 1)
                self.assertEqual(messages[0].headers, headers)
        finally:
            await producer.stop()

    async def test_send_with_producer_created_before_context_multiple(self):
        """Producer before context - multiple messages."""
        producer = AIOKafkaProducer()
        try:
            with aiokafka_test_context() as broker:
                await producer.send("topic-a", value=b"msg-1")
                await producer.send("topic-a", value=b"msg-2")
                await producer.send("topic-b", value=b"msg-3")
                self.assertEqual(len(broker.messages["topic-a"]), 2)
                self.assertEqual(len(broker.messages["topic-b"]), 1)
        finally:
            await producer.stop()

    async def test_send_with_producer_created_before_context_reset(self):
        """Producer before context - broker is reset after exit."""
        producer = AIOKafkaProducer()
        try:
            with aiokafka_test_context() as broker:
                await producer.send("test-topic", value=b"data")
                self.assertEqual(len(broker.messages["test-topic"]), 1)
        finally:
            await producer.stop()

        self.assertEqual(message_broker.topics(), [])

    async def test_send_with_producer_created_before_context_reenter(self):
        """Producer before context - re-entering gives a fresh broker."""
        producer = AIOKafkaProducer()
        try:
            with aiokafka_test_context() as broker:
                await producer.send("topic", value=b"first")
                self.assertEqual(len(broker.messages["topic"]), 1)

            with aiokafka_test_context() as broker:
                self.assertEqual(broker.topics(), [])
                await producer.send("topic", value=b"second")
                self.assertEqual(broker.messages["topic"][0].value, b"second")
        finally:
            await producer.stop()

    async def test_send_with_producer_created_before_context_none_value(self):
        """Producer before context - sending value=None."""
        producer = AIOKafkaProducer()
        try:
            with aiokafka_test_context() as broker:
                await producer.send("test-topic", value=None)
                messages = broker.messages["test-topic"]
                self.assertEqual(len(messages), 1)
                self.assertIsNone(messages[0].value)
        finally:
            await producer.stop()

    async def test_send_with_producer_created_before_context_none_key(self):
        """Producer before context - sending key=None."""
        producer = AIOKafkaProducer()
        try:
            with aiokafka_test_context() as broker:
                await producer.send("test-topic", value=b"data", key=None)
                messages = broker.messages["test-topic"]
                self.assertEqual(len(messages), 1)
                self.assertIsNone(messages[0].key)
        finally:
            await producer.stop()

    # ==================== 7. Basic Consumer Context Manager ====================

    async def test_context_manager_patches_consumer_start(self):
        """AIOKafkaConsumer.start is replaced with mock_start inside context."""
        original_start = AIOKafkaConsumer.start
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context():
                self.assertIsNot(AIOKafkaConsumer.start, original_start)
        finally:
            await consumer.stop()

    async def test_context_manager_patches_consumer_getone(self):
        """AIOKafkaConsumer.getone is replaced with mock_getone inside context."""
        original_getone = AIOKafkaConsumer.getone
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context():
                self.assertIsNot(AIOKafkaConsumer.getone, original_getone)
        finally:
            await consumer.stop()

    async def test_consumer_context_manager_yields_broker(self):
        """Context manager yields a MessageBroker instance when used with consumer."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                self.assertIsInstance(broker, MessageBroker)
        finally:
            await consumer.stop()

    # ==================== 8. Consumer Receiving Records ====================

    async def test_consumer_receives_single_record(self):
        """Consumer receives a single record added via broker.add_record()."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=0,
                        offset=0,
                        timestamp=1700000000000,
                        timestamp_type=0,
                        value=b"hello",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=5,
                        headers=[],
                    )
                )
                record = await consumer.getone()
                self.assertEqual(record.topic, "test-topic")
                self.assertEqual(record.value, b"hello")
        finally:
            await consumer.stop()

    async def test_consumer_receives_record_with_value(self):
        """Consumer receives record and value field is correctly retrieved."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"test-value",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=10,
                        headers=[],
                    )
                )
                record = await consumer.getone()
                self.assertEqual(record.value, b"test-value")
        finally:
            await consumer.stop()

    async def test_consumer_receives_record_with_key(self):
        """Consumer receives record and key field is correctly retrieved."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"data",
                        key=b"test-key",
                        checksum=None,
                        serialized_key_size=8,
                        serialized_value_size=4,
                        headers=[],
                    )
                )
                record = await consumer.getone()
                self.assertEqual(record.key, b"test-key")
        finally:
            await consumer.stop()

    async def test_consumer_receives_record_with_headers(self):
        """Consumer receives record and headers field is correctly retrieved."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                headers = [("content-type", b"json"), ("trace-id", b"abc123")]
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"data",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=4,
                        headers=headers,
                    )
                )
                record = await consumer.getone()
                self.assertEqual(record.headers, headers)
        finally:
            await consumer.stop()

    async def test_consumer_receives_record_all_fields(self):
        """Consumer receives record with all fields correctly populated."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                headers = [("key", b"value")]
                await broker.add_record(
                    ConsumerRecord(
                        topic="full-topic",
                        partition=2,
                        offset=42,
                        timestamp=1700000000000,
                        timestamp_type=0,
                        value=b"full-data",
                        key=b"full-key",
                        checksum=None,
                        serialized_key_size=8,
                        serialized_value_size=9,
                        headers=headers,
                    )
                )
                record = await consumer.getone()
                self.assertEqual(record.topic, "full-topic")
                self.assertEqual(record.partition, 2)
                self.assertEqual(record.value, b"full-data")
                self.assertEqual(record.key, b"full-key")
                self.assertEqual(record.timestamp, 1700000000000)
                self.assertEqual(record.offset, 42)
                self.assertEqual(record.headers, headers)
        finally:
            await consumer.stop()

    async def test_consumer_receives_multiple_records(self):
        """Consumer receives multiple records added via broker.add_record()."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                for i in range(5):
                    await broker.add_record(
                        ConsumerRecord(
                            topic="multi-topic",
                            partition=0,
                            offset=i,
                            timestamp=0,
                            timestamp_type=0,
                            value=f"msg-{i}".encode(),
                            key=None,
                            checksum=None,
                            serialized_key_size=0,
                            serialized_value_size=len(f"msg-{i}".encode()),
                            headers=[],
                        )
                    )
                for i in range(5):
                    record = await consumer.getone()
                    self.assertEqual(record.value, f"msg-{i}".encode())
        finally:
            await consumer.stop()

    # ==================== 9. Consumer with Partition Filtering ====================

    async def test_consumer_getone_with_partition_filter(self):
        """Consumer.getone with TopicPartition filter retrieves correct record."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=1,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"partitioned-data",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=16,
                        headers=[],
                    )
                )
                tp = TopicPartition(topic="test-topic", partition=1)
                record = await consumer.getone(tp)
                self.assertEqual(record.value, b"partitioned-data")
                self.assertEqual(record.partition, 1)
        finally:
            await consumer.stop()

    async def test_consumer_getone_filters_by_topic(self):
        """Consumer.getone filters records by topic when using TopicPartition."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="topic-a",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"data-a",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=6,
                        headers=[],
                    )
                )
                await broker.add_record(
                    ConsumerRecord(
                        topic="topic-b",
                        partition=0,
                        offset=1,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"data-b",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=6,
                        headers=[],
                    )
                )
                tp = TopicPartition(topic="topic-a", partition=0)
                record = await consumer.getone(tp)
                self.assertEqual(record.topic, "topic-a")
                self.assertEqual(record.value, b"data-a")
        finally:
            await consumer.stop()

    async def test_consumer_getone_mismatched_partition(self):
        """Consumer.getone with mismatched partition gets next matching record."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"data-p0",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=6,
                        headers=[],
                    )
                )
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=1,
                        offset=1,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"data-p1",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=6,
                        headers=[],
                    )
                )
                tp = TopicPartition(topic="test-topic", partition=1)
                record = await consumer.getone(tp)
                self.assertEqual(record.value, b"data-p1")
                self.assertEqual(record.partition, 1)
        finally:
            await consumer.stop()

    async def test_consumer_getone_multiple_partitions(self):
        """Consumer.getone with multiple TopicPartition arguments."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"data-p0",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=6,
                        headers=[],
                    )
                )
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=2,
                        offset=1,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"data-p2",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=6,
                        headers=[],
                    )
                )
                tp0 = TopicPartition(topic="test-topic", partition=0)
                tp2 = TopicPartition(topic="test-topic", partition=2)
                record = await consumer.getone(tp0, tp2)
                self.assertIn(record.partition, [0, 2])
        finally:
            await consumer.stop()

    # ==================== 10. Consumer Application Patterns ====================

    async def test_consumer_application_consume_loop(self):
        """Simulate a real consumer app with consume loop using getone()."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                # Simulate producer adding messages
                for i in range(3):
                    await broker.add_record(
                        ConsumerRecord(
                            topic="app-topic",
                            partition=0,
                            offset=i,
                            timestamp=0,
                            timestamp_type=0,
                            value=f"app-msg-{i}".encode(),
                            key=None,
                            checksum=None,
                            serialized_key_size=0,
                            serialized_value_size=len(f"app-msg-{i}".encode()),
                            headers=[],
                        )
                    )
                # Simulate consumer processing loop
                received = []
                for _ in range(3):
                    record = await consumer.getone()
                    received.append(record.value)
                self.assertEqual(len(received), 3)
                self.assertEqual(received[0], b"app-msg-0")
                self.assertEqual(received[1], b"app-msg-1")
                self.assertEqual(received[2], b"app-msg-2")
        finally:
            await consumer.stop()

    async def test_consumer_application_process_messages(self):
        """Simulate message processing: add records, consume and verify count."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                # Add 10 messages
                for i in range(10):
                    await broker.add_record(
                        ConsumerRecord(
                            topic="process-topic",
                            partition=0,
                            offset=i,
                            timestamp=0,
                            timestamp_type=0,
                            value=f"process-{i}".encode(),
                            key=None,
                            checksum=None,
                            serialized_key_size=0,
                            serialized_value_size=len(f"process-{i}".encode()),
                            headers=[],
                        )
                    )
                # Process all messages
                processed_count = 0
                for _ in range(10):
                    record = await consumer.getone()
                    self.assertIsNotNone(record)
                    processed_count += 1
                self.assertEqual(processed_count, 10)
        finally:
            await consumer.stop()

    async def test_consumer_application_with_key_routing(self):
        """Simulate key-based routing: add records with keys, consume and route by key."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                # Add messages with different keys
                keys = [b"user-1", b"user-2", b"user-1", b"user-3"]
                for i, key in enumerate(keys):
                    await broker.add_record(
                        ConsumerRecord(
                            topic="routing-topic",
                            partition=0,
                            offset=i,
                            timestamp=0,
                            timestamp_type=0,
                            value=b"event",
                            key=key,
                            checksum=None,
                            serialized_key_size=len(key),
                            serialized_value_size=5,
                            headers=[],
                        )
                    )
                # Route messages by key
                user_events = {b"user-1": [], b"user-2": [], b"user-3": []}
                for _ in range(4):
                    record = await consumer.getone()
                    if record.key in user_events:
                        user_events[record.key].append(record.value)
                self.assertEqual(len(user_events[b"user-1"]), 2)
                self.assertEqual(len(user_events[b"user-2"]), 1)
                self.assertEqual(len(user_events[b"user-3"]), 1)
        finally:
            await consumer.stop()

    async def test_consumer_application_multiple_topics(self):
        """Simulate subscribing to multiple topics, add records to all, consume from all."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                # Add records to multiple topics
                await broker.add_record(
                    ConsumerRecord(
                        topic="orders",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"order-created",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=13,
                        headers=[],
                    )
                )
                await broker.add_record(
                    ConsumerRecord(
                        topic="payments",
                        partition=0,
                        offset=1,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"payment-processed",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=17,
                        headers=[],
                    )
                )
                await broker.add_record(
                    ConsumerRecord(
                        topic="orders",
                        partition=0,
                        offset=2,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"order-shipped",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=13,
                        headers=[],
                    )
                )
                # Consume all records
                received_topics = []
                for _ in range(3):
                    record = await consumer.getone()
                    received_topics.append(record.topic)
                self.assertIn("orders", received_topics)
                self.assertIn("payments", received_topics)
                self.assertEqual(len(received_topics), 3)
        finally:
            await consumer.stop()

    async def test_consumer_application_with_headers_processing(self):
        """Simulate header-based processing: add records with headers, consume and inspect headers."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                # Add messages with headers
                headers_list = [
                    [("content-type", b"json"), ("priority", b"high")],
                    [("content-type", b"xml"), ("priority", b"low")],
                    [("content-type", b"json"), ("priority", b"medium")],
                ]
                for i, headers in enumerate(headers_list):
                    await broker.add_record(
                        ConsumerRecord(
                            topic="header-topic",
                            partition=0,
                            offset=i,
                            timestamp=0,
                            timestamp_type=0,
                            value=b"payload",
                            key=None,
                            checksum=None,
                            serialized_key_size=0,
                            serialized_value_size=7,
                            headers=headers,
                        )
                    )
                # Consume and inspect headers
                priorities = []
                for _ in range(3):
                    record = await consumer.getone()
                    for name, value in record.headers:
                        if name == "priority":
                            priorities.append(value)
                self.assertEqual(priorities, [b"high", b"low", b"medium"])
        finally:
            await consumer.stop()

    # ==================== 11. Consumer Created Before Context ====================

    async def test_consumer_created_before_context_receives_records(self):
        """Consumer created before context - receives records added via broker."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"pre-created-consumer",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=20,
                        headers=[],
                    )
                )
                record = await consumer.getone()
                self.assertEqual(record.value, b"pre-created-consumer")
        finally:
            await consumer.stop()

    async def test_consumer_created_before_context_multiple_records(self):
        """Consumer created before context - receives multiple records."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                for i in range(3):
                    await broker.add_record(
                        ConsumerRecord(
                            topic="multi-topic",
                            partition=0,
                            offset=i,
                            timestamp=0,
                            timestamp_type=0,
                            value=f"multi-{i}".encode(),
                            key=None,
                            checksum=None,
                            serialized_key_size=0,
                            serialized_value_size=len(f"multi-{i}".encode()),
                            headers=[],
                        )
                    )
                for i in range(3):
                    record = await consumer.getone()
                    self.assertEqual(record.value, f"multi-{i}".encode())
        finally:
            await consumer.stop()

    async def test_consumer_created_before_context_with_partition_filter(self):
        """Consumer created before context - consume with TopicPartition filtering."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="filter-topic",
                        partition=5,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"filtered-data",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=13,
                        headers=[],
                    )
                )
                tp = TopicPartition(topic="filter-topic", partition=5)
                record = await consumer.getone(tp)
                self.assertEqual(record.value, b"filtered-data")
                self.assertEqual(record.partition, 5)
        finally:
            await consumer.stop()

    async def test_consumer_created_before_context_app_pattern(self):
        """Consumer created before context - full application consume loop pattern."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                # Simulate adding messages
                for i in range(5):
                    await broker.add_record(
                        ConsumerRecord(
                            topic="app-topic",
                            partition=0,
                            offset=i,
                            timestamp=0,
                            timestamp_type=0,
                            value=f"app-{i}".encode(),
                            key=None,
                            checksum=None,
                            serialized_key_size=0,
                            serialized_value_size=len(f"app-{i}".encode()),
                            headers=[],
                        )
                    )
                # Consume in a loop
                received = []
                for _ in range(5):
                    record = await consumer.getone()
                    received.append(record.value)
                self.assertEqual(len(received), 5)
                for i, val in enumerate(received):
                    self.assertEqual(val, f"app-{i}".encode())
        finally:
            await consumer.stop()

    # ==================== 12. Edge Cases ====================

    async def test_consumer_receives_record_with_none_value(self):
        """Consumer receives record with value=None."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=None,
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=0,
                        headers=[],
                    )
                )
                record = await consumer.getone()
                self.assertIsNone(record.value)
        finally:
            await consumer.stop()

    async def test_consumer_receives_record_with_none_key(self):
        """Consumer receives record with key=None."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"data",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=4,
                        headers=[],
                    )
                )
                record = await consumer.getone()
                self.assertIsNone(record.key)
        finally:
            await consumer.stop()

    async def test_consumer_receives_record_with_empty_headers(self):
        """Consumer receives record with empty headers."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"data",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=4,
                        headers=[],
                    )
                )
                record = await consumer.getone()
                self.assertEqual(record.headers, [])
        finally:
            await consumer.stop()

    async def test_consumer_records_reset_after_context_exit(self):
        """Consumer records queue is cleared after context exit."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"data",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=4,
                        headers=[],
                    )
                )
        finally:
            await consumer.stop()
        self.assertEqual(broker.consumer_records.qsize(), 0)

    async def test_consumer_context_reenter_fresh_state(self):
        """Re-entering context gives fresh consumer_records queue."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"first",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=5,
                        headers=[],
                    )
                )
                record = await consumer.getone()
                self.assertEqual(record.value, b"first")
        finally:
            await consumer.stop()

        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                self.assertEqual(broker.consumer_records.qsize(), 0)
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"second",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=6,
                        headers=[],
                    )
                )
                record = await consumer.getone()
                self.assertEqual(record.value, b"second")
        finally:
            await consumer.stop()

    # ==================== 13. Producer + Consumer Integration ====================

    async def test_producer_consumer_integration_send_and_consume(self):
        """Send message via producer, then consume via consumer in same context."""
        producer = AIOKafkaProducer()
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await producer.send("integration-topic", value=b"integration-data")
                await broker.add_record(
                    ConsumerRecord(
                        topic="integration-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"integration-data",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=16,
                        headers=[],
                    )
                )
                record = await consumer.getone()
                self.assertEqual(record.value, b"integration-data")
                self.assertEqual(record.topic, "integration-topic")
        finally:
            await producer.stop()
            await consumer.stop()

    async def test_producer_consumer_integration_multiple_messages(self):
        """Send multiple messages via producer, consume all via consumer."""
        producer = AIOKafkaProducer()
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                for i in range(5):
                    await producer.send("multi-int-topic", value=f"msg-{i}".encode())
                for i in range(5):
                    await broker.add_record(
                        ConsumerRecord(
                            topic="multi-int-topic",
                            partition=0,
                            offset=i,
                            timestamp=0,
                            timestamp_type=0,
                            value=f"msg-{i}".encode(),
                            key=None,
                            checksum=None,
                            serialized_key_size=0,
                            serialized_value_size=len(f"msg-{i}".encode()),
                            headers=[],
                        )
                    )
                for i in range(5):
                    record = await consumer.getone()
                    self.assertEqual(record.value, f"msg-{i}".encode())
        finally:
            await producer.stop()
            await consumer.stop()

    async def test_producer_consumer_integration_different_topics(self):
        """Send to multiple topics via producer, consume from specific topic."""
        producer = AIOKafkaProducer()
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await producer.send("topic-a", value=b"data-a")
                await producer.send("topic-b", value=b"data-b")
                await broker.add_record(
                    ConsumerRecord(
                        topic="topic-a",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"data-a",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=6,
                        headers=[],
                    )
                )
                await broker.add_record(
                    ConsumerRecord(
                        topic="topic-b",
                        partition=0,
                        offset=1,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"data-b",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=6,
                        headers=[],
                    )
                )
                tp_a = TopicPartition(topic="topic-a", partition=0)
                record = await consumer.getone(tp_a)
                self.assertEqual(record.topic, "topic-a")
                self.assertEqual(record.value, b"data-a")
        finally:
            await producer.stop()
            await consumer.stop()

    async def test_producer_consumer_integration_with_key_and_headers(self):
        """Full integration: send with key + headers, consume and verify all fields."""
        producer = AIOKafkaProducer()
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                headers = [("content-type", b"json"), ("trace-id", b"trace-456")]
                await producer.send(
                    "full-int-topic",
                    value=b"full-integration",
                    key=b"integration-key",
                    headers=headers,
                )
                await broker.add_record(
                    ConsumerRecord(
                        topic="full-int-topic",
                        partition=0,
                        offset=0,
                        timestamp=1700000000000,
                        timestamp_type=0,
                        value=b"full-integration",
                        key=b"integration-key",
                        checksum=None,
                        serialized_key_size=13,
                        serialized_value_size=16,
                        headers=headers,
                    )
                )
                record = await consumer.getone()
                self.assertEqual(record.value, b"full-integration")
                self.assertEqual(record.key, b"integration-key")
                self.assertEqual(record.headers, headers)
                self.assertEqual(record.timestamp, 1700000000000)
        finally:
            await producer.stop()
            await consumer.stop()


    # ==================== 14. async for / async with Consumer ====================

    async def _consume_all(self, consumer, timeout=1.0):
        """Consume all records from consumer with timeout to prevent hanging."""
        records = []
        async def _iterate():
            async for record in consumer:
                records.append(record)
        try:
            await asyncio.wait_for(_iterate(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        return records

    async def test_async_for_single_record(self):
        """Iterate over a single record using async for."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"hello",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=5,
                        headers=[],
                    )
                )
                records = await self._consume_all(consumer)
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0].value, b"hello")
        finally:
            await consumer.stop()

    async def test_async_for_multiple_records(self):
        """Iterate over multiple records using async for."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                for i in range(5):
                    await broker.add_record(
                        ConsumerRecord(
                            topic="multi-topic",
                            partition=0,
                            offset=i,
                            timestamp=0,
                            timestamp_type=0,
                            value=f"msg-{i}".encode(),
                            key=None,
                            checksum=None,
                            serialized_key_size=0,
                            serialized_value_size=len(f"msg-{i}".encode()),
                            headers=[],
                        )
                    )
                records = await self._consume_all(consumer)
                self.assertEqual(len(records), 5)
                for i, record in enumerate(records):
                    self.assertEqual(record.value, f"msg-{i}".encode())
        finally:
            await consumer.stop()

    async def test_async_for_all_record_fields(self):
        """Verify all record fields during async for iteration."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                headers = [("content-type", b"json"), ("trace-id", b"abc123")]
                await broker.add_record(
                    ConsumerRecord(
                        topic="full-topic",
                        partition=2,
                        offset=42,
                        timestamp=1700000000000,
                        timestamp_type=0,
                        value=b"full-data",
                        key=b"full-key",
                        checksum=None,
                        serialized_key_size=8,
                        serialized_value_size=9,
                        headers=headers,
                    )
                )
                records = await self._consume_all(consumer)
                record = records[0]
                self.assertEqual(record.topic, "full-topic")
                self.assertEqual(record.value, b"full-data")
                self.assertEqual(record.key, b"full-key")
                self.assertEqual(record.partition, 2)
                self.assertEqual(record.timestamp, 1700000000000)
                self.assertEqual(record.offset, 42)
                self.assertEqual(record.headers, headers)
        finally:
            await consumer.stop()

    async def test_async_with_consumer(self):
        """Basic async with usage for Consumer."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="test-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"data",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=4,
                        headers=[],
                    )
                )
                async with consumer:
                    record = await consumer.getone()
                    self.assertEqual(record.value, b"data")
        finally:
            await consumer.stop()

    async def test_async_with_async_for_combined(self):
        """Combined async with + async for pattern."""
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                for i in range(3):
                    await broker.add_record(
                        ConsumerRecord(
                            topic="app-topic",
                            partition=0,
                            offset=i,
                            timestamp=0,
                            timestamp_type=0,
                            value=f"msg-{i}".encode(),
                            key=b"user-1",
                            checksum=None,
                            serialized_key_size=6,
                            serialized_value_size=len(f"msg-{i}".encode()),
                            headers=[],
                        )
                    )
                async with consumer:
                    records = await self._consume_all(consumer)
                    self.assertEqual(len(records), 3)
                    for record in records:
                        self.assertEqual(record.key, b"user-1")
        finally:
            await consumer.stop()

    async def test_async_for_producer_consumer_integration(self):
        """Send via producer, consume via async for."""
        producer = AIOKafkaProducer()
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await producer.send("integration-topic", value=b"integration-data")
                await broker.add_record(
                    ConsumerRecord(
                        topic="integration-topic",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"integration-data",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=16,
                        headers=[],
                    )
                )
                records = await self._consume_all(consumer)
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0].value, b"integration-data")
                self.assertEqual(records[0].topic, "integration-topic")
        finally:
            await producer.stop()
            await consumer.stop()


if __name__ == "__main__":
    unittest.main()
