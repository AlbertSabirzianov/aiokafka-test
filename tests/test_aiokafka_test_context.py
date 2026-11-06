import unittest
from aiokafka import AIOKafkaProducer

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


if __name__ == "__main__":
    unittest.main()
