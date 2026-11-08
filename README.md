# aiokafka-test

A testing utility library for [aiokafka](https://aiokafka.readthedocs.io/)-based applications. It provides a context manager that mocks Kafka producers, allowing you to test your services without a running Kafka cluster.

## Installation

```bash
pip install aiokafka-test
```

## Quick Start

```python
import unittest
from aiokafka import AIOKafkaProducer
from aiokafka_test import aiokafka_test_context


class MyServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_send_message(self):
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            await producer.send("my-topic", value=b"hello")
            await producer.stop()

            messages = broker.messages["my-topic"]
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0].value, b"hello")
```

## What is `aiokafka_test_context`?

`aiokafka_test_context` is a **synchronous** context manager that intercepts all Kafka message sends from `AIOKafkaProducer` and stores them in an in-memory `MessageBroker`. This allows you to:

- Test your Kafka-producing services **without a running Kafka broker**
- Verify that messages are sent to the **correct topics**
- Inspect **message content** (value, key, headers, partition, timestamp)
- Check **message counts** and distribution across topics

### How it works

When you enter the context:

1. `AIOKafkaProducer.send` is replaced with `mock_send`, which stores messages in the broker instead of sending them to Kafka
2. `AIOKafkaProducer.start` is replaced with a no-op `mock_start`
3. The `MessageBroker` instance is yielded for inspection

When you exit the context:

1. The broker is **automatically reset** (cleared)
2. The original `send` and `start` methods are **restored**

## API Reference

### `aiokafka_test_context()`

```python
from aiokafka_test import aiokafka_test_context

with aiokafka_test_context() as broker:
    # broker is a MessageBroker instance
    ...
```

- **Type**: Synchronous context manager (`@contextlib.contextmanager`)
- **Yields**: `MessageBroker` — an in-memory store for intercepted messages
- **Use**: `with` (not `async with`)
- **Cleanup**: Automatic — broker is reset and original methods restored on exit

### `MessageBroker`

```python
from aiokafka_test import MessageBroker, message_broker
```

| Attribute/Method | Type | Description |
|---|---|---|
| `messages` | `defaultdict(list)` | Mapping of topic name → list of `KafkaMessage` |
| `topics()` | `list[str]` | Returns a list of all topic names that have received messages |
| `reset()` | `None` | Clears all stored messages |

### `KafkaMessage`

```python
from aiokafka_test import KafkaMessage
```

| Attribute | Type | Description |
|---|---|---|
| `topic` | `str` | The Kafka topic name |
| `value` | `bytes \| None` | The message payload |
| `key` | `bytes \| None` | The message key (for partitioning) |
| `partition` | `int \| None` | The target partition |
| `timestamp_ms` | `int \| None` | The message timestamp (milliseconds) |
| `headers` | `list[tuple[str, bytes]] \| None` | Message headers as key-value pairs |

## Usage Examples

### Example 1: Basic Message Sending

Verify that a message is sent to the correct topic with the expected value.

```python
import unittest
from aiokafka import AIOKafkaProducer
from aiokafka_test import aiokafka_test_context


class BasicTest(unittest.IsolatedAsyncioTestCase):
    async def test_send_message(self):
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            await producer.send("orders", value=b'{"order_id": 123}')
            await producer.stop()

            messages = broker.messages["orders"]
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0].value, b'{"order_id": 123}')
```

### Example 2: Sending with Key, Partition, and Headers

Verify all message fields are preserved.

```python
class FullMessageTest(unittest.IsolatedAsyncioTestCase):
    async def test_send_with_all_fields(self):
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            headers = [("content-type", b"json"), ("trace-id", b"abc-123")]

            await producer.send(
                "orders",
                value=b'{"order_id": 456}',
                key=b"user-789",
                partition=2,
                timestamp=1700000000000,
                headers=headers,
            )
            await producer.stop()

            msg = broker.messages["orders"][0]
            self.assertEqual(msg.topic, "orders")
            self.assertEqual(msg.value, b'{"order_id": 456}')
            self.assertEqual(msg.key, b"user-789")
            self.assertEqual(msg.partition, 2)
            self.assertEqual(msg.timestamp_ms, 1700000000000)
            self.assertEqual(msg.headers, headers)
```

### Example 3: Multiple Messages Across Topics

Verify that messages are correctly distributed across different topics.

```python
class MultiTopicTest(unittest.IsolatedAsyncioTestCase):
    async def test_messages_distributed_correctly(self):
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()

            await producer.send("orders", value=b"order-1")
            await producer.send("orders", value=b"order-2")
            await producer.send("payments", value=b"payment-1")
            await producer.send("notifications", value=b"notify-1")
            await producer.stop()

            self.assertEqual(len(broker.messages["orders"]), 2)
            self.assertEqual(len(broker.messages["payments"]), 1)
            self.assertEqual(len(broker.messages["notifications"]), 1)
            self.assertEqual(
                broker.topics(),
                ["orders", "payments", "notifications"],
            )
```

### Example 4: Testing a Service That Uses AIOKafkaProducer

Test a real-world service class that wraps Kafka producer operations.

```python
class OrderService:
    """A service that processes orders and publishes them to Kafka."""

    def __init__(self):
        self.producer = AIOKafkaProducer()

    async def process_order(self, order_id: str, data: dict) -> None:
        message = {"order_id": order_id, **data}
        await self.producer.send(
            "orders",
            value=str(message).encode(),
            key=order_id.encode(),
        )

    async def close(self):
        await self.producer.stop()


class OrderServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_process_order(self):
        service = OrderService()

        with aiokafka_test_context() as broker:
            await service.process_order("ord-001", {"item": "book", "qty": 2})
            await service.close()

            messages = broker.messages["orders"]
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0].key, b"ord-001")
            self.assertIn(b'"item"', messages[0].value)
            self.assertIn(b'"book"', messages[0].value)
```

### Example 5: Producer Created Before Context

When the producer is initialized outside the context, it still works correctly.

```python
class ProducerBeforeContextTest(unittest.IsolatedAsyncioTestCase):
    async def test_producer_created_before_context(self):
        producer = AIOKafkaProducer()
        try:
            with aiokafka_test_context() as broker:
                await producer.send("events", value=b"user-login")
                await producer.send("events", value=b"user-logout")
                await producer.send("events", value=b"user-purchase")
                await producer.stop()

                messages = broker.messages["events"]
                self.assertEqual(len(messages), 3)
        finally:
            await producer.stop()
```

> **Note:** Always call `await producer.stop()` in a `finally` block to avoid `ResourceWarning: Unclosed AIOKafkaProducer`.

### Example 6: Message Accumulation

Verify that multiple messages accumulate in the same topic.

```python
class AccumulationTest(unittest.IsolatedAsyncioTestCase):
    async def test_messages_accumulate(self):
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()

            for i in range(10):
                await producer.send(
                    "counter",
                    value=f"message-{i}".encode(),
                )
            await producer.stop()

            messages = broker.messages["counter"]
            self.assertEqual(len(messages), 10)

            for i, msg in enumerate(messages):
                self.assertEqual(msg.value, f"message-{i}".encode())
```

### Example 7: Broker Reset Between Tests

The broker is automatically reset when exiting the context, making each test independent.

```python
class ResetTest(unittest.IsolatedAsyncioTestCase):
    async def test_broker_is_fresh_after_exit(self):
        # First context
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            await producer.send("topic", value=b"first")
            await producer.stop()
            self.assertEqual(len(broker.messages["topic"]), 1)

        # After exit, broker is empty
        from aiokafka_test import message_broker
        self.assertEqual(message_broker.topics(), [])

        # Second context starts fresh
        with aiokafka_test_context() as broker:
            producer = AIOKafkaProducer()
            await producer.send("topic", value=b"second")
            await producer.stop()

            messages = broker.messages["topic"]
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0].value, b"second")
```

## Consumer Testing

The library also provides a mock for `AIOKafkaConsumer`, allowing you to test consumer logic without a running Kafka broker.

### Adding Records for the Consumer

Use `broker.add_record()` to inject `ConsumerRecord` objects into the broker:

```python
import unittest
from aiokafka import AIOKafkaConsumer, ConsumerRecord
from aiokafka_test import aiokafka_test_context


class ConsumerTest(unittest.IsolatedAsyncioTestCase):
    async def test_receive_record(self):
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="my-topic",
                        partition=0,
                        offset=0,
                        timestamp=1700000000000,
                        timestamp_type=0,
                        value=b"hello",
                        key=b"key-1",
                        checksum=None,
                        serialized_key_size=5,
                        serialized_value_size=5,
                        headers=[("content-type", b"json")],
                    )
                )
                record = await consumer.getone()
                self.assertEqual(record.value, b"hello")
                self.assertEqual(record.key, b"key-1")
                self.assertEqual(record.topic, "my-topic")
        finally:
            await consumer.stop()
```

### Async For Iteration

Use `async for record in consumer` to iterate over records. A helper method with `asyncio.wait_for` prevents hanging when no records are available:

```python
class AsyncForTest(unittest.IsolatedAsyncioTestCase):
    async def _consume_all(self, consumer, timeout=1.0):
        records = []
        async def _iterate():
            async for record in consumer:
                records.append(record)
        try:
            await asyncio.wait_for(_iterate(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        return records

    async def test_async_for(self):
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                for i in range(3):
                    await broker.add_record(
                        ConsumerRecord(
                            topic="events",
                            partition=0,
                            offset=i,
                            timestamp=0,
                            timestamp_type=0,
                            value=f"event-{i}".encode(),
                            key=None,
                            checksum=None,
                            serialized_key_size=0,
                            serialized_value_size=len(f"event-{i}".encode()),
                            headers=[],
                        )
                    )
                records = await self._consume_all(consumer)
                self.assertEqual(len(records), 3)
                self.assertEqual(records[0].value, b"event-0")
                self.assertEqual(records[1].value, b"event-1")
                self.assertEqual(records[2].value, b"event-2")
        finally:
            await consumer.stop()
```

### Async With Consumer

Use `async with consumer` for context manager usage:

```python
class AsyncWithTest(unittest.IsolatedAsyncioTestCase):
    async def test_async_with(self):
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await broker.add_record(
                    ConsumerRecord(
                        topic="my-topic",
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
```

### Combined: Producer + Consumer

Test the full produce-and-consume cycle:

```python
class IntegrationTest(unittest.IsolatedAsyncioTestCase):
    async def test_produce_and_consume(self):
        producer = AIOKafkaProducer()
        consumer = AIOKafkaConsumer()
        try:
            with aiokafka_test_context() as broker:
                await producer.send("orders", value=b"order-123")
                await broker.add_record(
                    ConsumerRecord(
                        topic="orders",
                        partition=0,
                        offset=0,
                        timestamp=0,
                        timestamp_type=0,
                        value=b"order-123",
                        key=None,
                        checksum=None,
                        serialized_key_size=0,
                        serialized_value_size=9,
                        headers=[],
                    )
                )
                record = await consumer.getone()
                self.assertEqual(record.value, b"order-123")
                self.assertEqual(record.topic, "orders")
        finally:
            await producer.stop()
            await consumer.stop()
```

## Best Practices

### 1. Always use `with`, not `async with`

`aiokafka_test_context` is a **synchronous** context manager:

```python
# Correct
with aiokafka_test_context() as broker:
    producer = AIOKafkaProducer()
    await producer.send("topic", value=b"data")
    await producer.stop()

# Incorrect — will raise TypeError
# async with aiokafka_test_context() as broker:
```

### 2. Always stop the producer

Call `await producer.stop()` to avoid resource leaks and `ResourceWarning`:

```python
# Recommended: try/finally for guaranteed cleanup
producer = AIOKafkaProducer()
try:
    with aiokafka_test_context() as broker:
        await producer.send("topic", value=b"data")
        await producer.stop()
finally:
    await producer.stop()
```



### 3. Use `unittest.IsolatedAsyncioTestCase`

The recommended test class is `unittest.IsolatedAsyncioTestCase`, which provides isolated event loops for each async test method:

```python
import unittest
from aiokafka_test import aiokafka_test_context


class MyTest(unittest.IsolatedAsyncioTestCase):
    async def test_one(self):
        with aiokafka_test_context() as broker:
            ...

    async def test_two(self):
        with aiokafka_test_context() as broker:
            ...
```

## How It Works

The library uses monkey-patching to intercept Kafka producer operations:

```
Your code                          aiokafka_test
─────────────                      ─────────────
producer.send("topic", ...)  →  mock_send intercepts the call
                                   ↓
                              stores KafkaMessage in broker
                                   ↓
                              returns a completed Future
```

1. On entering the context, `AIOKafkaProducer.send` is replaced with `mock_send`
2. `mock_send` creates a `KafkaMessage` object and stores it in the `MessageBroker`
3. On exit, the broker is reset and the original `send`/`start` methods are restored

## Requirements

- Python >= 3.11
- aiokafka

## License

MIT
