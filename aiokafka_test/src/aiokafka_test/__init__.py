import dataclasses
import asyncio
import contextlib
from collections import defaultdict

from aiokafka import AIOKafkaProducer


@dataclasses.dataclass
class KafkaMessage:
    topic: str
    value: bytes | None = None
    key: bytes | None = None
    partition: int | None = None
    timestamp_ms: int | None = None
    headers: list[tuple[str, bytes]] | None = None


class MessageBroker:

    def __init__(self):
        self.messages = defaultdict(list)

    def topics(self) -> list[str]:
        return list(self.messages.keys())

    def add(self, kafka_message: KafkaMessage):
        self.messages[kafka_message.topic].append(kafka_message)

    def reset(self):
        self.messages = defaultdict(list)

message_broker = MessageBroker()


async def mock_send(self, topic, value=None, key=None, partition=None, timestamp=None, headers=None):
    message_broker.add(KafkaMessage(topic, value, key, partition, timestamp, headers))
    future = asyncio.Future()
    future.set_result(None)
    return future


async def mock_start():
    pass


@contextlib.contextmanager
def aiokafka_test_context():
    try:
        _original_send = AIOKafkaProducer.send
        _original_start = AIOKafkaProducer.start
        AIOKafkaProducer.send = mock_send
        AIOKafkaProducer.start = mock_start
        yield message_broker
    finally:
        message_broker.reset()
        AIOKafkaProducer.send = _original_send
        AIOKafkaProducer.start = _original_start

