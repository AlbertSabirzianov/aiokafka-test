import dataclasses
import asyncio
import contextlib
from collections import defaultdict

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, TopicPartition, ConsumerRecord


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
        self.consumer_records = asyncio.Queue()

    def topics(self) -> list[str]:
        return list(self.messages.keys())

    def add(self, kafka_message: KafkaMessage):
        self.messages[kafka_message.topic].append(kafka_message)

    def reset(self):
        self.messages = defaultdict(list)
        self.consumer_records = asyncio.Queue()

    async def add_record(self, consumer_record: ConsumerRecord):
        await self.consumer_records.put(consumer_record)


message_broker = MessageBroker()


async def mock_send(self, topic, value=None, key=None, partition=None, timestamp=None, headers=None):
    message_broker.add(KafkaMessage(topic, value, key, partition, timestamp, headers))
    future = asyncio.Future()
    future.set_result(None)
    return future


async def mock_getone(self, *partitions):
    assert all(isinstance(k, TopicPartition) for k in partitions)
    _record = None
    while _record is None:
        _record: ConsumerRecord = await message_broker.consumer_records.get()
        if partitions and TopicPartition(topic=_record.topic, partition=_record.partition) not in partitions:
            _record = None
    return _record


async def mock_start():
    pass


@contextlib.contextmanager
def aiokafka_test_context():
    try:
        ## Produser
        _original_send = AIOKafkaProducer.send
        _original_start = AIOKafkaProducer.start
        AIOKafkaProducer.send = mock_send
        AIOKafkaProducer.start = mock_start

        ## Consumer
        _original_consumer_start = AIOKafkaConsumer.start
        _original_consumer_get_one = AIOKafkaConsumer.getone
        AIOKafkaConsumer.start = mock_start
        AIOKafkaConsumer.getone = mock_getone
        yield message_broker
    finally:
        message_broker.reset()

        ## Produser
        AIOKafkaProducer.send = _original_send
        AIOKafkaProducer.start = _original_start

        ## Consumer
        AIOKafkaConsumer.start = _original_consumer_start
        AIOKafkaConsumer.getone = _original_consumer_get_one

