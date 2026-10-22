import asyncio
import logging
from common.models import Message

logger = logging.getLogger(__name__)

class FollowerNode:
    """
    Domain logic for the Follower role. Stores replicated messages while guaranteeing total ordering.
    """
    def __init__(self):
        self.messages = []
        self.received_ids = set()
        self.pending_buffer = {}
        self.next_expected_id = 1
        self.lock = asyncio.Lock()

    async def receive_replicated(self, message: Message):
        async with self.lock:
            self.received_ids.add(message.id)
            self.pending_buffer[message.id] = message

            while self.next_expected_id in self.pending_buffer:
                ordered_message = self.pending_buffer.pop(self.next_expected_id)
                self.messages.append(ordered_message)
                logger.info("Message applied in order: id=%s, content=%s", ordered_message.id, ordered_message.content)
                self.next_expected_id += 1

            if message.id in self.pending_buffer:
                logger.info("Message id=%s buffered out of order, waiting for id=%s", message.id, self.next_expected_id)

    def list_messages(self):
        return list(self.messages)
