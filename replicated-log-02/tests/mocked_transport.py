import asyncio
from common.models import Message
from follower.node import FollowerNode

class MockedTransport:
    """
    Test double for HttpTransport.
    """
    def __init__(self):
        self.followers = {}
        self.delays = {}
        self.reorder_counts = {}
        self.reorder_buffers = {}
        self.delivery_log = {}  # follower_id - list of message id in actual arrival order

    def register_follower(self, follower_id, node: FollowerNode):
        self.followers[follower_id] = node

    def set_delay(self, follower_id, seconds):
        self.delays[follower_id] = seconds

    def remove_delay(self, follower_id):
        self.delays.pop(follower_id, None)

    def reorder_messages(self, follower_id, messages_count=2):
        self.reorder_counts[follower_id] = messages_count
        self.reorder_buffers[follower_id] = []

    async def deliver(self, follower_id, message: Message):
        self.delivery_log.setdefault(follower_id, []).append(message.id)
        await self.followers[follower_id].receive_replicated(message)

    async def replicate(self, follower_id, message: Message):
        delay = self.delays.get(follower_id)
        if delay:
            await asyncio.sleep(delay)

        if follower_id in self.reorder_counts:
            buffer = self.reorder_buffers[follower_id]
            released = asyncio.get_running_loop().create_future()
            buffer.append((message, released))

            if len(buffer) < self.reorder_counts[follower_id]:
                # No ACK until the whole batch has really been delivered
                await released
                return

            batch = list(reversed(buffer))
            buffer.clear()
            for buffered_message, _ in batch:
                await self.deliver(follower_id, buffered_message)
            for _, fut in batch:
                fut.set_result(None)
            return

        await self.deliver(follower_id, message)
