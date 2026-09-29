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

    def register_follower(self, follower_id, node: FollowerNode):
        self.followers[follower_id] = node

    def set_delay(self, follower_id, seconds):
        self.delays[follower_id] = seconds

    def remove_delay(self, follower_id):
        self.delays.pop(follower_id, None)

    async def replicate(self, follower_id, message: Message):
        delay = self.delays.get(follower_id)
        if delay:
            await asyncio.sleep(delay)

        self.followers[follower_id].receive_replicated(message)
