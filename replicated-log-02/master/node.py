import asyncio
import logging
from common.models import Message

logger = logging.getLogger(__name__)

class MasterNode:
    """
    Domain logic for the Master role. Knows nothing about HTTP or FastAPI, 
    only about appending messages and replicating them to
    followers via an injected transport, with tunable write concern.
    """
    def __init__(self, transport, followers_urls):
        self.transport = transport
        self.followers_urls = followers_urls
        self.messages = []
        self.next_id = 1
        self.background_tasks = set()

    @property
    def total_nodes(self):
        return len(self.followers_urls) + 1  # master + all secondaries

    async def append_message(self, content, w):
        if not (1 <= w <= self.total_nodes):
            raise ValueError(f"write concern w={w} is out of allowed range 1...{self.total_nodes}")

        message = Message(id=self.next_id, content=content)
        self.messages.append(message)
        self.next_id += 1

        logger.info("Message appended: id=%s, content=%s, w=%s", message.id, message.content, w)

        state = {
            "acks": 1,  # master counts as the first ack
            "target": w,
            "lock": asyncio.Lock(),
            "event": asyncio.Event(),
        }
        if state["acks"] >= state["target"]:
            state["event"].set()

        for follower_url in self.followers_urls:
            task = asyncio.create_task(self._replicate_to_follower(follower_url, message, state))
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)

        await state["event"].wait()

        logger.info("Write concern satisfied for message id=%s (%s/%s acks)", message.id, state["acks"], state["target"])

        return message

    async def _replicate_to_follower(self, follower_url, message: Message, state):
        try:
            await self.transport.replicate(follower_url, message)
        except Exception:
            logger.exception("Replication to %s failed for message id=%s", follower_url, message.id)
            return

        async with state["lock"]:
            state["acks"] += 1
            logger.info("Ack count for message id=%s is now %s (target=%s)", message.id, state["acks"], state["target"])
            if state["acks"] >= state["target"] and not state["event"].is_set():
                state["event"].set()

    def list_messages(self):
        return list(self.messages)
