import asyncio
import logging
from common.models import Message

logger = logging.getLogger(__name__)

class MasterNode:
    """
    Domain logic for the Master. Appending messages and replicating them to
    followers via an injected transport.
    """
    def __init__(self, transport, followers_urls):
        self.transport = transport
        self.followers_urls = followers_urls
        self.messages = []
        self.next_id = 1

    async def append_message(self, content):
        message = Message(id=self.next_id, content=content)
        self.messages.append(message)
        self.next_id += 1

        logger.info("Message appended: id=%s, content=%s", message.id, message.content)

        if self.followers_urls:
            results = await asyncio.gather(
                *(self.transport.replicate(f_id, message) for f_id in self.followers_urls),
                return_exceptions=True
            )

            for follower_url, result in zip(self.followers_urls, results):
                if isinstance(result, Exception):
                    logger.error("Replication to %s failed: %s", follower_url, result)
                    raise result

        logger.info("Replication completed for message id=%s", message.id)

        return message

    def list_messages(self):
        return list(self.messages)
