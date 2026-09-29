import logging
import httpx
from common.models import Message

logger = logging.getLogger(__name__)

class HttpTransport:
    """
    Transport: delivers messages to real Follower services over HTTP.
    """
    def __init__(self, internal_token, timeout = 60.0):
        self.internal_token = internal_token
        self.timeout = timeout

    async def replicate(self, follower_id, message: Message):
        url = follower_id

        logger.info("Sending message id=%s to %s", message.id, url)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{url}/replicate",
                json={"id": message.id, "content": message.content},
                headers={"X-Master-Follower-Token": self.internal_token},
            )

        if response.status_code >= 400:
            logger.error("Follower %s returned %s: %s", url, response.status_code, response.text)

        response.raise_for_status()
        ack = response.json()

        if ack.get("status") != "ack":
            raise RuntimeError(f"Invalid ACK from {follower_id}: {ack}")

        logger.info("Received ACK from %s for message id=%s", follower_id, message.id)
