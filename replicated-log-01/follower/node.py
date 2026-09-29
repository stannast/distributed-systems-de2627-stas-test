import logging
from common.models import Message

logger = logging.getLogger(__name__)

class FollowerNode:
    """
    Domain logic for the Follower. Just stores replicated messages.
    """
    def __init__(self):
        self.messages = []

    def receive_replicated(self, message: Message):
        self.messages.append(message)
        logger.info("Message replicated: id=%s, content=%s", message.id, message.content)

    def list_messages(self):
        return list(self.messages)
