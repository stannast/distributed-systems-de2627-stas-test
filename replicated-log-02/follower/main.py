import asyncio
import logging
import os
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from common.models import Message
from follower.node import FollowerNode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Replicated Log - Follower Service")
# Prevents outside clients from injecting messages directly (only the Master node can call POST /replicate)
FOLLOWER_MASTER_TOKEN = os.environ.get("MASTER_FOLLOWER_TOKEN")
if FOLLOWER_MASTER_TOKEN is None:
    raise RuntimeError("Follower HTTP token is not configured")

REPLICATION_DELAY = float(os.environ.get("REPLICATION_DELAY") or "0")

node = FollowerNode()

# from master
class MessageIn(BaseModel):
    id: int
    content: str

# to client
class MessageListOut(BaseModel):
    content: str

@app.post("/replicate", include_in_schema=False)
async def replicate(message: MessageIn, x_master_follower_token: str = Header(default=None)):
    if x_master_follower_token != FOLLOWER_MASTER_TOKEN:
        logger.warning("Rejected /replicate call with invalid internal token")
        raise HTTPException(status_code=403, detail="Forbidden")

    logger.info("Received message for replication: id=%s, content=%s", message.id, message.content)

    if REPLICATION_DELAY > 0:
        await asyncio.sleep(REPLICATION_DELAY)

    await node.receive_replicated(Message(id=message.id, content=message.content))

    return {"status": "ack", "message_id": message.id}

@app.get("/messages", response_model=list[MessageListOut])
def get_messages():
    messages = node.list_messages()
    logger.info("Returning messages: %s", messages)
    return [MessageListOut(content=m.content) for m in messages]
