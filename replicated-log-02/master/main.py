import logging
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from master.transport import HttpTransport
from master.node import MasterNode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Replicated Log - Master Service")
# Prevents outside clients from injecting messages directly (only the Master node can call POST /replicate)
MASTER_FOLLOWER_TOKEN = os.environ.get("MASTER_FOLLOWER_TOKEN")
if MASTER_FOLLOWER_TOKEN is None:
    raise RuntimeError("Master HTTP token is not configured")

FOLLOWERS = [
    url.strip()
    for url in os.environ.get("FOLLOWERS", "http://127.0.0.1:8001,http://127.0.0.1:8002").split(",")
    if url.strip()
]

transport = HttpTransport(internal_token=MASTER_FOLLOWER_TOKEN)
node = MasterNode(transport=transport, followers_urls=FOLLOWERS)

# from client
class MessageRequest(BaseModel):
    content: str = Field(min_length=1)
    w: int = Field(ge=1, description="Write concern: how many acks (including master) to wait for")

# to client
class MessagePostOut(BaseModel):
    id: int
    content: str

# to client
class MessageListOut(BaseModel):
    content: str

@app.post("/messages", response_model=MessagePostOut)
async def append_message(message_request: MessageRequest):
    try:
        message = await node.append_message(message_request.content, message_request.w)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return MessagePostOut(id=message.id, content=message.content)

@app.get("/messages", response_model=list[MessageListOut])
def get_messages():
    messages = node.list_messages()
    logger.info("Returning messages: %s", messages)
    return [MessageListOut(content=m.content) for m in messages]
