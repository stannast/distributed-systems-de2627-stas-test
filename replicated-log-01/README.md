### Replicated Log

Master - accepts POST /messages from the client, stores the message in an in-memory list, 
replicates it to each Fpllower and returns a response to the client after receiving ACKs from all Followers.

Follower - accepts replicated messages stores them in its own in-memory list 
and serves them to clients via the public GET /messages.

### Project structure

- `master/node.py`, `follower/node.py` - core: business logic (append, replicate, wait for ACKs) with no knowledge of HTTP or FastAPI.
- `master/main.py`, `follower/main.py` - web: FastAPI layer that turns HTTP requests into calls to the node.
- `master/transport.py` - defines how Master delivers a message to a Follower (HttpTransport for real HTTP), kept separate so it can be swapped for a mock in tests.
- `common/models.py` - shared Message model used by both Master and Follower.
- `tests/mocked_transport.py` - test-only transport that delivers messages in memory and lets tests set a delay per Follower.
- `tests/test_harness.py` - checks that replication is blocking and parallel by running the Master against `MockedTransport`.

### Running

```bash
docker compose up
```
(or docker-compose up in V1)

This starts 3 containers:

master - port 8000
follower1 - port 8001
follower2 - port 8002

To stop:
```bash
docker compose down
```
(or docker-compose down in V1)

### Master — `http://localhost:8000/docs`

Swagger UI: **http://localhost:8000/docs**

Methods:
POST /messages - Appends a message and replicates it to all followers.
GET /messages - Returns all messages from the Master's list.

How to test in Swagger:
1. Open POST /messages, click `Try it out`, enter {"content": "hello"} and click `Execute`.
2. The request is blocking: it returns only after all followers acknowledge the message, so with the artificial delay on the followers (5 seconds in docker-compose.yml) the response takes about 5 seconds.
3. Open GET /messages on the Master and check that the message is present.

Test example without Swagger:

curl -X POST http://localhost:8000/messages \
  -H "Content-Type: application/json" \
  -d '{"content": "hello"}'

curl http://localhost:8000/messages

### Follower — `http://localhost:8001/docs` and `http://localhost:8002/docs`

Swagger UI: **http://localhost:8001/docs** and **http://localhost:8002/docs**

Methods:
GET /messages - Returns all replicated messages.

How to test in Swagger:
1. Open GET /messages on the Follower, click `Try it out` and check that the message is present.

Test example without Swagger:

curl http://localhost:8001/messages
curl http://localhost:8002/messages
 
`POST /replicate` on Follower is an internal endpoint used for replication from Master, protected by the X-Master-Follower-Token header. 
It is hidden from Swagger (include_in_schema=False) and is not part of Folower's public API.

### Running the test harness

The test harness (tests/test_harness.py) checks that replication is blocking and parallel.
It runs the Master against an in-memory MockedTransport, so no other containers are needed.

```bash
docker compose run --rm tests
```
(in V1: docker-compose run --rm tests)

The test takes about 11 seconds because it uses real delays (5s and 6s). It prints how long each append_message took and the messages stored on the Master and both Followers.

