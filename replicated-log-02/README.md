### Replicated Log

Master - accepts POST /messages from the client, stores the message in an in-memory list, replicates it to each Follower and returns a response to the client after receiving the number of ACKs requested by the client in the write concern parameter `w` (master itself counts as one ACK). The remaining Followers keep receiving the message in the background (eventual consistency).
Serves messages to clients via the public GET /messages.

Follower - accepts replicated messages, guarantees ordering of messages stores them in its own in-memory list and serves them to clients via the public GET /messages.

### Project structure

- `master/node.py`, `follower/node.py` - core: business logic (append, replicate, wait for `w` ACKs, total ordering) with no knowledge of HTTP or FastAPI.
- `master/main.py`, `follower/main.py` - web: FastAPI layer that turns HTTP requests into calls to the node.
- `master/transport.py` - defines how Master delivers a message to a Follower (HttpTransport for real HTTP), kept separate so it can be swapped for a mock in tests.
- `common/models.py` - shared Message model used by both Master and Follower.
- `tests/mocked_transport.py` - test-only transport that delivers messages in memory and lets tests set a delay per Follower and reorder messages.
- `tests/test_harness.py` - tests that run the Master against `MockedTransport`: blocking and parallel replication, write concern with temporary inconsistency (acceptance test), and messages reordering.

### Running

```bash
docker compose up
```
(or docker-compose up in V1)

This starts 3 containers:

master - port 8000
follower1 - port 8001 (no artificial delay)
follower2 - port 8002 (artificial delay 10 seconds, see `REPLICATION_DELAY` in docker-compose.yml)

To stop:
```bash
docker compose down
```
(or docker-compose down in V1)

### Master — `http://localhost:8000/docs`

Swagger UI: **http://localhost:8000/docs**

Methods:
POST /messages - Appends a message and replicates it to followers. Requires write concern `w`.
GET /messages - Returns all messages from the Master's list.

How to test in Swagger:
1. Open POST /messages, click `Try it out`, enter {"content": "hello", "w": 3} and click `Execute`.
2. The request returns only after `w` ACKs are received: with `w=3` it waits for follower2 and the response takes about 10 seconds, with `w=1` or `w=2` it returns immediately (follower1 has no delay).
3. Open GET /messages on the Master and check that the message is present.

Test example without Swagger:

curl -X POST http://localhost:8000/messages \
  -H "Content-Type: application/json" \
  -d '{"content": "hello", "w": 3}'

curl http://localhost:8000/messages

### Follower — `http://localhost:8001/docs` and `http://localhost:8002/docs`

Swagger UI: **http://localhost:8001/docs** and **http://localhost:8002/docs**

Methods:
GET /messages - Returns all replicated messages in the correct order.

How to test in Swagger:
1. Open GET /messages on the Follower, click `Try it out` and check that the message is present.
2. While follower2 is still processing the delayed message, its list temporarily differs from the Master's list. After the delay it catches up.

Test example without Swagger:

curl http://localhost:8001/messages
curl http://localhost:8002/messages
 
`POST /replicate` on Follower is an internal endpoint used for replication from Master, protected by the X-Master-Follower-Token header. 
It is hidden from Swagger (include_in_schema=False) and is not part of Follower's public API.

### Manual acceptance test

```bash
curl -X POST http://localhost:8000/messages -H "Content-Type: application/json" -d '{"content": "Msg1", "w": 1}'
curl -X POST http://localhost:8000/messages -H "Content-Type: application/json" -d '{"content": "Msg2", "w": 2}'
curl -X POST http://localhost:8000/messages -H "Content-Type: application/json" -d '{"content": "Msg3", "w": 3}' &
sleep 0.5
curl -X POST http://localhost:8000/messages -H "Content-Type: application/json" -d '{"content": "Msg4", "w": 1}'

curl http://localhost:8002/messages   # follower2: incomplete
curl http://localhost:8001/messages   # follower1: Msg1...Msg4
curl http://localhost:8000/messages   # master: Msg1...Msg4
```

### Running the test harness

The test harness (tests/test_harness.py) contains 3 tests. It runs the Master against an in-memory MockedTransport, so no other containers are needed.

- `test_replication_is_blocking_and_parallel` - with `w=3` the Master waits for all Followers, and replication to them runs in parallel (m2 takes about 6s, not 5s + 6s).
- `test_write_concern_inconsistency_and_eventual_consistency` - acceptance test: Msg1 (`w=1`), Msg2 (`w=2`), Msg3 (`w=3`) waits for the delayed follower, Msg4 (`w=1`) is not blocked by Msg3; the delayed follower temporarily differs from the Master and later catches up.
- `test_replication_reorders_out_of_order_delivery` - the transport delivers 2 messages in reverse order, but the Follower still lists them in the correct order.

```bash
docker compose run --rm tests
```
(in V1: docker-compose run --rm tests)

The tests take about 14 seconds because they use real delays (5s and 6s in the first test, 3s in the acceptance test).
