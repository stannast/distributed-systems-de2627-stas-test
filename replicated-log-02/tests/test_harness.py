import asyncio
import time
import pytest
from master.node import MasterNode
from follower.node import FollowerNode
from tests.mocked_transport import MockedTransport

DELAY_1 = 5.0
DELAY_2 = 6.0
# Extra time allowed on top of the expected delay. asyncio.sleep never wakes up early,
# but can wake up slightly late due to event loop and OS scheduling overhead.
TOLERANCE = 0.1

F1 = "servicename-follower1"
F2 = "servicename-follower2"

def contents(node):
    return [m.content for m in node.list_messages()]

@pytest.mark.asyncio
async def test_replication_is_blocking_and_parallel():
    transport = MockedTransport()
    follower1, follower2 = FollowerNode(), FollowerNode()
    transport.register_follower(F1, follower1)
    transport.register_follower(F2, follower2)
    master = MasterNode(transport, followers_urls=[F1, F2])

    # m1: only follower1 has a delay
    transport.set_delay(F1, DELAY_1)
    start = time.perf_counter()
    await master.append_message("m1", w=3)
    finish = time.perf_counter() - start
    assert DELAY_1 <= finish < DELAY_1 + TOLERANCE
    print(f"m1 (w=3) took {finish:.2f}s (expected ~{DELAY_1}s)")
    print("master after m1:", contents(master))

    # m2: follower1 keeps its delay, follower2 gets a bigger one
    transport.set_delay(F2, DELAY_2)
    start = time.perf_counter()
    await master.append_message("m2", w=3)
    finish = time.perf_counter() - start
    # It should take max(DELAY_1, DELAY_2) = DELAY_2.
    assert DELAY_2 <= finish < DELAY_2 + TOLERANCE
    print(f"m2 (w=3) took {finish:.2f}s (expected ~{DELAY_2}s)")
    print("master after m2:", contents(master))

    # m3: no delays
    transport.remove_delay(F1)
    transport.remove_delay(F2)
    start = time.perf_counter()
    await master.append_message("m3", w=3)
    finish = time.perf_counter() - start
    assert finish < TOLERANCE
    print(f"m3 (w=3) took {finish:.2f}s (expected no blocking)")

    expected = ["m1", "m2", "m3"]
    assert contents(master) == expected
    assert contents(follower1) == expected
    assert contents(follower2) == expected
    print("master:    ", contents(master))
    print("follower1: ", contents(follower1))
    print("follower2: ", contents(follower2))

@pytest.mark.asyncio
async def test_write_concern_inconsistency_and_eventual_consistency():
    transport = MockedTransport()
    fl1, fl2 = FollowerNode(), FollowerNode()
    transport.register_follower(F1, fl1)
    transport.register_follower(F2, fl2)
    transport.set_delay(F2, DELAY_2)
    master = MasterNode(transport, followers_urls=[F1, F2])

    # Msg1 (w=1) - master only
    start = time.perf_counter()
    await master.append_message("Msg1", w=1)
    finish = time.perf_counter() - start
    assert finish < TOLERANCE
    print(f"Msg1 (w=1) took {finish:.2f}s (expected no blocking)")

    # Msg2 (w=2) - master + F1
    start = time.perf_counter()
    await master.append_message("Msg2", w=2)
    finish = time.perf_counter() - start
    assert finish < TOLERANCE
    print(f"Msg2 (w=2) took {finish:.2f}s (expected no blocking)")

    # Msg3 (w=3) - must wait for F2
    msg3_start = time.perf_counter()
    msg3_task = asyncio.create_task(master.append_message("Msg3", w=3))
    await asyncio.sleep(TOLERANCE)  # let Msg3 get id=3 and start replicating
    assert not msg3_task.done()
    print("Msg3 (w=3) is waiting for F2")

    # Msg4 (w=1) - must not be blocked by Msg3
    start = time.perf_counter()
    await master.append_message("Msg4", w=1)
    finish = time.perf_counter() - start
    assert finish < TOLERANCE
    assert not msg3_task.done()
    print(f"Msg4 (w=1) took {finish:.2f}s (not blocked by Msg3)")

    # Let the follower receive Msg4 in the background.
    await asyncio.sleep(TOLERANCE)
    # Temporary inconsistency
    print("while Msg3 is still waiting (temporary inconsistency)")
    print("master: ", contents(master))
    print("F1:     ", contents(fl1))
    print("F2:     ", contents(fl2))
    assert contents(master) == ["Msg1", "Msg2", "Msg3", "Msg4"]
    assert contents(fl1) == ["Msg1", "Msg2", "Msg3", "Msg4"]
    assert contents(fl2) != contents(master)
    assert "Msg4" not in contents(fl2)  # Msg4 never appears before Msg3

    # Msg3 finishes only after F2 delay
    await msg3_task
    msg3_finish = time.perf_counter() - msg3_start
    assert DELAY_2 <= msg3_finish < DELAY_2 + TOLERANCE
    print(f"Msg3 (w=3) finished after {msg3_finish:.2f}s (expected ~{DELAY_2}s)")

    # Eventual consistency
    await asyncio.gather(*master.background_tasks)
    print("after background replication (eventual consistency)")
    print("master: ", contents(master))
    print("F1:     ", contents(fl1))
    print("F2:     ", contents(fl2))
    assert contents(fl2) == ["Msg1", "Msg2", "Msg3", "Msg4"]

@pytest.mark.asyncio
async def test_replication_reorders_out_of_order_delivery():
    transport = MockedTransport()
    follower1 = FollowerNode()
    transport.register_follower(F1, follower1)
    master = MasterNode(transport, followers_urls=[F1])

    # Should wait for N (2 by default) messages and reorder them 
    transport.reorder_messages(F1, messages_count=2)
    start = time.perf_counter()
    await master.append_message("m1", w=1)
    await master.append_message("m2", w=1)
    finish = time.perf_counter() - start
    assert finish < TOLERANCE
    print(f"m1, m2 (w=1) took {finish:.2f}s (expected no blocking)")
    print("master:    ", contents(master))
    print("follower1: ", contents(follower1), "(nothing delivered yet)")
    assert contents(master) == ["m1", "m2"]
    assert contents(follower1) == []

    await asyncio.gather(*master.background_tasks)

    # Reordering really happened
    print("arrival order at follower1 (ids):", transport.delivery_log[F1])
    assert transport.delivery_log[F1] == [2, 1]
    # The secondary exposes the messages in the correct order
    print("follower1 listing:", contents(follower1))
    assert contents(follower1) == ["m1", "m2"]
