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

@pytest.mark.asyncio
async def test_replication_is_blocking_and_parallel():
    transport = MockedTransport()

    follower1 = FollowerNode()
    follower2 = FollowerNode()
    transport.register_follower("servicename-follower1", follower1)
    transport.register_follower("servicename-follower2", follower2)
    master = MasterNode(transport, followers_urls=["servicename-follower1", "servicename-follower2"])

    # m1: only follower1 has a delay
    transport.set_delay("servicename-follower1", DELAY_1)

    # Remember the time before the call, run the call, then compute how long it really took.
    start = time.perf_counter()
    await master.append_message("m1")
    finish = time.perf_counter() - start
    
    assert DELAY_1 <= finish < DELAY_1 + TOLERANCE, (
        f"append_message('m1') should block for {DELAY_1}s"
    )
    print(f"m1 took {finish:.2f}s")
    assert [m.content for m in master.list_messages()] == ["m1"]
    print("master after m1:", [m.content for m in master.list_messages()])

    # m2: secondary1 still has its delay, secondary2 gets a bigger one
    transport.set_delay("servicename-follower2", DELAY_2)

    start = time.perf_counter()
    await master.append_message("m2")
    finish = time.perf_counter() - start

    # It should take max(DELAY_1, DELAY_2) = DELAY_2.
    assert DELAY_2 <= finish < DELAY_2 + TOLERANCE, (
        f"append_message('m2') should block for max({DELAY_1}, {DELAY_2})="
        f"{DELAY_2}s (parallel replication)"
    )
    print(f"m2 took {finish:.2f}s")
    assert [m.content for m in master.list_messages()] == ["m1", "m2"]
    print("master after m2:", [m.content for m in master.list_messages()])

    # m3: no delays
    transport.remove_delay("servicename-follower1")
    transport.remove_delay("servicename-follower2")

    start = time.perf_counter()
    await master.append_message("m3")
    finish = time.perf_counter() - start

    assert finish < TOLERANCE, f"append_message('m3') should not block"
    print(f"m3 took {finish:.2f}s")

    expected_contents = ["m1", "m2", "m3"]
    assert [m.content for m in master.list_messages()] == expected_contents
    assert [m.content for m in follower1.list_messages()] == expected_contents
    assert [m.content for m in follower2.list_messages()] == expected_contents
    print("master: ", [m.content for m in master.list_messages()])
    print("servicename-follower1: ", [m.content for m in follower1.list_messages()])
    print("servicename-follower2: ", [m.content for m in follower2.list_messages()])
