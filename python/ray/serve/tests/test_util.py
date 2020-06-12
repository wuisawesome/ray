import asyncio
import json

import numpy as np
import pytest

from ray.serve.utils import ServeEncoder, chain_future, unpack_future


def test_bytes_encoder():
    data_before = {"inp": {"nest": b"bytes"}}
    data_after = {"inp": {"nest": "bytes"}}
    assert json.loads(json.dumps(data_before, cls=ServeEncoder)) == data_after


def test_numpy_encoding():
    data = [1, 2]
    floats = np.array(data).astype(np.float32)
    ints = floats.astype(np.int32)
    uints = floats.astype(np.uint32)

    assert json.loads(json.dumps(floats, cls=ServeEncoder)) == data
    assert json.loads(json.dumps(ints, cls=ServeEncoder)) == data
    assert json.loads(json.dumps(uints, cls=ServeEncoder)) == data


@pytest.mark.asyncio
async def test_future_chaining():
    def make():
        return asyncio.get_event_loop().create_future()

    # Test 1 -> 1 chaining
    fut1, fut2 = make(), make()
    chain_future(fut1, fut2)
    fut1.set_result(1)
    assert await fut2 == 1

    # Test 1 -> 1 chaining with exception
    fut1, fut2 = make(), make()
    chain_future(fut1, fut2)
    fut1.set_exception(ValueError(""))
    with pytest.raises(ValueError):
        await fut2

    # Test many -> many chaining
    src_futs = [make() for _ in range(4)]
    dst_futs = [make() for _ in range(4)]
    chain_future(src_futs, dst_futs)
    [fut.set_result(i) for i, fut in enumerate(src_futs)]
    for i, fut in enumerate(dst_futs):
        assert await fut == i

    # Test 1 -> many unwrapping
    batched_future = make()
    single_futures = unpack_future(batched_future, 4)
    batched_future.set_result(list(range(4)))
    for i, fut in enumerate(single_futures):
        assert await fut == i

    # Test 1 -> many unwrapping with exception
    batched_future = make()
    single_futures = unpack_future(batched_future, 4)
    batched_future.set_exception(ValueError(""))
    for future in single_futures:
        with pytest.raises(ValueError):
            await future


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
