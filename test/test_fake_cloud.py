# SPDX-License-Identifier: Apache-2.0
"""FakeCloud's exception-surfacing contract: a behaviour's error must
never vanish into the server task. Reproduces the masking that cost a day
of debugging — a test passed while its assertion never ran; see
fake_cloud.py's __aexit__ docstring."""
import asyncio

import aiohttp
import pytest

from fake_cloud import FakeCloud


async def _connect_once(cloud: FakeCloud) -> None:
    """A behaviour only runs once a client connects — this is the minimal
    client both tests need, standing in for a real BridgeClient."""
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(cloud.url):
            await asyncio.sleep(0.05)  # give the handler a moment to run and raise


def test_a_behaviour_error_with_no_client_side_exception_is_raised_as_is():
    async def behavior(session):
        raise ValueError("boom")

    async def scenario():
        async with FakeCloud(behavior) as cloud:
            await _connect_once(cloud)

    with pytest.raises(ValueError, match="boom"):
        asyncio.run(scenario())


def test_a_behaviour_error_is_not_discarded_when_the_block_also_raises():
    """The bug this pins down: a behaviour's error (schema validation or
    otherwise) was silently dropped whenever the `async with` block also
    raised (e.g. a client helper's own timeout, because the client never
    got a reply from a handler that had already died). The block's
    exception won, with no trace of the real cause. Now the handler's
    exception propagates, chained onto the block's via __cause__, so
    neither is lost."""

    async def behavior(session):
        raise ValueError("the real, actionable cause")

    async def scenario():
        async with FakeCloud(behavior) as cloud:
            await _connect_once(cloud)
            raise RuntimeError("the block's own symptom")

    with pytest.raises(ValueError, match="the real, actionable cause") as excinfo:
        asyncio.run(scenario())
    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert str(excinfo.value.__cause__) == "the block's own symptom"
