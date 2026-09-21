# SPDX-License-Identifier: Apache-2.0
"""The asset half of the client: the assets/asset_progress pumps,
`report_current_urdf_availability`'s once-per-connection restatement, and the
`asset_request` dispatch — driven against the fake cloud with the same
duck-typed `FakeRos` double as test_client_datapoints.py (real ROS URDF
detection and mesh resolution/upload are ros_runtime.py's own test suite;
this file is wire protocol and session plumbing)."""
import asyncio

from fake_cloud import FakeCloud, accepts, sequence
from helpers import make_client, run_until
from test_client_datapoints import FakeRos, _run_one_exchange

from fleetless_bridge.ros_runtime import AssetProgress, AssetsAvailable


def test_assets_available_is_pumped_out_once_connected():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        fake_ros.assets.put_nowait(
            AssetsAvailable(urdf=True, meshes=("package://foo/bar.stl", "package://foo/baz.stl"))
        )
        return await session.recv_assets_available()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload == {
        "type": "assets_available",
        "urdf": True,
        "meshes": ["package://foo/bar.stl", "package://foo/baz.stl"],
    }


def test_assets_available_with_no_meshes_is_an_empty_list_not_omitted():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        fake_ros.assets.put_nowait(AssetsAvailable(urdf=True, meshes=()))
        return await session.recv_assets_available()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload["meshes"] == []


# ---: current URDF availability is re-stated once per session, at hello -


def test_current_urdf_availability_is_reported_after_the_first_config():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_config(1, {})
        await session.recv_config_applied()
        return True

    _run_one_exchange(send_and_recv, ros=fake_ros)
    assert fake_ros.report_current_urdf_availability_calls == 1


def test_current_urdf_availability_is_not_re_reported_on_a_later_config_same_session():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_config(1, {})
        await session.recv_config_applied()
        await session.send_config(2, {})
        await session.recv_config_applied()
        return True

    _run_one_exchange(send_and_recv, ros=fake_ros)
    # Same reasoning as camera health's equivalent test: steady-state changes
    # stay on the transition-only path (RosRuntime._on_robot_description);
    # only a fresh session re-states.
    assert fake_ros.report_current_urdf_availability_calls == 1


def test_current_urdf_availability_is_reported_again_after_a_reconnect():
    """A cloud restart looks like any other dropped connection to the bridge
    and erases whatever the cloud remembered — same defect class
    `report_current_camera_health` closes for cameras, now closed for assets
    too. One `FakeRos` spans both sessions, so the call count accumulates
    the way a real robot's single long-lived RosRuntime would."""
    fake_ros = FakeRos()

    async def first_session(session):
        await session.send_config(1, {})
        await session.recv_config_applied()
        await session.close(1000, "")

    async def second_session(session):
        await session.send_config(1, {})
        await session.recv_config_applied()
        await session.drain()

    behavior = sequence(accepts(then=first_session), accepts(then=second_session))

    async def scenario():
        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, ros=fake_ros)
            await run_until(client, lambda: fake_ros.report_current_urdf_availability_calls >= 2)

    asyncio.run(scenario())
    assert fake_ros.report_current_urdf_availability_calls == 2


# --- asset_request dispatch and the asset_progress pump -----------------


def test_an_asset_request_is_dispatched_to_the_ros_runtime():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_asset_request(
            "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
            "https://cloud.example/api/bridge/assets",
            "upload-tok-1",
            meshes=["package://foo/bar.stl"],
        )
        await asyncio.sleep(0.05)  # give the dispatched coroutine a turn
        return True

    _run_one_exchange(send_and_recv, ros=fake_ros)
    assert fake_ros.sync_assets_calls == [
        (
            "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
            "https://cloud.example/api/bridge/assets",
            "upload-tok-1",
            ("package://foo/bar.stl",),
        )
    ]


def test_an_asset_request_with_no_meshes_dispatches_an_empty_tuple():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_asset_request(
            "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
            "https://cloud.example/api/bridge/assets",
            "upload-tok-1",
        )
        await asyncio.sleep(0.05)
        return True

    _run_one_exchange(send_and_recv, ros=fake_ros)
    assert fake_ros.sync_assets_calls[0][3] == ()


def test_asset_progress_is_pumped_out_once_connected():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        fake_ros.asset_progress.put_nowait(
            AssetProgress(
                sync_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
                done=1,
                total=2,
                failed=(),
                state="running",
            )
        )
        return await session.recv_asset_progress()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload == {
        "type": "asset_progress",
        "sync_id": "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
        "done": 1,
        "total": 2,
        "failed": [],
        "state": "running",
    }


def test_asset_progress_carries_the_failed_list_and_finished_state():
    """`failed` entries are `(reference, kind, details)`
    triples on the wire, `{"reference": ..., "kind": ..., "details": ...}`
    — not bare strings, and `details` is always sent, `null` when there
    are no numbers behind the entry."""
    fake_ros = FakeRos()

    async def send_and_recv(session):
        fake_ros.asset_progress.put_nowait(
            AssetProgress(
                sync_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
                done=2,
                total=2,
                failed=(("package://foo/missing.stl", "unresolvable", None),),
                state="finished",
            )
        )
        return await session.recv_asset_progress()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload["failed"] == [
        {"reference": "package://foo/missing.stl", "kind": "unresolvable", "details": None}
    ]
    assert payload["state"] == "finished"


def test_asset_progress_carries_the_store_numbers_of_a_refusal():
    """A `refused` entry the robot's store had no room for carries the
    cloud's three numbers all the way to the wire, unchanged — this is the
    only path they travel, and a developer reading "refused" with nothing
    beside it cannot tell a full store from a producer that declined."""
    fake_ros = FakeRos()

    async def send_and_recv(session):
        fake_ros.asset_progress.put_nowait(
            AssetProgress(
                sync_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
                done=1,
                total=2,
                failed=(
                    (
                        "package://foo/huge.dae",
                        "refused",
                        {
                            "store_bytes": 1000000000,
                            "used_bytes": 900000000,
                            "size_bytes": 193886766,
                        },
                    ),
                ),
                state="running",
            )
        )
        return await session.recv_asset_progress()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload["failed"] == [
        {
            "reference": "package://foo/huge.dae",
            "kind": "refused",
            "details": {
                "store_bytes": 1000000000,
                "used_bytes": 900000000,
                "size_bytes": 193886766,
            },
        }
    ]
