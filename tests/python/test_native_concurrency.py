import subprocess
import sys
import textwrap
import threading
import time

import fluxion._core as core
import pytest
from fluxion import Engine

pytestmark = pytest.mark.skipif(
    not hasattr(core, "_test_probe"),
    reason="native probes are available only in debug builds",
)


def run_isolated(source, timeout=15):
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        capture_output=True,
        check=False,
        text=True,
        timeout=timeout,
    )


def test_independent_native_tasks_reach_a_barrier_concurrently():
    probe_type = core._NativeProbe
    probe_operation = core._test_probe
    engine = Engine(worker_count=4, queue_capacity=16)
    probe = probe_type(participants=4, timeout=5)

    try:
        handles = [engine.submit_native(probe_operation, probe) for _ in range(4)]
        for handle in handles:
            assert handle.result(5) is True
    finally:
        probe.release()
        engine.close()


def test_python_threads_keep_running_while_native_work_waits():
    result = run_isolated(
        """
        import threading
        import time

        import fluxion._core as core
        from fluxion import Engine

        engine = Engine(worker_count=1, queue_capacity=4)
        probe = core._NativeProbe(
            participants=1,
            block_until_release=True,
            timeout=5,
        )
        beats = [0]
        stop = threading.Event()

        def heartbeat():
            while not stop.is_set():
                beats[0] += 1
                time.sleep(0.001)

        heartbeat_thread = threading.Thread(target=heartbeat)
        heartbeat_thread.start()

        try:
            handle = engine.submit_native(core._test_probe, probe)
            assert probe.wait_started(5)

            time.sleep(0.05)
            assert beats[0] > 0

            assert probe.release()
            assert handle.result(5) is True
        finally:
            stop.set()
            heartbeat_thread.join(5)
            assert not heartbeat_thread.is_alive()
            probe.release()
            engine.close()
        """
    )

    assert result.returncode == 0, result.stderr


def test_native_wait_does_not_block_other_python_threads():
    engine = Engine(worker_count=1, queue_capacity=4)
    probe_type = core._NativeProbe
    probe_operation = core._test_probe
    probe = probe_type(participants=1, block_until_release=True, timeout=5)
    completed = []

    try:
        handle = engine.submit_native(probe_operation, probe)
        assert probe.wait_started(5)

        waiter = threading.Thread(target=lambda: completed.append(handle.result(5)))
        waiter.start()
        time.sleep(0.05)
        assert not completed

        assert probe.release()
        waiter.join(5)
        assert not waiter.is_alive()
        assert completed == [True]
    finally:
        probe.release()
        engine.close()
