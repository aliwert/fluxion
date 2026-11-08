import asyncio
import concurrent.futures
import subprocess
import sys
import textwrap
import threading
import time
from importlib.metadata import version

import pytest

import fluxion
from fluxion import Engine, TaskHandle


def run_isolated(source, timeout=10):
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_public_api_is_exposed():
    assert isinstance(fluxion.__version__, str)
    assert fluxion.__version__
    assert version("fluxion") == fluxion.__version__
    assert Engine.__name__ == "Engine"
    assert TaskHandle.__name__ == "TaskHandle"
    assert Engine.__module__ == "fluxion._core"
    assert TaskHandle.__module__ == "fluxion._core"


def test_engine_configuration_is_exposed():
    engine = Engine(worker_count=2, queue_capacity=16)
    try:
        assert engine.worker_count == 2
        assert engine.queue_capacity == 16
    finally:
        engine.shutdown()


def test_engine_uses_core_defaults():
    engine = Engine()
    try:
        assert engine.worker_count >= 1
        assert engine.queue_capacity == 1024
    finally:
        engine.shutdown()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"worker_count": 0},
        {"queue_capacity": 0},
    ],
)
def test_invalid_configuration_raises_value_error(kwargs):
    with pytest.raises(ValueError):
        Engine(**kwargs)


def test_negative_configuration_raises_overflow_error():
    with pytest.raises(OverflowError):
        Engine(worker_count=-1)


def test_submit_and_wait_return_python_objects():
    engine = Engine(worker_count=2, queue_capacity=8)
    try:
        sentinel = object()

        handle = engine.submit(lambda: sentinel)

        assert isinstance(handle, TaskHandle)
        assert handle.wait() is sentinel
        assert engine.submit(lambda: [1, 2, 3]).wait() == [1, 2, 3]
    finally:
        engine.shutdown()


def test_python_exception_propagates_and_worker_survives():
    class CallbackError(Exception):
        pass

    engine = Engine(worker_count=1, queue_capacity=8)
    try:
        error = CallbackError("callback failed")

        def fail():
            raise error

        handle = engine.submit(fail)

        with pytest.raises(CallbackError, match="callback failed") as caught:
            handle.wait()

        assert caught.value is error
        assert engine.submit(lambda: 7).wait() == 7
    finally:
        engine.shutdown()


def test_multiple_handles_can_be_waited_out_of_order():
    engine = Engine(worker_count=2, queue_capacity=16)
    try:
        handles = [
            engine.submit(lambda value=value: value * 2) for value in range(5)
        ]

        results = [handles[index].wait() for index in reversed(range(5))]

        assert results == [8, 6, 4, 2, 0]
    finally:
        engine.shutdown()


def test_wait_can_only_be_called_once():
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        handle = engine.submit(lambda: 1)

        assert handle.wait() == 1
        with pytest.raises(RuntimeError, match="already been consumed"):
            handle.wait()
    finally:
        engine.shutdown()


def test_dropped_handles_do_not_block_workers():
    engine = Engine(worker_count=1, queue_capacity=16)
    try:
        completed = []

        for value in range(5):
            engine.submit(lambda value=value: completed.append(value))

        assert engine.submit(lambda: "marker").wait() == "marker"
        assert completed == list(range(5))
    finally:
        engine.shutdown()


def test_shutdown_waits_for_accepted_tasks_and_preserves_results():
    engine = Engine(worker_count=2, queue_capacity=16)
    handles = [engine.submit(lambda value=value: value * value) for value in range(8)]

    engine.shutdown()

    assert [handle.wait() for handle in handles] == [0, 1, 4, 9, 16, 25, 36, 49]


def test_submit_after_shutdown_raises_runtime_error():
    engine = Engine(worker_count=1, queue_capacity=4)
    engine.shutdown()

    with pytest.raises(RuntimeError, match="shut down"):
        engine.submit(lambda: 1)

    with pytest.raises(RuntimeError, match="shut down"):
        engine.shutdown()


def test_submit_rejects_non_callable():
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        with pytest.raises(TypeError, match="not callable"):
            engine.submit(42)
    finally:
        engine.shutdown()


def test_queue_full_maps_to_runtime_error():
    engine = Engine(worker_count=1, queue_capacity=1)
    started = threading.Event()
    release = threading.Event()

    def blocking_task():
        started.set()
        assert release.wait(5)
        return "first"

    try:
        first = engine.submit(blocking_task)
        assert started.wait(5)

        second = engine.submit(lambda: "second")
        with pytest.raises(RuntimeError, match="queue is full"):
            engine.submit(lambda: "third")
    finally:
        release.set()

    assert first.wait() == "first"
    assert second.wait() == "second"
    engine.shutdown()


def test_multiple_python_threads_can_submit_and_wait():
    engine = Engine(worker_count=4, queue_capacity=64)
    results = []
    errors = []

    def submit_batch(offset):
        try:
            handles = [
                engine.submit(lambda value=value: value)
                for value in range(offset, offset + 5)
            ]
            results.extend(handle.wait() for handle in handles)
        except BaseException as error:
            errors.append(error)

    threads = [
        threading.Thread(target=submit_batch, args=(offset,))
        for offset in (0, 5, 10, 15)
    ]

    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)

        assert not errors
        assert all(not thread.is_alive() for thread in threads)
        assert sorted(results) == list(range(20))
    finally:
        engine.shutdown()


def test_wait_releases_gil_while_callback_is_pending():
    result = run_isolated(
        """
        from fluxion import Engine

        engine = Engine(worker_count=1, queue_capacity=4)
        try:
            assert engine.submit(lambda: 42).wait() == 42
        finally:
            engine.shutdown()
        """
    )

    assert result.returncode == 0, result.stderr


def test_shutdown_releases_gil_while_waiting_for_callbacks():
    result = run_isolated(
        """
        from fluxion import Engine

        engine = Engine(worker_count=2, queue_capacity=8)
        handles = [
            engine.submit(lambda value=value: value) for value in range(8)
        ]
        engine.shutdown()
        assert [handle.wait() for handle in handles] == list(range(8))
        """
    )

    assert result.returncode == 0, result.stderr


def test_implicit_engine_destruction_releases_gil():
    result = run_isolated(
        """
        from fluxion import Engine

        engine = Engine(worker_count=1, queue_capacity=4)
        handle = engine.submit(lambda: 99)
        del engine
        assert handle.wait() == 99
        """
    )

    assert result.returncode == 0, result.stderr


def test_shutdown_from_worker_callback_is_rejected_and_engine_remains_usable():
    engine = Engine(worker_count=1, queue_capacity=8)
    errors = []

    def callback():
        try:
            engine.shutdown()
        except RuntimeError as error:
            errors.append(error)
        return 42

    try:
        assert engine.submit(callback).wait() == 42
        assert len(errors) == 1
        assert "cannot be shut down from one of its worker threads" in str(errors[0])
        assert engine.submit(lambda: 7).wait() == 7
    finally:
        engine.shutdown()

    with pytest.raises(RuntimeError, match="already been shut down"):
        engine.shutdown()


def test_engine_destruction_from_worker_callback_drains_accepted_tasks():
    engine = Engine(worker_count=1, queue_capacity=8)
    callback_started = threading.Event()
    release_callback = threading.Event()
    completed = []

    def callback():
        nonlocal engine
        callback_started.set()
        assert release_callback.wait(5)
        del engine
        return "destroyed"

    destructor = engine.submit(callback)
    queued = [
        engine.submit(lambda value=value: completed.append(value) or value)
        for value in range(4)
    ]

    assert callback_started.wait(5)
    release_callback.set()

    assert destructor.wait() == "destroyed"
    assert [handle.wait() for handle in queued] == [0, 1, 2, 3]
    assert completed == [0, 1, 2, 3]


def test_concurrent_submit_and_shutdown_have_deterministic_admission():
    engine = Engine(worker_count=2, queue_capacity=512)
    barrier = threading.Barrier(5)
    accepted = []
    rejected = []
    accepted_lock = threading.Lock()

    def submit_batch():
        barrier.wait()
        local_handles = []

        for _ in range(32):
            try:
                local_handles.append(engine.submit(lambda: 1))
            except RuntimeError as error:
                with accepted_lock:
                    rejected.append(error)

        with accepted_lock:
            accepted.extend(local_handles)

    def shutdown():
        barrier.wait()
        engine.shutdown()

    threads = [
        threading.Thread(target=submit_batch),
        threading.Thread(target=submit_batch),
        threading.Thread(target=submit_batch),
        threading.Thread(target=submit_batch),
        threading.Thread(target=shutdown),
    ]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)

    assert all(not thread.is_alive() for thread in threads)
    assert len(accepted) + len(rejected) == 128
    assert all("already been shut down" in str(error) for error in rejected)
    assert [handle.wait() for handle in accepted] == [1] * len(accepted)


def test_task_lifecycle_and_cancel_before_start():
    engine = Engine(worker_count=1, queue_capacity=8)
    started = threading.Event()
    release = threading.Event()
    executed = []

    def blocker():
        started.set()
        assert release.wait(5)

    try:
        blocking_handle = engine.submit(blocker)
        assert started.wait(5)

        handle = engine.submit(lambda: executed.append(True))

        assert handle.status == "queued"
        assert not handle.done()
        assert not handle.cancelled()
        assert handle.cancel() is True
        assert handle.cancel() is False
        assert handle.status == "cancelled"
        assert handle.done()
        assert handle.cancelled()

        with pytest.raises(concurrent.futures.CancelledError):
            handle.wait()

        assert handle.status == "cancelled"
        assert executed == []

        release.set()
        assert blocking_handle.wait() is None
    finally:
        release.set()
        engine.shutdown()


def test_cancel_prefetched_task_prevents_execution():
    engine = Engine(worker_count=1, queue_capacity=32)
    blocker_started = threading.Event()
    release_blocker = threading.Event()
    running_started = threading.Event()
    release_running = threading.Event()
    target_executed = []

    def blocker():
        blocker_started.set()
        assert release_blocker.wait(5)

    def running():
        running_started.set()
        assert release_running.wait(5)

    try:
        blocking_handle = engine.submit(blocker)
        assert blocker_started.wait(5)

        running_handle = engine.submit(running)
        target = engine.submit(lambda: target_executed.append(True))
        fillers = [engine.submit(lambda: None) for _ in range(6)]

        release_blocker.set()
        assert running_started.wait(5)

        assert target.status == "queued"
        assert target.cancel() is True

        release_running.set()
        assert running_handle.wait() is None

        with pytest.raises(concurrent.futures.CancelledError):
            target.wait()

        assert target.status == "cancelled"
        assert target_executed == []
        assert blocking_handle.wait() is None
        assert all(handle.wait() is None for handle in fillers)
    finally:
        release_blocker.set()
        release_running.set()
        engine.shutdown()


def test_wait_timeout_preserves_handle_and_does_not_cancel_task():
    engine = Engine(worker_count=1, queue_capacity=8)
    started = threading.Event()
    release = threading.Event()

    def blocking_task():
        started.set()
        assert release.wait(5)
        return "finished"

    try:
        handle = engine.submit(blocking_task)
        assert started.wait(5)

        with pytest.raises(concurrent.futures.TimeoutError):
            handle.wait(timeout=0.05)

        assert handle.status == "running"
        assert not handle.done()
        assert handle.cancel() is False

        release.set()
        assert handle.wait(timeout=5) == "finished"
        assert handle.status == "completed"
        assert handle.done()
        assert not handle.cancelled()
    finally:
        release.set()
        engine.shutdown()


@pytest.mark.parametrize("timeout", [0.0, -1.0])
def test_zero_and_negative_timeouts_are_immediate(timeout):
    engine = Engine(worker_count=1, queue_capacity=8)
    started = threading.Event()
    release = threading.Event()

    def blocking_task():
        started.set()
        assert release.wait(5)
        return "finished"

    try:
        handle = engine.submit(blocking_task)
        assert started.wait(5)

        with pytest.raises(concurrent.futures.TimeoutError):
            handle.wait(timeout=timeout)

        release.set()
        assert handle.wait(timeout=5) == "finished"
    finally:
        release.set()
        engine.shutdown()


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_timeouts_raise_value_error(timeout):
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        handle = engine.submit(lambda: 42)

        with pytest.raises(ValueError, match="finite"):
            handle.wait(timeout=timeout)

        assert handle.wait(timeout=5) == 42
    finally:
        engine.shutdown()


def test_out_of_range_timeout_raises_value_error():
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        handle = engine.submit(lambda: 42)

        with pytest.raises(ValueError, match="out of range"):
            handle.wait(timeout=1e300)

        assert handle.wait(timeout=5) == 42
    finally:
        engine.shutdown()


def test_status_reports_callback_failure_before_wait():
    class CallbackError(Exception):
        pass

    engine = Engine(worker_count=1, queue_capacity=4)
    callback_finished = threading.Event()
    error = CallbackError("callback failed")

    def fail():
        callback_finished.set()
        raise error

    try:
        handle = engine.submit(fail)
        assert callback_finished.wait(5)

        deadline = time.monotonic() + 5
        while not handle.done() and time.monotonic() < deadline:
            time.sleep(0.001)

        assert handle.done()
        assert handle.status == "failed"
        assert not handle.cancelled()

        with pytest.raises(CallbackError, match="callback failed") as caught:
            handle.wait()

        assert caught.value is error
        assert handle.status == "failed"
    finally:
        engine.shutdown()


def test_completed_status_is_visible_before_wait():
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        handle = engine.submit(lambda: "done")

        deadline = time.monotonic() + 5
        while not handle.done() and time.monotonic() < deadline:
            time.sleep(0.001)

        assert handle.done()
        assert handle.status == "completed"
        assert not handle.cancelled()
        assert handle.wait() == "done"
        assert handle.status == "completed"
    finally:
        engine.shutdown()


def test_submit_async_returns_future_bound_to_running_loop():
    async def main():
        loop = asyncio.get_running_loop()
        engine = Engine(worker_count=2, queue_capacity=8)
        try:
            future = engine.submit_async(lambda: 42)

            assert isinstance(future, asyncio.Future)
            assert future.get_loop() is loop
            assert not future.done()
            assert await future == 42
        finally:
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_from_background_event_loop_thread():
    started = threading.Event()
    result = {}
    errors = []

    def run_loop():
        async def main():
            loop = asyncio.get_running_loop()
            engine = Engine(worker_count=1, queue_capacity=8)
            try:
                future = engine.submit_async(lambda: 42)
                assert future.get_loop() is loop
                started.set()
                result["value"] = await future
            finally:
                engine.shutdown()

        try:
            asyncio.run(main())
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run_loop)
    thread.start()
    try:
        assert started.wait(5)
        thread.join(5)
        assert not thread.is_alive()
        assert errors == []
        assert result["value"] == 42
    finally:
        thread.join(5)


def test_submit_async_requires_a_running_event_loop():
    engine = Engine(worker_count=1, queue_capacity=4)
    try:
        with pytest.raises(RuntimeError, match="running event loop"):
            engine.submit_async(lambda: 1)
    finally:
        engine.shutdown()


def test_submit_async_rejects_non_callable_and_submissions_after_shutdown():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=4)
        try:
            with pytest.raises(TypeError, match="not callable"):
                engine.submit_async(42)

            engine.shutdown()
            with pytest.raises(RuntimeError, match="shut down"):
                engine.submit_async(lambda: 1)
        finally:
            with pytest.raises(RuntimeError, match="shut down"):
                engine.shutdown()

    asyncio.run(main())


def test_submit_async_queue_full_does_not_leave_unobserved_error():
    async def main():
        loop = asyncio.get_running_loop()
        messages = []
        loop.set_exception_handler(lambda _loop, context: messages.append(context))
        engine = Engine(worker_count=1, queue_capacity=1)
        started = threading.Event()
        release = threading.Event()

        def blocking_task():
            started.set()
            assert release.wait(5)
            return "first"

        try:
            first = engine.submit(blocking_task)
            assert await asyncio.to_thread(started.wait, 5)
            second = engine.submit(lambda: "second")

            with pytest.raises(RuntimeError, match="queue is full"):
                engine.submit_async(lambda: "third")

            await asyncio.sleep(0)
            assert not messages

            release.set()
            assert await asyncio.to_thread(first.wait, 5) == "first"
            assert await asyncio.to_thread(second.wait, 5) == "second"
        finally:
            release.set()
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_preserves_callback_exception_identity():
    class CallbackError(Exception):
        pass

    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        error = CallbackError("async callback failed")

        def fail():
            raise error

        try:
            future = engine.submit_async(fail)

            with pytest.raises(CallbackError, match="async callback failed") as caught:
                await future

            assert caught.value is error
            assert caught.value.__traceback__ is not None
        finally:
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_multiple_concurrent_submissions():
    async def main():
        engine = Engine(worker_count=2, queue_capacity=64)
        try:
            futures = [
                engine.submit_async(lambda value=value: value * 2)
                for value in range(16)
            ]

            assert await asyncio.gather(*futures) == [value * 2 for value in range(16)]
        finally:
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_queued_cancellation_prevents_execution():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        started = threading.Event()
        release = threading.Event()
        executed = []

        def blocker():
            started.set()
            assert release.wait(5)

        try:
            blocker_handle = engine.submit(blocker)
            assert await asyncio.to_thread(started.wait, 5)

            future = engine.submit_async(lambda: executed.append(True))
            assert not future.done()
            assert future.cancel() is True

            with pytest.raises(asyncio.CancelledError):
                await future

            release.set()
            marker = engine.submit(lambda: 17)
            assert await asyncio.to_thread(marker.wait, 5) == 17
            assert await asyncio.to_thread(blocker_handle.wait, 5) is None
            assert executed == []
        finally:
            release.set()
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_cancel_accepts_message_keyword():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        try:
            future = engine.submit_async(lambda: 42)

            if sys.version_info < (3, 11):
                with pytest.raises(TypeError):
                    future.cancel(msg="cancelled by test")
                assert await future == 42
            else:
                assert future.cancel(msg="cancelled by test") is True
                with pytest.raises(asyncio.CancelledError) as caught:
                    await future
                assert caught.value.args == ("cancelled by test",)
        finally:
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_wait_for_timeout_cancels_task_awaiting_future():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        started = threading.Event()
        release = threading.Event()
        executed = []

        def callback():
            started.set()
            assert release.wait(5)
            executed.append(True)
            return "done"

        try:
            future = engine.submit_async(callback)
            assert await asyncio.to_thread(started.wait, 5)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(future, 0.05)
            assert future.cancelled()

            release.set()
            marker = engine.submit(lambda: 11)
            assert await asyncio.to_thread(marker.wait, 5) == 11
            assert executed == [True]
        finally:
            release.set()
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_cancellation_after_start_does_not_interrupt_callback():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        started = threading.Event()
        release = threading.Event()
        executed = []

        def callback():
            started.set()
            assert release.wait(5)
            executed.append(True)
            return 7

        try:
            future = engine.submit_async(callback)
            assert await asyncio.to_thread(started.wait, 5)
            assert future.cancel() is True

            with pytest.raises(asyncio.CancelledError):
                await future

            release.set()
            marker = engine.submit(lambda: 11)
            assert await asyncio.to_thread(marker.wait, 5) == 11
            assert executed == [True]
            assert future.cancelled()
        finally:
            release.set()
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_cancel_after_completion_preserves_result():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        try:
            future = engine.submit_async(lambda: 42)
            assert await future == 42

            assert future.cancel() is False
            assert future.result() == 42
        finally:
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_cancellation_completion_races_are_safe():
    async def main():
        engine = Engine(worker_count=2, queue_capacity=64)
        try:
            for _ in range(64):
                future = engine.submit_async(lambda: 1)
                await asyncio.sleep(0)

                if future.cancel():
                    with pytest.raises(asyncio.CancelledError):
                        await future
                else:
                    assert await future == 1

                await asyncio.sleep(0)
        finally:
            engine.shutdown()

    asyncio.run(main())


def test_submit_async_discards_result_after_event_loop_closed():
    engine = Engine(worker_count=1, queue_capacity=8)
    started = threading.Event()
    release = threading.Event()
    loop = asyncio.new_event_loop()
    shutdown = False

    async def submit():
        def callback():
            started.set()
            assert release.wait(5)
            return "done"

        return engine.submit_async(callback)

    try:
        future = loop.run_until_complete(submit())
        assert started.wait(5)
        loop.close()
        release.set()

        engine.shutdown()
        shutdown = True

        assert not future.done()
    finally:
        release.set()
        if not loop.is_closed():
            loop.close()
        if not shutdown:
            engine.shutdown()


def test_submit_async_keeps_event_loop_responsive():
    async def main():
        loop = asyncio.get_running_loop()
        started = asyncio.Event()
        release = threading.Event()
        engine = Engine(worker_count=1, queue_capacity=8)

        def callback():
            loop.call_soon_threadsafe(started.set)
            assert release.wait(5)
            return "done"

        try:
            future = engine.submit_async(callback)
            await asyncio.wait_for(started.wait(), 5)

            for _ in range(3):
                await asyncio.sleep(0)

            assert not future.done()
            release.set()
            assert await asyncio.wait_for(future, 5) == "done"
        finally:
            release.set()
            engine.shutdown()

    asyncio.run(main())


def test_engine_shutdown_with_pending_async_tasks_completes_futures():
    async def main():
        engine = Engine(worker_count=1, queue_capacity=8)
        started = threading.Event()
        release = threading.Event()
        shutdown = False

        def blocking_task():
            started.set()
            assert release.wait(5)
            return "first"

        try:
            first = engine.submit_async(blocking_task)
            second = engine.submit_async(lambda: "second")
            assert await asyncio.to_thread(started.wait, 5)

            shutdown_task = asyncio.create_task(asyncio.to_thread(engine.shutdown))
            release.set()
            await asyncio.wait_for(shutdown_task, 5)
            shutdown = True

            assert await asyncio.wait_for(first, 5) == "first"
            assert await asyncio.wait_for(second, 5) == "second"
        finally:
            release.set()
            if not shutdown:
                engine.shutdown()

    asyncio.run(main())


def test_submit_async_completes_future_on_event_loop_thread():
    async def main():
        loop_thread = threading.get_ident()
        engine = Engine(worker_count=1, queue_capacity=8)
        try:
            future = engine.submit_async(lambda: 42)
            observed_threads = []
            set_result = future.set_result

            def checked_set_result(value):
                observed_threads.append(threading.get_ident())
                set_result(value)

            future.set_result = checked_set_result

            assert await future == 42
            assert observed_threads == [loop_thread]
        finally:
            engine.shutdown()

    asyncio.run(main())


def test_engine_context_manager_shuts_down_on_exit():
    with Engine(worker_count=1, queue_capacity=4) as engine:
        assert engine.submit(lambda: 42).wait() == 42

    with pytest.raises(RuntimeError, match="shut down"):
        engine.submit(lambda: 1)

    with pytest.raises(RuntimeError, match="shut down"):
        engine.shutdown()


def test_engine_context_manager_propagates_block_exception_and_shuts_down():
    class BlockError(Exception):
        pass

    with pytest.raises(BlockError, match="block failed"):
        with Engine(worker_count=1, queue_capacity=4) as engine:
            handle = engine.submit(lambda: "accepted")
            raise BlockError("block failed")

    assert handle.wait() == "accepted"

    with pytest.raises(RuntimeError, match="shut down"):
        engine.submit(lambda: 1)


def test_engine_context_manager_ignores_duplicate_shutdown_on_clean_exit():
    engine = Engine(worker_count=1, queue_capacity=4)

    with engine as entered:
        assert entered is engine
        assert entered.submit(lambda: 42).wait() == 42
        engine.shutdown()

    with pytest.raises(RuntimeError, match="shut down"):
        engine.submit(lambda: 1)


def test_engine_context_manager_shutdown_error_takes_precedence():
    class BlockError(Exception):
        pass

    engine = Engine(worker_count=1, queue_capacity=4)

    with pytest.raises(RuntimeError, match="engine has already been shut down") as caught:
        with engine:
            engine.shutdown()
            raise BlockError("block failed")

    assert isinstance(caught.value.__context__, BlockError)
    assert caught.value.__cause__ is None
    assert not caught.value.__suppress_context__
