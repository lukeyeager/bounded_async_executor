import pytest

from bounded_async_executor import Executor


def test_simple():
    executor = Executor(lambda x: x)
    executor.add(None)


def test_with():
    with Executor(lambda x: x) as executor:
        executor.add(None)


def test_success():
    total = 0

    def success_handler(x):
        nonlocal total
        total += x

    with Executor(lambda x: x, success_handler=success_handler) as executor:
        executor.add(1)
        executor.add(1)

    assert total == 2


def test_failure():
    failure_count = 0

    def processor():
        raise ValueError

    def failure_handler(e):
        nonlocal failure_count
        failure_count += 1

    with Executor(processor, failure_handler=failure_handler) as executor:
        executor.add()
        executor.add()

    assert failure_count == 2


def test_uncaught_exception():
    def processor():
        raise RuntimeError('Intentionally throwing an exception')

    with pytest.raises(RuntimeError):
        with Executor(processor) as executor:
            executor.add()


def test_destructor():
    total = 0

    def processor(x):
        nonlocal total
        total += x

    executor = Executor(processor)
    executor.add(1)
    executor.add(1)

    assert total == 2


def test_queue_size():
    total = 0
    count = 10

    def processor(x):
        nonlocal total
        total += x

    with Executor(processor, queue_size=1) as executor:
        for _ in range(count):
            executor.add(1)

    assert total == count


def _multiprocessing_processor(x):
    """Cannot be a local function because it must be pickled."""
    return x


def test_processes():
    total = 0

    def success_handler(x):
        nonlocal total
        total += x

    with Executor(_multiprocessing_processor, success_handler=success_handler,
                  processes=True) as executor:
        executor.add(1)

    assert total == 1
