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

    def result_handler(x):
        nonlocal total
        total += x

    with Executor(lambda x: x, result_handler=result_handler) as executor:
        executor.add(1)
        executor.add(1)

    assert total == 2


def test_failure():
    failure_count = 0

    def processor():
        raise ValueError

    def error_handler(e):
        nonlocal failure_count
        failure_count += 1

    with Executor(processor, error_handler=error_handler) as executor:
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
    del executor

    assert total == 2


def test_exit_and_destructor():
    count = 100

    def _internal():
        results = list()

        def result_handler(x):
            nonlocal results
            results.append(x)

        with Executor(lambda x: x, result_handler=result_handler) as executor:
            for x in range(count):
                executor.add(x)
        return results

    results = _internal()
    assert len(results) == count


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

    def result_handler(x):
        nonlocal total
        total += x

    with Executor(_multiprocessing_processor, result_handler=result_handler,
                  processes=True) as executor:
        executor.add(1)

    assert total == 1


def test_no_processor():
    with Executor() as executor:
        with pytest.raises(TypeError):
            executor.add()
        with pytest.raises(TypeError):
            executor.add(1)
        executor.submit(lambda x: x, 1)


def test_submit():
    successes = 0
    failures = 0

    def processor(x):
        if not x % 2:
            raise RuntimeError

    def result_handler(result):
        nonlocal successes
        successes += 1

    def error_handler(error):
        nonlocal failures
        failures += 1

    with Executor(result_handler=result_handler,
                  error_handler=error_handler) as executor:
        for x in range(100):
            executor.submit(processor, x)

    assert successes == 50
    assert failures == 50


def test_wait():
    total = 0

    def result_handler(x):
        nonlocal total
        total += x

    executor = Executor(lambda x: x, result_handler)

    for _ in range(100):
        executor.add(1)

    executor.wait()

    assert total == 100
