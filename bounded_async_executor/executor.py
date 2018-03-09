import concurrent.futures


class Executor:
    def __init__(self, processor, success_handler=None, failure_handler=None,
                 num_workers=None, queue_size=None, processes=False):
        """
        Arguments:
        processor -- The function used to process each entry.

        Keyword arguments:
        success_handler -- Called on the main thread for each successful result
        failure_handler -- Called on the main thread for each exception
        num_workers -- The number of workers in the pool
            If unset, uses the default for concurrent.futures
        queue_size -- The maximum queue size.
            Defaults to 2 * the number of workers.
        processes -- If True, use processes instead of threads
        """
        self._process_func = processor
        self._success_func = success_handler
        self._fail_func = failure_handler

        # Create the executor
        args = dict()
        if num_workers is not None:
            args['max_workers'] = num_workers
        if processes:
            self._executor = concurrent.futures.ProcessPoolExecutor(**args)
        else:
            self._executor = concurrent.futures.ThreadPoolExecutor(**args)

        self._queue_size = (queue_size if queue_size is not None
                            else self._executor._max_workers * 2)
        self._futures = set()

    def add(self, *args, **kwargs):
        """Call the processor with the given arguments."""
        # Block if the queue is full
        while len(self._futures) > self._queue_size:
            done, self._futures = concurrent.futures.wait(
                self._futures, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                self._process_future(future)

        # Add the new future
        self._futures.add(self._executor.submit(
            self._process_func, *args, **kwargs))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._shutdown()

    def __del__(self):
        self._shutdown()

    def _process_future(self, future):
        try:
            result = future.result()
        except Exception as e:
            if self._fail_func is None:
                raise
            else:
                self._fail_func(e)

        if self._success_func is not None:
            self._success_func(result)

    def _shutdown(self):
        # Wait for all the remaining futures to complete
        for future in concurrent.futures.as_completed(self._futures):
            self._process_future(future)
