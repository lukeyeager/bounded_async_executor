import concurrent.futures


class Executor:
    """Asynchronous bounded executor.

    Used to process entries asynchronously in threads (or processes), then
    pass the results back to the main thread.

    This class is just a thin wrapper around the PoolExecutor classes from the
    built-in concurrent.futures library. The main addition is to create an
    upper bound on the number of submitted futures. This is useful to estimate
    the time to completion (e.g. with tqdm) and to bound the program's memory
    usage.
    """

    def __init__(self, processor=None, result_handler=None, error_handler=None,
                 num_workers=None, queue_size=None, processes=False):
        """
        processor -- The function used to process each entry asynchronously
            Required in order to use add()
        result_handler -- Called on the main thread for each successful result
            If unset, result is ignored
        error_handler -- Called on the main thread for each exception
            If unset, exceptions will shut down the executor
        num_workers -- The number of workers in the pool
            If unset, uses the default for concurrent.futures
        queue_size -- The maximum queue size
            Defaults to 2 * the number of workers.
        processes -- If True, use processes instead of threads
        """
        self._process_func = processor
        self._result_func = result_handler
        self._error_func = error_handler

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
        if self._process_func is None:
            raise TypeError(
                "When no processor function is specified in the constructor, "
                "the first argument to add() must be the desired processor "
                "function.")

        self.submit(self._process_func, *args, **kwargs)

    def submit(self, processor, *args, **kwargs):
        """Call the processor with the given arguments."""
        # Block if the queue is full
        while len(self._futures) > self._queue_size:
            done, self._futures = concurrent.futures.wait(
                self._futures, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                self._process_future(future)

        # Add the new future
        self._futures.add(self._executor.submit(processor, *args, **kwargs))

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
        except Exception as error:
            if self._error_func is None:
                raise
            else:
                self._error_func(error)
                return

        if self._result_func is not None:
            self._result_func(result)

    def _shutdown(self):
        # Wait for all the remaining futures to complete
        for future in concurrent.futures.as_completed(self._futures):
            self._process_future(future)
        self._futures = set()
