# bounded_async_executor

[![Build Status](https://travis-ci.org/lukeyeager/bounded_async_executor.svg?branch=master)](https://travis-ci.org/lukeyeager/bounded_async_executor)

I found myself writing too much code that looked like this:
```python
def download_urls(urls):
    downloaded = 0

    # Use concurrent.futures to create a pool of worker threads
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = set()
        for url in urls:
            # Create a future for each url
            futures.add(executor.submit(download_url, url))

            # Bound the results so that `futures` doesn't take up too much memory
            while len(futures) >= 1000:
                done, futures = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                for future in done:
                    try:
                        future.result()
                        downloaded += 1
                    except Exception as e:
                        print(e)

        # Process the remaining futures
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
                downloaded += 1
            except Exception as e:
                print(e)

    print('Downloaded {} files successfully.'.format(downloaded))
```

So, I wrote a library to abstract away much of that complexity:
```python
def download_urls(urls):
    downloaded = 0

    def on_success(result):
        nonlocal downloaded
        downloaded += 1

    def on_error(error):
        print(error)

    with bounded_async_executor.Executor(download_url, on_success, on_error) as executor:
        for url in urls:
            executor.add(url)

    print('Downloaded {} files successfully.'.format(downloaded))
```
