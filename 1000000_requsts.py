import asyncio
import requests


urls = ['url_1', 'url_2', 'url_3']
bodies =[{str(i): i} for i in range(1_000_000)]


queue = asyncio.Queue(300)
semaphores = {
    url: asyncio.Semaphore(100) for url in urls
}
responses = {
    url: [] for url in urls
}
lock = asyncio.Lock()


async def create_tasks():
    for body in bodies:
        for url in urls:
            await queue.put({
                "url": url,
                "body": body
            })


def generate_headers():
    # generate random headers
    pass


def get_random_proxy():
    # get random proxy
    pass


async def worker(queue):
    while True:
        task = await queue.get()
        url = task["url"]
        headers = generate_headers()
        proxy = get_random_proxy()

        async with semaphores[url]:
            # Здесь асинхронный запрос через aiohttp
            try:
                resp = requests.post(url, task["body"], headers=headers, proxies=proxy, timeout=5)
            except Exception:
                # log error
                await queue.put(task)
                continue

        if resp.status_code == 200:
            async with lock:
                responses[task["url"]].append(resp.json())
        elif 400 <= resp.status_code < 500:
            # log error
            # проблема на стороне клиента -> возможные проблемы с заголовками или с прокси
            await queue.put(task)
        elif resp.status_code > 500:
            # log error
            # проблема на стороне сервера -> в зависимости от ответа предпринимаем различные действия
            await queue.put(task)
            await asyncio.sleep(10)


async def main():
    task_creater = asyncio.create_task(create_tasks())
    workers = [asyncio.create_task(worker(queue)) for _ in range(300)]

    await task_creater
    await queue.join()

    for w in workers:
        w.cancel()
    
    await asyncio.gather(*workers, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
