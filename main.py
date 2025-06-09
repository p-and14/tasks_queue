import asyncio
import logging
import random


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")


TASKS_COUNT = 100
WORKERS_COUNT = 5
COUNT_SIMULTANEOUS_TASKS = 5


async def create_queue_with_tasks(count: int) -> asyncio.Queue:
    "Создаёт очередь с заданным количеством задач"

    queue = asyncio.Queue()

    for i in range(1, count + 1):
        await queue.put({
            "task_id": i,
            "duration": random.uniform(0.5, 2.0)
        })
    return queue


async def worker(queue: asyncio.Queue, semaphore: asyncio.Semaphore) -> None:
    "Выполняет задачи из очереди в Semaphore"

    async with semaphore:
        while True:
            task: dict[str, int | float] = await queue.get()
            duration = task.get("duration", 0)
            task_id = task.get("task_id")

            log.info(f"Начало выполнении задачи с id={task_id}")
            await asyncio.sleep(duration)
            queue.task_done()
            log.info(f"Задача с id={task_id} выполнена")
                

async def main() -> None:
    """Создаёт очередь и запускает воркеров в количестве WORKERS_COUNT.
    Одновременно выполняется не более COUNT_SIMULTANEOUS_TASKS задач"""

    queue = await create_queue_with_tasks(TASKS_COUNT)
    semaphore = asyncio.Semaphore(COUNT_SIMULTANEOUS_TASKS)
    workers = [worker(queue, semaphore) for _ in range(WORKERS_COUNT)]
    tasks: list[asyncio.Task] = []

    for w in workers:
        tasks.append(asyncio.create_task(w))
    
    await queue.join()

    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
