import asyncio
import logging
import random


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")

TASKS_COUNT = 100
WORKERS_COUNT = 10
COUNT_SIMULTANEOUS_TASKS = 5
TASKS_WITH_ERROR = []


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
    "Выполняет задачи, до последней задачи в очереди"
    async with semaphore:
        while not queue.empty():
            task: dict[str, int | float] = await queue.get()
            duration = task.get("duration", 0)
            task_id = task.get("task_id")

            log.info(f"Начало выполнении задачи с id={task_id}")

            try:
                await asyncio.sleep(duration)
                if duration < 0.6:
                    raise ValueError("Low duration")
                log.info(f"Задача с id={task_id} выполнена")
                queue.task_done()
            except Exception as e:
                log.error(f"Ошибка при выполнении задачи с id={task_id}: {e}")
                TASKS_WITH_ERROR.append((task, e))


async def main():
    """Создаёт очередь и запускает воркеров в количестве WORKERS_COUNT.
    Одновременно выполняется не более COUNT_SIMULTANEOUS_TASKS задач"""
    queue = await create_queue_with_tasks(TASKS_COUNT)
    semaphore = asyncio.Semaphore(COUNT_SIMULTANEOUS_TASKS)
    await asyncio.gather(*[worker(queue, semaphore) for _ in range(WORKERS_COUNT)])


if __name__ == "__main__":
    asyncio.run(main())
    print(TASKS_WITH_ERROR)
