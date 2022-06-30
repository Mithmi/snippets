from asyncio import (
    Task,
    Semaphore,
)
import asyncio
from typing import List


async def shopping(sem: Semaphore):
    while True:
        async with sem:
            print(shopping.__name__)
        await asyncio.sleep(0.25)  # Transfer control to the loop, and it will assign another job (is idle) to run.


async def coding(sem: Semaphore):
    while True:
        async with sem:
            print(coding.__name__)
        await asyncio.sleep(0.25)


async def main():
    sem = Semaphore(value=1)
    list_task: List[Task] = [asyncio.create_task(_coroutine(sem)) for _coroutine in (shopping, coding)]
    """ 
    # Normally, we will wait until all the task has done, but that is impossible in your case.
    for task in list_task:
        await task
    """
    await asyncio.sleep(2)  # So, I let the main loop wait for 2 seconds, then close the program.


asyncio.run(main())