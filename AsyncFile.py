from asyncio import get_event_loop
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from io import FileIO
from os.path import getsize
from typing import Callable, Any


class FastIO:
    def __init__(self, filename: str):
        self.cursor = 0
        self.file = open(filename, 'rb+', buffering=0)

    def seek(self, offset: int):
        if offset != self.cursor:
            self.file.seek(offset)

    def read(self, offset: int, length: int):
        self.seek(offset)
        self.cursor = offset + length
        return self.file.read(length)

    def write(self, offset: int, data: bytes):
        self.seek(offset)
        self.cursor = offset + len(data)
        self.file.write(data)

    def execute(self, offset: int, action: Callable[[FileIO], Any]):
        self.seek(offset)
        result = action(self.file)
        self.cursor = self.file.tell()
        return result


class AsyncFile:
    def __init__(self, filename: str, io_num=8):
        self.size = getsize(filename)
        self.event_loop = get_event_loop()
        self.executor = ThreadPoolExecutor(io_num)
        self.io_queue = deque((FastIO(filename) for _ in range(io_num)), io_num)

    async def read(self, offset: int, length: int):
        def async_call():
            io = self.io_queue.pop()
            result = io.read(offset, length)
            self.io_queue.append(io)
            return result

        return await self.event_loop.run_in_executor(self.executor, async_call)

    async def write(self, offset: int, data: bytes):
        assert self.size >= offset + len(data)

        def async_call():
            io = self.io_queue.pop()
            io.write(offset, data)
            self.io_queue.append(io)

        await self.event_loop.run_in_executor(self.executor, async_call)

    async def execute(self, offset: int, action: Callable[[FileIO], Any]):
        # read-only
        def async_call():
            io = self.io_queue.pop()
            result = io.execute(offset, action)
            self.io_queue.append(io)
            return result

        return await self.event_loop.run_in_executor(self.executor, async_call)
