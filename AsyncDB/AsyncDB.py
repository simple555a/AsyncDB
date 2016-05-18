from asyncio import get_event_loop
from collections import UserDict
from collections.abc import Awaitable
from sys import exit

from .Engine import Engine


def handler(loop, context):
    loop.default_exception_handler(context)
    exit()


loop = get_event_loop()
loop.set_exception_handler(handler)


class Cache(UserDict):
    def __init__(self, max_len=1024):
        super().__init__()
        self.max_len = max_len

    def __setitem__(self, key, value):
        self.data[key] = value
        if len(self.data) > self.max_len:
            self.data.popitem()

    def remove(self, key):
        if key in self.data:
            del self.data[key]


class AsyncDB:
    def __init__(self, filename: str):
        self.cache = Cache()
        self.engine = Engine(filename)

    def __getitem__(self, key) -> Awaitable:
        async def coro():
            return self.cache[key] if key in self.cache else await self.engine.get(key)

        return coro()

    def __setitem__(self, key, value):
        self.cache[key] = value
        self.engine.set(key, value)

    def __delitem__(self, key):
        self.cache.remove(key)
        self.engine.remove(key)

    def items(self, item_from=None, item_to=None, max_len=0) -> Awaitable:
        return self.engine.items(item_from, item_to, max_len)

    async def close(self):
        await self.engine.close()
