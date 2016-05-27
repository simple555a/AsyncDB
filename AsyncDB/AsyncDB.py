from asyncio import ensure_future
from collections import UserDict

from .Engine import Engine


class Cache(UserDict):
    def __init__(self, max_len=128):
        super().__init__()
        self.max_len = max_len

    def __setitem__(self, key, value):
        self.data[key] = value
        if len(self.data) > self.max_len:
            self.data.popitem()


class AsyncDB:
    def __init__(self, filename: str):
        self.cache = Cache()
        self.engine = Engine(filename)
        self.open = True

    async def get(self, key):
        self.assert_open()
        return self.cache[key] if key in self.cache else await self.engine.get(key)

    async def set(self, key, value):
        self.assert_open()
        if key not in self.cache or self.cache[key] != value:
            self.cache[key] = value
            await self.engine.set(key, value)

    async def pop(self, key):
        self.assert_open()
        if key in self.cache:
            del self.cache[key]
        return await self.engine.pop(key)

    async def items(self, item_from=None, item_to=None, max_len=0, reverse=False):
        self.assert_open()
        return await self.engine.items(item_from, item_to, max_len, reverse)

    async def close(self):
        self.open = False
        await ensure_future(self.engine.close())

    def assert_open(self):
        if not self.open:
            raise Exception('Closed DB')
