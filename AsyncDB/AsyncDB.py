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

    def __getitem__(self, key):
        async def coro():
            return self.cache[key] if key in self.cache else await self.engine.get(key)

        self.assert_open()
        return coro()

    def __setitem__(self, key, value):
        self.assert_open()
        if key not in self.cache or self.cache[key] != value:
            self.cache[key] = value
            self.engine.set(key, value)

    def pop(self, key):
        self.assert_open()
        if key in self.cache:
            del self.cache[key]
        return self.engine.pop(key)

    def items(self, item_from=None, item_to=None, max_len=0, reverse=False):
        self.assert_open()
        return self.engine.items(item_from, item_to, max_len, reverse)

    async def close(self):
        self.assert_open()
        self.open = False
        if self.engine.task_que.que:
            await self.engine.lock.acquire()
            await self.engine.lock.acquire()
        self.engine.close()

    def assert_open(self):
        if not self.open:
            raise Exception('Closed DB')
