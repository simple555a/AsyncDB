from asyncio import ensure_future, get_event_loop
from os.path import getsize
from time import time

from Engine import Engine

NUM = 100
NAME = 'Test.db'


async def insert_search_test():
    engine = Engine(NAME)
    for i in range(NUM):
        engine.set(i, i)
        print('set', i)
    for i in range(NUM):
        ensure_future(search(engine, i))
    await engine.close()


async def search_test():
    engine = Engine(NAME)
    for _ in range(3):
        for i in range(NUM):
            ensure_future(search(engine, i))
    await engine.close()


async def search(engine, key):
    value = await engine.get(key)
    if value != key:
        print('Output:', value, 'Expect:', key)


if __name__ == '__main__':
    def main(action):
        time_go = time()
        get_event_loop().run_until_complete(action())
        print('文件大小：', getsize(NAME))
        print('耗时：', int(time() - time_go), '秒')


    # main(insert_search_test)
    main(search_test)
