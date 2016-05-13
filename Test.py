from asyncio import ensure_future, get_event_loop
from os.path import getsize
from time import time

from Engine import Engine

NUM = 200
NAME = 'Test.db'


async def set_get_t():
    engine = Engine(NAME)
    for i in range(NUM):
        engine.set(i, i)
        print('set', i)
    for i in range(NUM):
        ensure_future(get(engine, i))
    await engine.close()


async def get_t():
    engine = Engine(NAME)
    for i in range(NUM):
        ensure_future(get(engine, i))
    await engine.close()


async def set_get_replace_t():
    engine = Engine(NAME)
    for i in range(NUM):
        engine.set(i, i)
        print('set', i)
    for i in range(NUM // 10):
        engine.set(i, 0)
        print('set', i, 'to', 0)
    for i in range(NUM):
        ensure_future(get(engine, i))
    await engine.close()


async def get(engine, key):
    value = await engine.get(key)
    print('get', value)
    if value != key:
        print('Output:', value, 'Expect:', key)


if __name__ == '__main__':
    def main(t):
        time_go = time()
        get_event_loop().run_until_complete(t())
        print('大小：', getsize(NAME))
        print('耗时：', int(time() - time_go), '秒')


    main(set_get_t)
    # main(get_t)
    # main(set_get_replace_t)
