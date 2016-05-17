from asyncio import ensure_future, get_event_loop, sleep
from os.path import getsize
from time import time

from Engine import Engine

NUM = 100
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


async def set_replace_get_t():
    engine = Engine(NAME)
    for i in range(NUM):
        engine.set(i, i)
        print('set', i)
    for i in range(NUM // 10):
        engine.set(i, 0)
        print('change', i, 'to', 0)
    for i in range(NUM):
        ensure_future(get(engine, i))
    await engine.close()


async def get(engine, key):
    value = await engine.get(key)
    print('get', value)
    if value != key:
        print('Output:', value, 'Expect:', key)


async def con_curr_t():
    engine = Engine(NAME)
    for i in (1, 2, 3, 4, 7, 8, 9, 10, 12, 13):
        engine.set(i, i)
    for i in (2, 3, 5, 6, 11):
        ensure_future(get(engine, i))
    await sleep(0)
    for i in (5, 6, 11):
        engine.set(i, i)
    for i in (2, 3, 5, 6, 11):
        ensure_future(get(engine, i))
    await engine.close()


async def delete_t():
    engine = Engine(NAME)
    for i in range(NUM):
        engine.set(i, i)
        print('set', i)
    for i in range(NUM // 10):
        engine.remove(i)
        print('del', i)
    for i in range(NUM):
        ensure_future(get(engine, i))
    await engine.close()


if __name__ == '__main__':
    def main(t):
        time_go = time()
        get_event_loop().run_until_complete(t())
        print('大小：', getsize(NAME))
        print('耗时：', int(time() - time_go), '秒')


    # main(set_get_t)
    # main(get_t)
    # main(set_replace_get_t)
    # main(con_curr_t)
    main(delete_t)
