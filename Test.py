from asyncio import ensure_future, sleep, get_event_loop
from os import remove
from random import randint

from AsyncDB import AsyncDB

T = 100
M = 10000
NAME = 'Test.db'

cmp = {}


# c, i = consistency, isolation
async def ci_t():
    db = AsyncDB(NAME)

    async def check(key, expect):
        output = await db[key]
        assert output == expect

    for i in range(T):
        # 增
        if 1:
            rand_key = randint(0, M)
            if rand_key not in cmp:
                rand_value = randint(0, M)
                cmp[rand_key] = rand_value
                db[rand_key] = rand_value
                print('set', rand_key, 'to', rand_value)

        # 删
        if 0:
            rand_key = randint(0, M)
            if rand_key in cmp:
                del cmp[rand_key]
                del db[rand_key]
            print('del', rand_key)

        # 读
        if 1:
            rand_key = randint(0, M)
            print('get', rand_key)
            ensure_future(check(rand_key, cmp.get(rand_key)))
            print(len(db.engine.task_que.virtual_map))
            await sleep(0)

    # 遍历
    print('Final', len(db.engine.task_que.virtual_map))
    items = cmp.items()
    for key, value in items:
        db_value = await db[key]
        print(len(db.engine.task_que.virtual_map))
        assert db_value == value

    # items = await db.items()
    # for key, value in items:
    #     assert cmp[key] == value
    #     db_value = await db[key]
    #     assert value == db_value
    await db.close()
    print('CI OK')


# d = durability
async def d_t():
    db = AsyncDB(NAME)
    items = await db.items()
    for key, value in items:
        assert cmp[key] == value
        assert value == await db[key]

    # 检测参数是否有效
    cmp_keys = sorted(cmp.keys())
    lo_key = cmp_keys[0]
    hi_key = cmp_keys[len(cmp_keys) // 2]

    items = await db.items(lo_key, hi_key)
    assert lo_key == items[0][0]
    assert hi_key != items[-1][0]
    for i in range(len(items)):
        if i + 1 < len(items):
            assert items[i][0] < items[i + 1][0]
        assert cmp[items[0][0]] == items[0][1]

    items = await db.items(lo_key, hi_key, max_len=3)
    assert lo_key == items[0][0]
    assert len(items) == 3
    await db.close()
    print('D OK')


def main():
    loop = get_event_loop()
    loop.run_until_complete(ci_t())
    # loop.run_until_complete(d_t())
    remove(NAME)


if __name__ == '__main__':
    main()
