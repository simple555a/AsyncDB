from asyncio import get_event_loop, ensure_future, sleep
from os import remove
from os.path import isfile
from random import randint

from AsyncDB import AsyncDB

T = 10000
M = 10000
NAME = 'Test.db'


async def acid_t():
    if isfile(NAME):
        remove(NAME)

    cp = {}
    db = AsyncDB(NAME)

    async def compare(key, expect_value):
        db_value = await db[key]
        assert db_value == expect_value

    for i in range(T):
        # 增改
        if randint(0, 1):
            rand_key = randint(0, M)
            rand_value = randint(0, M)
            print('set', rand_key, 'to', rand_value)

            cp[rand_key] = rand_value
            db[rand_key] = rand_value

        # 删
        if randint(0, 1):
            rand_key = randint(0, M)
            print('del', rand_key)

            if rand_key in cp:
                del cp[rand_key]
            db.pop(rand_key)

        # 读
        if randint(0, 1):
            rand_key = randint(0, M)
            expect_value = cp.get(rand_key)

            ensure_future(compare(rand_key, expect_value))
        await sleep(0)
    # 遍历
    cp_items = list(cp.items())
    for key, value in cp_items:
        db_value = await db[key]
        assert db_value == value

    items = await db.items()
    for key, value in items:
        assert value == cp[key]
    assert len(items) == len(cp_items)
    print('iter OK')

    # 参数
    cp_items.sort()
    max_i = len(cp_items) - 1
    i_from = randint(0, max_i - 1)
    i_to = randint(i_from + 1, max_i)
    sub_items = cp_items[i_from:i_to]

    items = await db.items(item_from=sub_items[0][0], item_to=sub_items[-1][0])
    assert items == sub_items
    items = await db.items(item_from=sub_items[0][0], item_to=sub_items[-1][0], reverse=True)
    assert items == sorted(sub_items, reverse=True)
    param_len = randint(1, M)
    items = await db.items(item_from=sub_items[0][0], item_to=sub_items[-1][0], max_len=param_len)
    assert len(items) == min(param_len, len(sub_items))
    print('params OK')
    await db.close()

    db = AsyncDB(NAME)
    for key, value in cp_items:
        db_value = await db[key]
        assert db_value == value
    await db.close()
    print('ACID OK')


def main():
    loop = get_event_loop()
    for i in range(10):
        loop.run_until_complete(acid_t())
        remove(NAME)


if __name__ == '__main__':
    main()
