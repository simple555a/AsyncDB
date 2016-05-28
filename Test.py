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

    cmp = {}
    db = AsyncDB(NAME)

    async def compare(key, expect_value):
        db_value = await db.get(key)
        assert db_value == expect_value

    for i in range(T):
        # 增改
        if randint(0, 1):
            rand_key = randint(0, M)
            rand_value = randint(0, M)
            print('set', rand_key, 'to', rand_value)

            cmp[rand_key] = rand_value
            ensure_future(db.set(rand_key, rand_value))

        # 删
        if randint(0, 1):
            rand_key = randint(0, M)
            print('del', rand_key)

            if rand_key in cmp:
                del cmp[rand_key]
            ensure_future(db.pop(rand_key))

        # 读
        if randint(0, 1):
            rand_key = randint(0, M)
            expect_value = cmp.get(rand_key)

            ensure_future(compare(rand_key, expect_value))
    await sleep(0)
    # 遍历
    cmp_items = list(cmp.items())
    for key, value in cmp_items:
        db_value = await db.get(key)
        assert db_value == value

    items = await db.items()
    for key, value in items:
        assert value == cmp[key]
    assert len(items) == len(cmp_items)
    print('iter OK')

    # 参数
    cmp_items.sort()
    max_i = len(cmp_items) - 1
    i_from = randint(0, max_i - 1)
    i_to = randint(i_from + 1, max_i)
    sub_items = cmp_items[i_from:i_to]

    items = await db.items(item_from=sub_items[0][0], item_to=sub_items[-1][0])
    assert items == sub_items
    items = await db.items(item_from=sub_items[0][0], item_to=sub_items[-1][0], reverse=True)
    assert items == sorted(sub_items, reverse=True)
    expect_len = randint(1, M)
    items = await db.items(item_from=sub_items[0][0], item_to=sub_items[-1][0], max_len=expect_len)
    assert len(items) == min(expect_len, len(sub_items))
    print('params OK')
    await db.close()

    db = AsyncDB(NAME)
    for key, value in cmp_items:
        db_value = await db.get(key)
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
