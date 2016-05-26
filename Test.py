from asyncio import get_event_loop, sleep, ensure_future
from os import remove
from os.path import isfile
from random import randint

from AsyncDB import AsyncDB

T = 10000  # iter times
M = 10000  # range of keys and values
NAME = 'Test.db'


# The idea of the test is randomized controlled trial
# cmp is a normal Python dict. The DB should act exactly the same with cmp
async def acid_t():
    if isfile(NAME):
        remove(NAME)

    cmp = {}
    db = AsyncDB(NAME)

    # due to async, need a coro to check output and expected value
    async def check(key, expect):
        output = await db[key]
        assert output == expect

    # use randint to exec series of random operation
    for i in range(T):
        # setitem
        if randint(0, 1):
            rand_key = randint(0, M)
            rand_value = randint(0, M)
            print('set', rand_key, 'to', rand_value)
            cmp[rand_key] = rand_value
            db[rand_key] = rand_value

        # delitem
        if randint(0, 1):
            rand_key = randint(0, M)
            print('del', rand_key)
            if rand_key in cmp:
                del cmp[rand_key]
            del db[rand_key]

        # getitem
        if randint(0, 1):
            rand_key = randint(0, M)
            expect = cmp.get(rand_key)
            print('get', rand_key)
            ensure_future(check(rand_key, expect))
            await sleep(0)

    # iter
    cmp_items = list(cmp.items())
    for key, value in cmp_items:
        print('iter', key)
        db_value = await db[key]
        assert db_value == value
    cmp_items.sort()

    items = await db.items()
    for key, value in items:
        print('compare', key)
        assert cmp[key] == value
    assert len(items) == len(cmp_items)

    # test params of iter
    max_i = len(cmp_items) - 1
    i_from = randint(0, max_i - 1)
    i_to = randint(i_from + 1, max_i)
    sub_items = cmp_items[i_from:i_to]

    items = await db.items(item_from=sub_items[0][0], item_to=sub_items[-1][0])
    assert items == sub_items
    max_len = randint(1, M)
    items = await db.items(item_from=sub_items[0][0], item_to=sub_items[-1][0], max_len=max_len)
    assert len(items) == min(max_len, len(sub_items))
    print('iter params OK')
    await db.close()

    # re-open the DB, it should still act like cmp
    db = AsyncDB(NAME)
    for key, value in cmp_items:
        print('po_iter', key)
        db_value = await db[key]
        assert db_value == value
    await db.close()
    print('ACID OK')


def main():
    loop = get_event_loop()
    for i in range(100):
        loop.run_until_complete(acid_t())
        remove(NAME)


if __name__ == '__main__':
    main()
