from asyncio import get_event_loop, sleep, ensure_future
from os import remove
from os.path import isfile
from random import randint

from AsyncDB import AsyncDB

T = 10000
M = 7000
NAME = 'Test.db'


# c, i = consistency, isolation
async def ci_t():
    if isfile(NAME):
        remove(NAME)

    cmp = {}
    db = AsyncDB(NAME)
    record = open('$Record.txt', 'w')

    async def check(key, expect):
        output = await db[key]
        assert output == expect

    for i in range(T):
        # 增改
        if randint(0, 1):
            rand_key = randint(0, M)
            rand_value = randint(0, M)
            print('set', rand_key, 'to', rand_value)
            record.write('db[{}]={}\n'.format(rand_key, rand_value))
            cmp[rand_key] = rand_value
            db[rand_key] = rand_value

        # 删
        if randint(0, 1):
            rand_key = randint(0, M)
            print('del', rand_key)
            if rand_key in cmp:
                record.write('del db[{}]\n'.format(rand_key))
                del cmp[rand_key]
            del db[rand_key]

        # 读
        if randint(0, 1):
            rand_key = randint(0, M)
            expect = cmp.get(rand_key)
            print('get', rand_key)
            record.write('ensure_future(check({}, {}))\n'.format(rand_key, expect))
            ensure_future(check(rand_key, expect))
            await sleep(0)

    # 遍历
    cmp_items = list(cmp.items())
    for key, value in cmp_items:
        print('iter', key)
        db_value = await db[key]
        assert db_value == value
    cmp_items.sort()

    items = await db.items()
    for key, value in items:
        print('check', key)
        assert cmp[key] == value
    assert len(items) == len(cmp_items)

    # 检查参数有效性
    sub_items = cmp_items[1:100]
    items = await db.items(item_from=sub_items[0][0], item_to=sub_items[-1][0])
    assert items == sub_items
    items = await db.items(item_from=sub_items[0][0], item_to=sub_items[-1][0], max_len=10)
    assert len(items) == 10
    print('Iter Param OK')

    await db.close()
    print('CI OK')


def main():
    loop = get_event_loop()
    for i in range(20):
        loop.run_until_complete(ci_t())
        remove(NAME)


if __name__ == '__main__':
    main()
