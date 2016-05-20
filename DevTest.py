from asyncio import get_event_loop, ensure_future, sleep
from os import remove
from os.path import isfile

from AsyncDB import AsyncDB

NAME = 'Test.db'


# c, i = consistency, isolation
async def ci_t():
    if isfile(NAME):
        remove(NAME)

    cmp = {}
    db = AsyncDB(NAME)

    async def check(key, expect):
        output = await db[key]
        assert output == expect

    _ = cmp, check, ensure_future
    record = open('$Record.txt', 'r')
    for command in record.readlines():
        print(command)
        if 'ensure_future' in command:
            exec(command)
            await sleep(0)
        else:
            exec(command)
            exec(command.replace('db', 'cmp'))

    await db.close()
    print('CI OK')


def main():
    loop = get_event_loop()
    for i in range(10):
        loop.run_until_complete(ci_t())
    remove(NAME)


if __name__ == '__main__':
    main()
