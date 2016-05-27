from asyncio import get_event_loop

from AsyncDB import AsyncDB

M = 10000
NAME = 'Test.db'


async def write():
    db = AsyncDB(NAME)
    for i in range(M):
        await db.set(i, i)
        print('set', i)


async def read():
    db = AsyncDB(NAME)
    for i in range(M):
        value = await db.get(i)
        print('get', value)


def main():
    loop = get_event_loop()
    loop.run_until_complete(write())
    # loop.run_until_complete(read())


if __name__ == '__main__':
    main()
