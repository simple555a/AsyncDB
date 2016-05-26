from asyncio import sleep
from bisect import bisect
from collections import deque, namedtuple
from collections.abc import Callable

# the key of async DB is using 'finite state machine' https://en.wikipedia.org/wiki/Finite-state_machine
# when jumping between different coros, find and load a proper state

# state is a memo
Memo = namedtuple('Memo', 'head tail')


class Task:
    # every query has a task to get/set proper states
    def __init__(self, task_id: int, is_active: bool, command_num=0):
        self.id = task_id
        self.is_active = is_active
        self.command_num = command_num

        if self.is_active:
            self.ptrs = []
            # free_param: (ptr, size)
            self.free_param = None

    def __lt__(self, other: 'Task'):
        return self.id < other.id


class TaskQue:
    def __init__(self):
        self.next_id = 0
        self.que = deque()
        # virtual_map: {..., ptr: ([..., id], [..., memo])}
        self.virtual_map = {}

    # make a task to get/set states later
    def create(self, is_active: bool) -> Task:
        if is_active or not self.que or self.que[-1].is_active:
            token = Task(self.next_id, is_active)
            self.next_id += 1
            self.que.append(token)
        else:
            token = self.que[-1]
        return token

    def set(self, token: Task, ptr: int, head, tail):
        if ptr == 0:
            return

        # make a state
        memo = Memo(head, tail)
        if ptr in self.virtual_map:
            id_list, memo_list = self.virtual_map[ptr]
        else:
            id_list = []
            memo_list = []
            self.virtual_map[ptr] = (id_list, memo_list)

        if id_list and id_list[-1] == token.id:
            memo_list[-1] = memo
        else:
            id_list.append(token.id)
            memo_list.append(memo)
            token.ptrs.append(ptr)

    def get(self, token: Task, ptr: int, depend=0, is_active=True):
        def get_depend_id():
            if depend in self.virtual_map:
                id_list, _ = self.virtual_map[depend]
                index = bisect(id_list, token.id)
                if index - 1 >= 0:
                    return id_list[index - 1]
                elif index < len(id_list):
                    return id_list[index]
            else:
                return 0

        # get a state
        if ptr in self.virtual_map:
            depend_id = get_depend_id()
            id_list, memo_list = self.virtual_map[ptr]
            index = bisect(id_list, token.id)

            result = None
            if index - 1 >= 0 and depend_id <= id_list[index - 1]:
                result = memo_list[index - 1].tail
            elif index < len(id_list) and depend_id <= id_list[index]:
                result = memo_list[index].head

            if is_active and not (isinstance(result, int) or result is None):
                result = result.clone()
            return result

    # we can know if the write command is not up-to-date i.e. there is a newer state
    def is_canceled(self, token: Task, ptr: int) -> bool:
        if ptr in self.virtual_map:
            id_list, memo_list = self.virtual_map[ptr]
            if id_list[-1] > token.id or not memo_list[-1].tail:
                return True

    def clean(self):
        while self.que:
            head = self.que.popleft()
            if head.command_num > 0:
                self.que.appendleft(head)
                break
            else:
                if head.is_active:
                    # clean states tree
                    if isinstance(head.free_param, Callable):
                        head.free_param()
                    for ptr in head.ptrs:
                        id_list, memo_list = self.virtual_map[ptr]
                        del id_list[0]
                        del memo_list[0]
                        if not id_list:
                            del self.virtual_map[ptr]
        else:
            # no tasks, we can count from 0 again
            self.next_id = 0

    async def close(self):
        await sleep(0)
        while self.que:
            await sleep(1)
