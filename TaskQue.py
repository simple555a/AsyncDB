from asyncio import sleep
from bisect import bisect_left
from collections import deque, namedtuple

Memo = namedtuple('Memo', 'head tail')


class Task:
    # 每一个Query都有对应Task，用于查询和清理映射
    def __init__(self, task_id: int, command_num=0):
        self.id = task_id
        self.command_num = command_num
        self.free_param = None
        self.mod_ptrs = []


class TaskQue:
    # 通过deque确保异步下的ACID
    def __init__(self):
        self.next_id = 0
        self.que = deque()
        # virtual_map: {..., ptr: ([..., id], [..., memo])}
        self.virtual_map = {}

    def create(self, is_active: bool) -> Task:
        # 3种情况需一个新Task：当前Query改动索引；Queue为空；上一个Query改动索引
        if is_active or not self.que or self.que[-1].mod_ptrs:
            token = Task(self.next_id)
            self.next_id += 1
            self.que.append(token)
        else:
            token = self.que[-1]
        return token

    def set(self, token: Task, ptr: int, head, tail):
        # 建立一个映射
        memo = Memo(head, tail)
        if ptr in self.virtual_map:
            id_list, memo_list = self.virtual_map[ptr]
        else:
            id_list = []
            memo_list = []
            self.virtual_map[ptr] = (id_list, memo_list)

        # 避免重复
        if id_list and id_list[-1] == token.id:
            memo_list[-1] = memo
        else:
            id_list.append(token.id)
            memo_list.append(memo)
            token.mod_ptrs.append(ptr)

    def get(self, token: Task, ptr: int):
        # 根据id查询一个映射
        if ptr in self.virtual_map:
            id_list, memo_list = self.virtual_map[ptr]
            index = bisect_left(id_list, token.id)
            if index < len(id_list):
                return memo_list[index].head
            index -= 1
            if index >= 0:
                return memo_list[index].tail

    def is_canceled(self, token: Task, ptr: int):
        if ptr in self.virtual_map:
            id_list, _ = self.virtual_map[ptr]
            if id_list[-1] != token.id:
                return True

    def clean(self):
        while self.que:
            head = self.que.popleft()
            if head.command_num > 0:
                self.que.appendleft(head)
                break
            else:
                # 清理映射
                for ptr in head.mod_ptrs:
                    id_list, memo_list = self.virtual_map[ptr]
                    del id_list[0]
                    del memo_list[0]
                    if not id_list:
                        del self.virtual_map[ptr]
        else:
            # 重计数
            self.next_id = 0

    async def close(self):
        # Queue有可能非空，进行非阻塞等待
        while self.que:
            await sleep(1)
