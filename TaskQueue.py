from asyncio import sleep
from bisect import bisect_left
from collections import deque


class Task:
    # 每一个Query都有对应Task，用于寻找和清理映射
    def __init__(self, task_id: int, command_num=0):
        self.task_id = task_id
        self.command_num = command_num
        self.mod_ptrs = []


class TaskQueue:
    # 通过deque确保异步下的ACID
    def __init__(self):
        self.curr_id = 0
        self.queue = deque()
        # virtual_map: {..., ptr: ([..., id], [..., memo])}
        self.virtual_map = {}

    def create(self, is_active: bool) -> Task:
        # 3种情况需一个新Task：当前Query改动索引树；Queue为空；前一个Query改动索引树
        if is_active or not self.queue or self.queue[-1].mod_ptrs:
            token = Task(self.curr_id)
            self.curr_id += 1
            self.queue.append(token)
        else:
            token = self.queue[-1]
        return token

    def set(self, token: Task, ptr: int, memo):
        # 建立一个映射
        if ptr in self.virtual_map:
            id_list, memo_list = self.virtual_map[ptr]
        else:
            id_list = []
            memo_list = []
            self.virtual_map[ptr] = (id_list, memo_list)

        # 避免重复
        if not id_list or id_list[-1] != token.task_id:
            id_list.append(token.task_id)
            memo_list.append(memo)
            token.mod_ptrs.append(ptr)
        else:
            memo_list[-1] = memo

    def get(self, token: Task, ptr: int):
        # 根据id查询一个映射
        if ptr in self.virtual_map:
            id_list, memo_list = self.virtual_map[ptr]
            index = bisect_left(id_list, token.task_id) - 1
            if index >= 0:
                return memo_list[index]

    def get_last(self, ptr):
        if ptr in self.virtual_map:
            id_list, memo_list = self.virtual_map[ptr]
            return id_list[-1], memo_list[-1]

    def clean(self):
        while self.queue:
            head = self.queue.popleft()
            if head.command_num > 0:
                self.queue.appendleft(head)
                break
            else:
                # 释放映射
                for ptr in head.mod_ptrs:
                    id_list, memo_list = self.virtual_map[ptr]
                    index = bisect_left(id_list, head.task_id)
                    del id_list[index]
                    del memo_list[index]

                    if not id_list:
                        del self.virtual_map[ptr]
        else:
            # id重计数
            self.curr_id = 0

    async def ensure_empty(self):
        # Query之后，有可能Queue非空，进行非阻塞等待，避免用锁
        while self.queue:
            await sleep(1)
