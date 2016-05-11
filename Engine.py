from asyncio import ensure_future
from bisect import insort
from collections import UserList
from os.path import isfile
from struct import pack, unpack

from Allocator import Allocator
from AsyncFile import AsyncFile
from Node import IndexNode
from TaskQueue import TaskQueue, Task
from Tinker import Tinker


class SortedList(UserList):
    def append(self, item):
        insort(self.data, item)


MIN_DEGREE = 128


class BasicEngine:
    # 处理基础事务
    def __init__(self, filename: str):
        self.allocator = Allocator()
        self.commands = SortedList()
        self.task_queue = TaskQueue()

        if not isfile(filename):
            with open(filename, 'wb') as file:
                # 0表示未关闭，1反之
                file.write(b'\x00')
                # 随后为root地址，初始值为9
                file.write(pack('Q', 9))
                self.root = IndexNode(is_leaf=True)
                self.root.dump(file)
        else:
            with open(filename, 'rb') as file:
                indicator = file.read(1)
                if indicator == b'\x00':
                    Tinker(filename).repair()

                ptr = unpack('Q', file.read(8))[0]
                file.seek(ptr)
                self.root = IndexNode(file=file)

        self.async_file = AsyncFile(filename)
        self.file = open(filename, 'rb+', buffering=0)

    def malloc(self, size: int) -> int:
        ptr = self.allocator.malloc(size)
        if not ptr:
            ptr = self.async_file.size
            self.async_file.size += size
        return ptr

    def free(self, ptr: int, size: int):
        self.allocator.free(ptr, size)

    def time_travel(self, token: Task, node: IndexNode):
        for i in range(len(node.ptrs_value)):
            result = self.task_queue.get(token, node.nth_value_address(i))
            if result:
                node.ptrs_value[i] = result

        if not node.is_leaf:
            for i in range(len(node.ptrs_child)):
                result = self.task_queue.get(token, node.nth_child_address(i))
                if result:
                    node.ptrs_child[i] = result

    def done_a_command(self, token: Task):
        token.command_num -= 1
        if token.command_num == 0:
            self.task_queue.clean()

    def ensure_exec(self, token: Task, ptr: int, data: bytes, parent_ptr=0):
        async def coro():
            while self.commands:
                ptr, parent_ptr, data, token = self.commands.pop(0)
                return_later = not self.commands

                is_canceled = False
                if (parent_ptr and self.task_queue.is_canceled(token, parent_ptr)) or self.task_queue.is_canceled(token,
                                                                                                                  ptr):
                    is_canceled = True
                if not is_canceled:
                    await self.async_file.write(ptr, data)

                self.done_a_command(token)
                if return_later:
                    return

        if not self.commands:
            ensure_future(coro())
        self.commands.append((ptr, parent_ptr, data, token))


class Engine(BasicEngine):
    def __init__(self, filename: str):
        super().__init__(filename)

    def get(self):
        pass

    def set(self):
        pass

    def pop(self):
        pass

    def items(self):
        pass
