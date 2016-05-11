from os.path import isfile
from struct import pack, unpack

from Allocator import Allocator
from AsyncFile import AsyncFile
from Node import IndexNode
from TaskQueue import TaskQueue, Task
from Tinker import Tinker

MIN_DEGREE = 128


class BasicEngine:
    # 处理基础事务
    def __init__(self, filename: str):
        self.allocator = Allocator()
        self.task_queue = TaskQueue()

        if not isfile(filename):
            with open(filename, 'wb') as file:
                file.write(b'\x00')
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

    async def coro_exec(self, ptr: int, data: bytes):
        pass


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
