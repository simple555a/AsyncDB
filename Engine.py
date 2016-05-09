from os.path import isfile
from struct import pack, unpack

from Allocator import Allocator
from AsyncFile import AsyncFile
from Node import IndexNode
from TaskQueue import TaskQueue
from Tinker import Tinker

MIN_DEGREE = 64


class BasicEngine:
    # 处理琐碎事务
    def __init__(self, filename: str):
        self.allocator = Allocator()
        self.task_queue = TaskQueue()

        if not isfile(filename):
            with open(filename, 'wb') as file:
                # 第1个字节作为Indicator，0尚未正常关闭，1反之
                file.write(b'\x00')
                # 随后8个字节为root地址，初始值为9
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

    def time_travel(self, node: IndexNode):
        pass

    def command_done(self):
        pass

    async def coro_exec(self):
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
