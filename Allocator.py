from bisect import insort, bisect_left
from collections import UserList, UserDict


class SizeQueue(UserList):
    def __init__(self):
        super().__init__()
        self.max_len = 1024

    def append(self, size: int) -> int:
        insort(self.data, size)
        if len(self.data) > self.max_len:
            return self.data.pop(0)

    def find(self, size: int) -> int:
        # 默认Queue非空
        index = bisect_left(self.data, size)
        if index < len(self.data):
            return index


class SizeMap(UserDict):
    def discard(self, size: int) -> int:
        ptrs = self.data.get(size)
        if ptrs:
            ptr = ptrs.pop()
            if not ptrs:
                del self.data[size]
            return ptr

    def insert(self, size: int, ptr: int):
        ptrs = self.data.setdefault(size, [])
        ptrs.append(ptr)


class Allocator:
    def __init__(self):
        # size_queue: [..., size]
        self.size_queue = SizeQueue()
        # size_map: {..., size: [..., ptr]}
        self.size_map = SizeMap()
        # ptr_map: {..., ptr: size}
        self.ptr_map = {}

    def malloc(self, size: int) -> int:
        if self.size_queue:
            index = self.size_queue.find(size)
            # 存在可用空间
            if index is not None:
                size_exist = self.size_queue[index]
                ptr = self.size_map.discard(size_exist)
                if size_exist not in self.size_map:
                    del self.size_queue[index]
                del self.ptr_map[ptr]
                # 剩余空间写回
                self.free(ptr + size, size_exist - size)
                return ptr

    def free(self, ptr: int, size: int):
        if size <= 0:
            return

        # 检测是否可以合并
        tail_ptr = ptr + size
        while tail_ptr in self.ptr_map:
            tail_size = self.ptr_map.pop(tail_ptr)
            self.size_map.discard(tail_size)
            if tail_size not in self.size_map:
                del self.size_queue[self.size_queue.find(tail_size)]
            tail_ptr += tail_size
        size = tail_ptr - ptr

        if size in self.size_map:
            self.ptr_map[ptr] = size
            self.size_map.insert(size, ptr)
        else:
            size_remove = self.size_queue.append(size)
            if size_remove == size:
                return
            # size未被直接移除
            else:
                self.ptr_map[ptr] = size
                self.size_map.insert(size, ptr)

            # 溢出
            if size_remove:
                for ptr in self.size_map[size_remove]:
                    del self.ptr_map[ptr]
                del self.size_map[size_remove]
