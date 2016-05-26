from bisect import insort, bisect_left
from collections import UserList, UserDict


# The DB is highly dynamic, which means the HDD space of items deleted will be released
# This file is all about GC

class SizeQue(UserList):
    def __init__(self, max_len=1024):
        super().__init__()
        self.max_len = max_len

    def append(self, size: int) -> int:
        insort(self.data, size)
        if len(self.data) > self.max_len:
            return self.data.pop(0)

    # find a size >= size requested
    def find(self, size: int) -> int:
        index = bisect_left(self.data, size)
        if index < len(self.data):
            return index


class SizeMap(UserDict):
    # discard a block of free space by on size
    def discard(self, size: int) -> int:
        ptrs = self.data.get(size)
        if ptrs:
            ptr = ptrs.pop()
            if not ptrs:
                del self.data[size]
            return ptr

    def add(self, size: int, ptr: int):
        ptrs = self.data.setdefault(size, [])
        insort(ptrs, ptr)


class Allocator:
    def __init__(self):
        # size_que: [..., size]
        self.size_que = SizeQue()
        # size_map: {..., size: [..., ptr]}
        self.size_map = SizeMap()
        # ptr_map: {..., ptr: size}
        self.ptr_map = {}

    def malloc(self, size: int) -> int:
        if self.size_que:
            index = self.size_que.find(size)
            # exist a free block that is >= size needed
            if index is not None:
                size_exist = self.size_que[index]
                ptr = self.size_map.discard(size_exist)
                if size_exist not in self.size_map:
                    del self.size_que[index]
                del self.ptr_map[ptr]
                # recycle unused part
                self.free(ptr + size, size_exist - size)
                return ptr

    def free(self, ptr: int, size: int):
        assert size >= 0
        if size == 0:
            return

        # check if can merge another free block
        tail_ptr = ptr + size
        while tail_ptr in self.ptr_map:
            tail_size = self.ptr_map.pop(tail_ptr)
            ptrs = self.size_map[tail_size]
            del ptrs[bisect_left(ptrs, tail_ptr)]
            if not self.size_map[tail_size]:
                del self.size_map[tail_size]
                del self.size_que[self.size_que.find(tail_size)]
            tail_ptr += tail_size
        size = tail_ptr - ptr

        if size in self.size_map:
            if len(self.size_map[size]) < self.size_que.max_len:
                self.ptr_map[ptr] = size
                self.size_map.add(size, ptr)
        else:
            size_remove = self.size_que.append(size)
            if size_remove == size:
                return
            # successfully append free block to allocator
            # it may fail, cuz there is a limit of length of size_que
            else:
                self.ptr_map[ptr] = size
                self.size_map.add(size, ptr)

            # the size queue is full, then del overflow part
            if size_remove:
                for ptr in self.size_map[size_remove]:
                    del self.ptr_map[ptr]
                del self.size_map[size_remove]
