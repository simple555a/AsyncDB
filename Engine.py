from asyncio import ensure_future
from bisect import insort, bisect
from collections import UserList
from os.path import isfile
from struct import pack, unpack

from Allocator import Allocator
from AsyncFile import AsyncFile
from Node import IndexNode, ValueNode
from TaskQue import TaskQue, Task
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
        self.task_que = TaskQue()

        if not isfile(filename):
            with open(filename, 'wb') as file:
                # 0未关闭，1反之
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
            ptr = self.task_que.get(token, node.nth_value_ads(i), is_active=True)
            if ptr:
                node.ptrs_value[i] = ptr
        if not node.is_leaf:
            for i in range(len(node.ptrs_child)):
                ptr = self.task_que.get(token, node.nth_child_ads(i), is_active=True)
                if ptr:
                    node.ptrs_child[i] = ptr

    def a_command_done(self, token: Task):
        token.command_num -= 1
        if token.command_num == 0:
            if token.free_param:
                self.free(*token.free_param)
            self.task_que.clean()

    def ensure_write(self, token: Task, ptr: int, data: bytes, prt_ptr=0):
        async def coro():
            while self.commands:
                ptr, prt_ptr, data, token = self.commands.pop(0)
                ret = (len(self.commands) == 0)

                if not ((prt_ptr and self.task_que.is_canceled(token, prt_ptr)) or self.task_que.is_canceled(token,
                                                                                                             ptr)):
                    await self.async_file.write(ptr, data)

                self.a_command_done(token)
                if ret:
                    return

        if not self.commands:
            ensure_future(coro())
        self.commands.append((ptr, prt_ptr, data, token))
        token.command_num += 1


class Engine(BasicEngine):
    def __init__(self, filename: str):
        super().__init__(filename)

    async def get(self, key):
        token = self.task_que.create(is_active=False)
        token.command_num += 1

        async def travel(ptr: int):
            init = self.task_que.get(token, ptr)  # type: IndexNode
            if not init:
                init = await self.async_file.exec(ptr, lambda f: IndexNode(file=f))

            index = bisect(init.keys, key)
            if init.keys[index - 1] == key:
                ptr = self.task_que.get(token, init.nth_value_ads(index - 1)) or init.ptrs_value[index - 1]
                val = await self.async_file.exec(ptr, lambda f: ValueNode(file=f))  # type: ValueNode
                assert val.key == key
                self.a_command_done(token)
                return val.value

            elif not init.is_leaf:
                ptr = self.task_que.get(token, init.nth_child_ads(index)) or init.ptrs_child[index]
                return await travel(ptr)

        # root ptrs实时更新
        index = bisect(self.root.keys, key)
        if self.root.keys[index - 1] == key:
            ptr = self.root.ptrs_value[index - 1]
            val = await self.async_file.exec(ptr, lambda f: ValueNode(file=f))  # type: ValueNode
            assert val.key == key
            self.a_command_done(token)
            return val.value

        elif not self.root.is_leaf:
            return await travel(self.root.ptrs_child[index])

    def set(self, key, value):
        # 强一致性，非纯异步
        token = self.task_que.create(is_active=True)

        def replace(prt_ptr: int, address: int, ptr: int):
            self.file.seek(ptr)
            org_val = ValueNode(file=self.file)

            # 最后写入一个新Val
            val = ValueNode(key, value)
            self.file.seek(self.async_file.size)
            self.async_file.size += val.size
            val.dump(self.file)
            # 被替换节点状态设为0
            self.file.seek(ptr)
            self.file.write(pack('B', 0))

            token.free_param = (org_val.ptr, org_val.size)
            # 同步
            self.task_que.set(token, address, org_val.ptr, val.ptr)
            # root可能改变
            self.time_travel(token, self.root)
            self.ensure_write(token, address, pack('Q', val.ptr), prt_ptr)

        def split(prt_ptr: int, address: int, prt: IndexNode, child_index: int, child: IndexNode):
            org_prt = prt.clone()
            org_child = child.clone()

            # 一半数据给sibling
            mi_index = (len(child.keys) - 1) // 2 + 1
            sibling = IndexNode(is_leaf=child.is_leaf)
            sibling.keys = child.keys[mi_index:]
            sibling.ptrs_value = child.ptrs_value[mi_index:]
            del child.keys[mi_index:]
            del child.ptrs_value[mi_index:]
            if not sibling.is_leaf:
                sibling.ptrs_child = child.ptrs_child[mi_index:]
                del child.ptrs_child[mi_index:]

            # parent需一个值
            prt.keys.insert(child_index, child.keys.pop())
            prt.ptrs_value.insert(child_index, child.ptrs_value.pop())

            # 分配空间
            child_b = bytes(child)
            sibling_b = bytes(sibling)
            child.ptr = self.malloc(child.size)
            sibling.ptr = self.malloc(sibling.size)

            prt.ptrs_child[child_index] = child.ptr
            prt.ptrs_child.insert(child_index + 1, sibling.ptr)
            prt_b = bytes(prt)
            prt.ptr = self.malloc(prt.size)
            # RAM数据更新完毕

            # 同步

    def remove(self):
        pass

    def items(self):
        pass
