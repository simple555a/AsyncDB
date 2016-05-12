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
                # 随后为8字节的root地址，初始值为9
                file.write(pack('Q', 9))
                self.root = IndexNode(is_leaf=True)
                self.root.dump(file)
        else:
            with open(filename, 'rb') as file:
                if file.read(1) == b'\x00':
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
                do_ret = (len(self.commands) == 0)

                if not ((prt_ptr and self.task_que.is_canceled(token, prt_ptr)) or self.task_que.is_canceled(token,
                                                                                                             ptr)):
                    await self.async_file.write(ptr, data)

                self.a_command_done(token)
                if do_ret:
                    return

        if not self.commands:
            ensure_future(coro())
        self.commands.append((ptr, prt_ptr, data, token))
        token.command_num += 1

    async def close(self):
        await self.task_que.close()
        self.file.seek(0)
        self.file.write(b'\x01')
        self.file.close()
        for io in self.async_file.io_que:
            io.close()


class Engine(BasicEngine):
    def __init__(self, filename: str):
        super().__init__(filename)

    async def get(self, key):
        token = self.task_que.create(is_active=False)
        token.command_num += 1

        async def travel(ptr: int):
            init = self.task_que.get(token, ptr)
            """:type : IndexNode"""
            if not init:
                init = await self.async_file.exec(ptr, lambda f: IndexNode(file=f))

            index = bisect(init.keys, key)
            if init.keys[index - 1] == key:
                ptr = self.task_que.get(token, init.nth_value_ads(index - 1)) or init.ptrs_value[index - 1]
                val = await self.async_file.exec(ptr, lambda f: ValueNode(file=f))
                """:type : ValueNode"""
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
            val = await self.async_file.exec(ptr, lambda f: ValueNode(file=f))
            """:type : ValueNode"""
            assert val.key == key
            self.a_command_done(token)
            return val.value

        elif not self.root.is_leaf:
            return await travel(self.root.ptrs_child[index])

    def set(self, key, value):
        # 强一致性，非纯异步
        token = self.task_que.create(is_active=True)
        free_nodes = []
        command_map = {}

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

            # 释放
            token.free_param = (org_val.ptr, org_val.size)
            # 同步
            self.task_que.set(token, address, org_val.ptr, val.ptr)
            # 命令
            self.ensure_write(token, address, pack('Q', val.ptr), prt_ptr)
            # root可能改变，需更新
            self.time_travel(token, self.root)

        # address为pointer的硬盘位置
        def split(address: int, prt: IndexNode, child_index: int, child: IndexNode, grd_ptr=0):
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

            # 释放
            free_nodes.extend((org_prt, org_child))
            # 同步
            _ = None
            for ptr, head, tail in ((address, org_prt.ptr, prt.ptr),
                                    (org_prt.ptr, org_prt, _), (org_child.ptr, org_child, _),
                                    (prt.ptr, _, prt), (child.ptr, _, child), (sibling.ptr, _, sibling)):
                self.task_que.set(token, ptr, head, tail)
            # 命令
            command_map.update({address: (pack('Q', prt.ptr), grd_ptr),
                                prt.ptr: prt_b, child.ptr: child_b, sibling.ptr: sibling_b})

        cursor = self.root
        address = 1
        # root准满载的情况
        if len(cursor.keys) == 2 * MIN_DEGREE - 1:
            # 新建一个root
            root = IndexNode(is_leaf=False)
            root.ptrs_child.append(self.root.ptr)
            split(address, root, 0, self.root)
            self.root = cursor = root

        prt_ptr = 9
        # 向下循环直到叶节点
        while not cursor.is_leaf:
            self.time_travel(token, cursor)
            index = bisect(cursor.keys, key)
            # 检查key是否已存在
            if cursor.keys[index - 1] == key:
                return replace(prt_ptr, cursor.nth_value_ads(index - 1), cursor.ptr)

            ptr = cursor.ptrs_child[index]
            child = self.task_que.get(token, ptr)
            if not child:
                self.file.seek(ptr)
                child = IndexNode(file=self.file)
            self.time_travel(token, child)

            if len(child.keys) == 2 * MIN_DEGREE - 1:
                split(address, cursor, index, child, prt_ptr)
                if cursor.keys[index] < key:
                    # 路径转移至sibling
                    index += 1
                    ptr = cursor.ptrs_child[index]
                    child = self.task_que.get(token, ptr, is_active=True)

            address = cursor.nth_child_ads(index)
            prt_ptr = cursor.ptr
            cursor = child

        # 到达叶节点
        index = bisect(cursor.keys, key)
        # 检测key是否已存在
        if cursor.keys and cursor.keys[index - 1] == key:
            return replace(cursor.nth_value_ads(index - 1), cursor.ptrs_value[index - 1], prt_ptr)

        org_cursor = cursor.clone()
        val = ValueNode(key, value)
        val.ptr = self.malloc(len(bytes(val)))

        # 阻塞以确保ACID
        self.file.seek(val.ptr)
        val.dump(self.file)

        cursor.keys.insert(index, key)
        cursor.ptrs_value.insert(index, val.ptr)
        cursor_b = bytes(cursor)
        cursor.ptr = self.malloc(cursor.size)
        # RAM数据更新完毕

        # 释放
        free_nodes.append(org_cursor)
        # 同步
        _ = None
        for ptr, head, tail in ((address, org_cursor.ptr, cursor.ptr), (org_cursor.ptr, org_cursor, _),
                                (cursor.ptr, _, cursor)):
            self.task_que.set(token, ptr, head, tail)
        # 命令
        command_map.update({address: (pack('Q', cursor.ptr), prt_ptr), cursor.ptr: cursor_b})
        self.time_travel(token, self.root)

        # 执行
        for command in command_map:
            ptr, params = command
            if isinstance(params, tuple):
                self.ensure_write(token, ptr, *params)
            else:
                self.ensure_write(token, ptr, params)
        for node in free_nodes:
            self.free(node.ptr, node.size)

    def remove(self):
        pass

    def items(self):
        pass
