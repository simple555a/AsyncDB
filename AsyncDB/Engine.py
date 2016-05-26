from asyncio import ensure_future
from bisect import insort, bisect, bisect_left
from collections import UserList
from contextlib import suppress
from os import remove, rename
from os.path import getsize, isfile
from pickle import load, dump, UnpicklingError
from struct import pack, unpack

from .Allocator import Allocator
from .AsyncFile import AsyncFile
from .Node import IndexNode, ValueNode
from .TaskQue import TaskQue, Task


class SortedList(UserList):
    def append(self, item):
        insort(self.data, item)


MIN_DEGREE = 64
TEMP = '__items__'


class BasicEngine:
    # deal with basic methods
    def __init__(self, filename: str):
        if not isfile(filename):
            with open(filename, 'wb') as file:
                # indicator
                file.write(b'\x00')
                # root address
                file.write(pack('Q', 9))
                self.root = IndexNode(is_leaf=True)
                self.root.dump(file)
        else:
            with open(filename, 'rb+') as file:
                if file.read(1) == b'\x00':
                    file.close()
                    return BasicEngine.repair(filename)
                else:
                    ptr = unpack('Q', file.read(8))[0]
                    file.seek(ptr)
                    self.root = IndexNode(file=file)
                    file.seek(0)
                    file.write(b'\x00')

        self.allocator = Allocator()
        self.file = open(filename, 'rb+', buffering=0)
        self.async_file = AsyncFile(filename)
        self.on_write = False
        # on_interval: (begin, end)
        self.on_interval = None
        self.command_que = SortedList()
        self.task_que = TaskQue()

    def malloc(self, size: int) -> int:
        def is_inside(ptr: int) -> bool:
            begin, end = self.on_interval
            return begin <= ptr <= end or begin <= ptr + size <= end

        ptr = self.allocator.malloc(size)
        if ptr and self.on_interval and is_inside(ptr):
            self.free(ptr, size)
            ptr = 0
        if not ptr:
            ptr = self.async_file.size
            self.async_file.size += size
        return ptr

    def free(self, ptr: int, size: int):
        self.allocator.free(ptr, size)

    # load a proper state, I call it "time_travel"
    def time_travel(self, token: Task, node: IndexNode):
        address = node.nth_value_ads(0)
        for i in range(len(node.ptrs_value)):
            ptr = self.task_que.get(token, address, node.ptr)
            if ptr:
                node.ptrs_value[i] = ptr
            address += 8
        if not node.is_leaf:
            for i in range(len(node.ptrs_child)):
                ptr = self.task_que.get(token, address, node.ptr)
                if ptr:
                    node.ptrs_child[i] = ptr
                address += 8

    def a_command_done(self, token: Task):
        token.command_num -= 1
        if token.command_num == 0:
            self.task_que.clean()

    # cum = cumulation
    # the command part of operations that change the B-Tree index: releases nodes and dispatch write commands
    def do_cum(self, token: Task, free_nodes, command_map):
        for node in free_nodes:
            self.free(node.ptr, node.size)
        for ptr, param in command_map.items():
            data, depend = param if isinstance(param, tuple) else (param, 0)
            self.ensure_write(token, ptr, data, depend)
        self.time_travel(token, self.root)
        self.root = self.root.clone()

    def ensure_write(self, token: Task, ptr: int, data: bytes, depend=0):
        async def coro():
            while self.command_que:
                ptr, token, data, depend = self.command_que.pop(0)
                cancel = depend and self.task_que.is_canceled(token, depend)
                if not cancel:
                    cancel = self.task_que.is_canceled(token, ptr)
                if not cancel:
                    # ensure it will never be that two threads write same area, which cause dead lock
                    self.on_interval = (ptr - 1, ptr + len(data) + 1)
                    await self.async_file.write(ptr, data)
                    self.on_interval = None
                self.a_command_done(token)
            self.on_write = False

        if not self.on_write:
            self.on_write = True
            ensure_future(coro())
        # sort the write command by ptr and token.id
        self.command_que.append((ptr, token, data, depend))
        token.command_num += 1

    async def close(self):
        await self.task_que.close()
        self.file.seek(0)
        self.file.write(b'\x01')
        self.file.close()
        self.async_file.close()

    @staticmethod
    def repair(filename: str):
        size = getsize(filename)
        with open(filename, 'rb') as file, open('$' + TEMP, 'wb') as items:
            file.seek(9)
            while True:
                if file.tell() == size:
                    break
                indic = file.read(1)
                if indic != b'\x01':
                    continue
                with suppress(EOFError, UnpicklingError):
                    item = load(file)
                    if isinstance(item, tuple) and len(item) == 2:
                        dump(item, items)
        rename('$' + TEMP, TEMP)


class Engine(BasicEngine):
    # B-Tree Core Part
    def __init__(self, filename: str):
        if not isfile(TEMP):
            super().__init__(filename)

        if isfile(TEMP):
            if isfile(filename):
                remove(filename)

            super().__init__(filename)
            with open(TEMP, 'rb') as items:
                while True:
                    try:
                        item = load(items)
                        self.set(*item)
                    except EOFError:
                        break
            remove(TEMP)

    async def get(self, key):
        token = self.task_que.create(is_active=False)
        token.command_num += 1

        async def travel(ptr: int):
            init = self.task_que.get(token, ptr, is_active=False)
            if not init:
                init = await self.async_file.exec(ptr, lambda f: IndexNode(file=f))

            index = bisect(init.keys, key)
            if init.keys[index - 1] == key:
                ptr = self.task_que.get(token, init.nth_value_ads(index - 1), init.ptr) or init.ptrs_value[index - 1]
                val = await self.async_file.exec(ptr, lambda f: ValueNode(file=f))
                assert val.key == key
                self.a_command_done(token)
                return val.value

            elif not init.is_leaf:
                ptr = self.task_que.get(token, init.nth_child_ads(index), init.ptr) or init.ptrs_child[index]
                return await travel(ptr)
            else:
                return self.a_command_done(token)

        # root ptrs is always up-to-date, we don't need to find a state here
        index = bisect(self.root.keys, key)
        if index - 1 >= 0 and self.root.keys[index - 1] == key:
            ptr = self.root.ptrs_value[index - 1]
            val = await self.async_file.exec(ptr, lambda f: ValueNode(file=f))
            assert val.key == key
            self.a_command_done(token)
            return val.value

        elif not self.root.is_leaf:
            return await travel(self.root.ptrs_child[index])
        else:
            return self.a_command_done(token)

    def set(self, key, value):
        token = self.task_que.create(is_active=True)
        free_nodes = []
        # command_map: {..., ptr: data OR (data, depend)}
        command_map = {}

        def replace(address: int, ptr: int, depend: int):
            self.file.seek(ptr)
            org_val = ValueNode(file=self.file)
            if org_val.value != value:
                # write a new Value node in the tail of DB file
                val = ValueNode(key, value)
                self.file.seek(self.async_file.size)
                val.dump(self.file)
                self.async_file.size += val.size
                # set the indicator of original value node to 0, it means deletion
                self.file.seek(org_val.ptr)
                self.file.write(pack('B', 0))

                # release node
                token.free_param = lambda: self.free(org_val.ptr, org_val.size)
                # sync states and RAM
                self.task_que.set(token, address, org_val.ptr, val.ptr)
                # dispatch write command
                self.ensure_write(token, address, pack('Q', val.ptr), depend)
            self.do_cum(token, free_nodes, command_map)

        # address = ptr of ptr
        # address is where ptr is in HDD
        def split(address: int, par: IndexNode, child_index: int, child: IndexNode, depend: int):
            org_par = par.clone()
            org_child = child.clone()

            # pass half data to sibling
            mi = (len(child.keys) - 1) // 2 + 1
            sibling = IndexNode(is_leaf=child.is_leaf)
            sibling.keys = child.keys[mi:]
            sibling.ptrs_value = child.ptrs_value[mi:]
            del child.keys[mi:]
            del child.ptrs_value[mi:]
            if not sibling.is_leaf:
                sibling.ptrs_child = child.ptrs_child[mi:]
                del child.ptrs_child[mi:]

            # parent needs another key
            par.keys.insert(child_index, child.keys.pop())
            par.ptrs_value.insert(child_index, child.ptrs_value.pop())

            # allocator HDD space
            child_b = bytes(child)
            sibling_b = bytes(sibling)
            child.ptr = self.malloc(child.size)
            sibling.ptr = self.malloc(sibling.size)

            par.ptrs_child[child_index] = child.ptr
            par.ptrs_child.insert(child_index + 1, sibling.ptr)
            par_b = bytes(par)
            par.ptr = self.malloc(par.size)
            # data in RAM is up-to-date

            # release
            free_nodes.extend((org_par, org_child))
            # sync
            _ = None
            for ptr, head, tail in ((address, org_par.ptr, par.ptr),
                                    (org_par.ptr, org_par, _), (org_child.ptr, org_child, _),
                                    (par.ptr, _, par), (child.ptr, _, child), (sibling.ptr, _, sibling)):
                self.task_que.set(token, ptr, head, tail)
            # commands
            command_map.update({address: (pack('Q', par.ptr), depend),
                                par.ptr: par_b, child.ptr: child_b, sibling.ptr: sibling_b})

        cursor = self.root
        address = 1
        depend = 0
        # root is full
        if len(cursor.keys) == 2 * MIN_DEGREE - 1:
            # create a new root
            root = IndexNode(is_leaf=False)
            root.ptrs_child.append(self.root.ptr)
            split(address, root, 0, self.root, depend)
            self.root = cursor = root

        # do until meet leaf
        while not cursor.is_leaf:
            index = bisect(cursor.keys, key)
            # check if key exist already
            if cursor.keys[index - 1] == key:
                return replace(cursor.nth_value_ads(index - 1), cursor.ptrs_value[index - 1], cursor.ptr)

            ptr = cursor.ptrs_child[index]
            child = self.task_que.get(token, ptr)
            if not child:
                self.file.seek(ptr)
                child = IndexNode(file=self.file)
            self.time_travel(token, child)

            i = bisect_left(child.keys, key)
            if i < len(child.keys) and child.keys[i] == key:
                return replace(child.nth_value_ads(i), child.ptrs_value[i], child.ptr)

            if len(child.keys) == 2 * MIN_DEGREE - 1:
                split(address, cursor, index, child, depend)
                if cursor.keys[index] < key:
                    # path is transfer to sibling
                    index += 1
                    ptr = cursor.ptrs_child[index]
                    child = self.task_que.get(token, ptr)

            address = cursor.nth_child_ads(index)
            depend = cursor.ptr
            cursor = child

        # arrive leaf
        index = bisect(cursor.keys, key)
        # cursor might be leaf and empty
        if cursor is self.root and cursor.keys and cursor.keys[index - 1] == key:
            return replace(cursor.nth_value_ads(index - 1), cursor.ptrs_value[index - 1], cursor.ptr)

        org_cursor = cursor.clone()
        val = ValueNode(key, value)
        val_b = bytes(val)
        val.ptr = self.malloc(val.size)
        self.file.seek(val.ptr)
        self.file.write(val_b)

        cursor.keys.insert(index, val.key)
        cursor.ptrs_value.insert(index, val.ptr)
        cursor_b = bytes(cursor)
        cursor.ptr = self.malloc(cursor.size)
        # data in RAM is up-to-date

        # release
        free_nodes.append(org_cursor)
        # sync
        _ = None
        for ptr, head, tail in ((address, org_cursor.ptr, cursor.ptr),
                                (org_cursor.ptr, org_cursor, _), (cursor.ptr, _, cursor)):
            self.task_que.set(token, ptr, head, tail)
        # command
        command_map.update({address: (pack('Q', cursor.ptr), depend), cursor.ptr: cursor_b})
        self.do_cum(token, free_nodes, command_map)

    def remove(self, key):
        # you should be very familiar with B-Tree algorithm, otherwise you can never understand the code
        token = self.task_que.create(is_active=True)
        free_nodes = []
        command_map = {}

        def indicate(val: ValueNode):
            self.file.seek(val.ptr)
            self.file.write(pack('B', 0))
            token.free_param = lambda: self.free(val.ptr, val.size)

        def fetch(ptr: int) -> IndexNode:
            result = self.task_que.get(token, ptr)
            if not result:
                self.file.seek(ptr)
                result = IndexNode(file=self.file)
            self.time_travel(token, result)
            return result

        def rotate_left(address: int, par: IndexNode, val_index: int,
                        left_child: IndexNode, right_child: IndexNode, depend: int):
            org_par = par.clone()
            org_left = left_child.clone()
            org_right = right_child.clone()

            # modify RAM
            last_val_key = left_child.keys.pop()
            last_val_ptr = left_child.ptrs_value.pop()
            val_key = par.keys[val_index]
            val_ptr = par.ptrs_value[val_index]

            par.keys[val_index] = last_val_key
            par.ptrs_value[val_index] = last_val_ptr
            right_child.keys.insert(0, val_key)
            right_child.ptrs_value.insert(0, val_ptr)

            if not left_child.is_leaf:
                last_ptr_child = left_child.ptrs_child.pop()
                right_child.ptrs_child.insert(0, last_ptr_child)

            # allocate HDD space
            left_b = bytes(left_child)
            right_b = bytes(right_child)
            left_child.ptr = self.malloc(left_child.size)
            right_child.ptr = self.malloc(right_child.size)

            par.ptrs_child[val_index] = left_child.ptr
            par.ptrs_child[val_index + 1] = right_child.ptr
            par_b = bytes(par)
            par.ptr = self.malloc(par.size)
            # data in RAM is up-to-date

            # release space unneeded
            free_nodes.extend((org_par, org_left, org_right))
            # sync
            _ = None
            for ptr, head, tail in ((address, org_par.ptr, par.ptr),
                                    (org_par.ptr, org_par, _), (par.ptr, _, par),
                                    (org_left.ptr, org_left, _), (left_child.ptr, _, left_child),
                                    (org_right.ptr, org_right, _), (right_child.ptr, _, right_child)):
                self.task_que.set(token, ptr, head, tail)
            # dispatch write command
            command_map.update({address: (pack('Q', par.ptr), depend),
                                par.ptr: par_b, left_child.ptr: left_b, right_child.ptr: right_b})

        def rotate_right(address: int, par: IndexNode, val_index: int,
                         left_child: IndexNode, right_child: IndexNode, depend: int):
            org_par = par.clone()
            org_left = left_child.clone()
            org_right = right_child.clone()

            # modify RAM
            first_val_key = right_child.keys.pop(0)
            first_val_ptr = right_child.ptrs_value.pop(0)
            val_key = par.keys[val_index]
            val_ptr = par.ptrs_value[val_index]

            par.keys[val_index] = first_val_key
            par.ptrs_value[val_index] = first_val_ptr
            left_child.keys.append(val_key)
            left_child.ptrs_value.append(val_ptr)

            if not right_child.is_leaf:
                first_ptr_child = right_child.ptrs_child.pop(0)
                left_child.ptrs_child.append(first_ptr_child)

            # allocate HDD space
            left_b = bytes(left_child)
            right_b = bytes(right_child)
            left_child.ptr = self.malloc(left_child.size)
            right_child.ptr = self.malloc(right_child.size)

            par.ptrs_child[val_index] = left_child.ptr
            par.ptrs_child[val_index + 1] = right_child.ptr
            par_b = bytes(par)
            par.ptr = self.malloc(par.size)
            # data in RAM is up-to-date

            # release space unneeded
            free_nodes.extend((org_par, org_left, org_right))
            # sync
            _ = None
            for ptr, head, tail in ((address, org_par.ptr, par.ptr),
                                    (org_par.ptr, org_par, _), (par.ptr, _, par),
                                    (org_left.ptr, org_left, _), (left_child.ptr, _, left_child),
                                    (org_right.ptr, org_right, _), (right_child.ptr, _, right_child)):
                self.task_que.set(token, ptr, head, tail)
            # dispatch write command
            command_map.update({address: (pack('Q', par.ptr), depend),
                                par.ptr: par_b, left_child.ptr: left_b, right_child.ptr: right_b})

        def merge_left(address: int, par: IndexNode, val_index: int,
                       left_child: IndexNode, cursor: IndexNode, depend: int):
            org_par = par.clone()
            org_cursor = cursor.clone()

            # modify RAM
            val_key = par.keys.pop(val_index)
            val_ptr = par.ptrs_value.pop(val_index)
            del par.ptrs_child[val_index]

            cursor.keys = [*left_child.keys, val_key, *cursor.keys]
            cursor.ptrs_value = [*left_child.ptrs_value, val_ptr, *cursor.ptrs_value]
            if not left_child.is_leaf:
                cursor.ptrs_child = [*left_child.ptrs_child, *cursor.ptrs_child]

            # allocate HDD space
            cursor_b = bytes(cursor)
            cursor.ptr = self.malloc(cursor.size)

            par.ptrs_child[val_index] = cursor.ptr
            par_b = bytes(par)
            par.ptr = self.malloc(par.size)
            # data in RAM is up-to-date

            # release space unneeded
            free_nodes.extend((org_par, org_cursor, left_child))
            # sync
            _ = None
            for ptr, head, tail in ((address, org_par.ptr, par.ptr),
                                    (org_par.ptr, org_par, _), (par.ptr, _, par),
                                    (org_cursor.ptr, org_cursor, _), (cursor.ptr, _, cursor),
                                    (left_child.ptr, left_child, _)):
                self.task_que.set(token, ptr, head, tail)
            # dispatch write command
            command_map.update({address: (pack('Q', par.ptr), depend), par.ptr: par_b, cursor.ptr: cursor_b})

        def merge_right(address: int, par: IndexNode, val_index: int,
                        cursor: IndexNode, right_child: IndexNode, depend: int):
            org_par = par.clone()
            org_cursor = cursor.clone()

            # modify RAM
            val_key = par.keys.pop(val_index)
            val_ptr = par.ptrs_value.pop(val_index)
            del par.ptrs_child[val_index + 1]

            cursor.keys.extend((val_key, *right_child.keys))
            cursor.ptrs_value.extend((val_ptr, *right_child.ptrs_value))
            if not cursor.is_leaf:
                cursor.ptrs_child.extend(right_child.ptrs_child)

            # allocate HDD space
            cursor_b = bytes(cursor)
            cursor.ptr = self.malloc(cursor.size)

            par.ptrs_child[val_index] = cursor.ptr
            par_b = bytes(par)
            par.ptr = self.malloc(par.size)
            # data in RAM is up-to-date

            # release space unneeded
            free_nodes.extend((org_par, org_cursor, right_child))
            # sync
            _ = None
            for ptr, head, tail in ((address, org_par.ptr, par.ptr),
                                    (org_par.ptr, org_par, _), (par.ptr, _, par),
                                    (org_cursor.ptr, org_cursor, _), (cursor.ptr, _, cursor),
                                    (right_child.ptr, right_child, _)):
                self.task_que.set(token, ptr, head, tail)
            # dispatch write command
            command_map.update({address: (pack('Q', par.ptr), depend), par.ptr: par_b, cursor.ptr: cursor_b})

        def travel(address: int, init: IndexNode, key, depend: int):
            index = bisect(init.keys, key) - 1

            def key_in_leaf():
                org_init = init.clone()
                self.file.seek(init.ptrs_value[index])
                val = ValueNode(file=self.file)
                # modify RAM
                del init.keys[index]
                del init.ptrs_value[index]
                # allocate HDD space
                init_b = bytes(init)
                init.ptr = self.malloc(init.size)
                # release space unneeded
                indicate(val)
                free_nodes.append(org_init)
                # sync
                _ = None
                for ptr, head, tail in ((address, org_init.ptr, init.ptr),
                                        (org_init.ptr, org_init, _), (init.ptr, _, init)):
                    self.task_que.set(token, ptr, head, tail)
                # dispatch write command
                command_map.update({address: (pack('Q', init.ptr), depend), init.ptr: init_b})

            def root_is_empty(successor: IndexNode):
                free_nodes.append(self.root)
                _ = None
                for ptr, head, tail in ((address, self.root.ptr, successor.ptr),
                                        (self.root.ptr, self.root, _), (successor.ptr, _, successor)):
                    self.task_que.set(token, ptr, head, tail)
                command_map[address] = pack('Q', successor.ptr)
                self.root = successor

            # find key
            if index >= 0 and init.keys[index] == key:
                # in leaf
                if init.is_leaf:
                    return key_in_leaf()
                # in inner nodes
                else:
                    left_ptr = init.ptrs_child[index]
                    left_child = fetch(left_ptr)
                    right_ptr = init.ptrs_child[index + 1]
                    right_child = fetch(right_ptr)

                    if len(left_child.keys) >= MIN_DEGREE:
                        rotate_left(address, init, index, left_child, right_child, depend)
                        return travel(init.nth_child_ads(index + 1), right_child, key, init.ptr)

                    elif len(right_child.keys) >= MIN_DEGREE:
                        rotate_right(address, init, index, left_child, right_child, depend)
                        return travel(init.nth_child_ads(index), left_child, key, init.ptr)

                    else:
                        merge_left(address, init, index, left_child, right_child, depend)
                        if len(self.root.keys) == 0:
                            root_is_empty(right_child)
                        return travel(init.nth_child_ads(index), right_child, key, init.ptr)
            # travel down
            elif not init.is_leaf:
                index += 1
                ptr = init.ptrs_child[index]
                cursor = fetch(ptr)

                if len(cursor.keys) < MIN_DEGREE:
                    left_sibling = right_sibling = None

                    if index - 1 >= 0:
                        left_ptr = init.ptrs_child[index - 1]
                        left_sibling = fetch(left_ptr)

                        if len(left_sibling.keys) >= MIN_DEGREE:
                            rotate_left(address, init, index - 1, left_sibling, cursor, depend)
                            return travel(init.nth_child_ads(index), cursor, key, init.ptr)

                    if index + 1 < len(init.ptrs_child):
                        right_ptr = init.ptrs_child[index + 1]
                        right_sibling = fetch(right_ptr)

                        if len(right_sibling.keys) >= MIN_DEGREE:
                            rotate_right(address, init, index, cursor, right_sibling, depend)
                            return travel(init.nth_child_ads(index), cursor, key, init.ptr)

                    if left_sibling:
                        index -= 1
                        merge_left(address, init, index, left_sibling, cursor, depend)
                    else:
                        merge_right(address, init, index, cursor, right_sibling, depend)
                    if len(self.root.keys) == 0:
                        root_is_empty(cursor)
                return travel(init.nth_child_ads(index), cursor, key, init.ptr)

        travel(1, self.root, key, 0)
        self.do_cum(token, free_nodes, command_map)

    async def items(self, item_from=None, item_to=None, max_len=0):
        assert item_from <= item_to if item_from and item_to else True
        if item_from is not None and item_from == item_to:
            value = await self.get(item_from)
            return [(item_from, value)]

        token = self.task_que.create(is_active=False)
        token.command_num += 1
        result = []

        async def travel(init: IndexNode):
            async def get_item(index: int):
                ptr = init.ptrs_value[index]
                val = await self.async_file.exec(ptr, lambda f: ValueNode(file=f))
                return val.key, val.value

            async def get_child(index: int) -> IndexNode:
                ptr = init.ptrs_child[index]
                child = self.task_que.get(token, ptr, is_active=False)
                if not child:
                    child = await self.async_file.exec(ptr, lambda f: IndexNode(file=f))
                self.time_travel(token, child)
                return child

            # lo_key >= item_from
            # hi_key >  item_to
            lo = 0 if item_from is None else bisect_left(init.keys, item_from)
            hi = len(init.keys) if item_to is None else bisect(init.keys, item_to)

            # check lo_key predecessor contains desired keys
            if not init.is_leaf and (item_from is None or lo == len(init.keys) or init.keys[lo] > item_from):
                child = await get_child(lo)
                await travel(child)

            for i in range(lo, hi):
                if max_len and len(result) >= max_len:
                    return
                item = await get_item(i)
                result.append(item)

                if not init.is_leaf:
                    child = await get_child(i + 1)
                    await travel(child)

        await travel(self.root)
        self.a_command_done(token)
        return result
