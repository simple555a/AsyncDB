[中文版](#CHN)

# AsyncDB
When traditional databases encounter concurrency problems, multiple threads and locks is a reasonable solution and is
widely used. However, the DB tries to solve the problem by Python Coroutine and AsyncIO.

The DB can get some optimizations that cannot be done with traditional databases, by modifying the B-Tree algorithm.

## Attention
* Python 3.5.1 or later is required and no other dependency.
* Only compatible with coroutine environment.
* All keys must be "bisectable" i.e. can be sorted by bisect.insort.
* There is a cache inside. The result can be a reference of the previous result.
* The DB guarantees the ACID properties with software failures but hardware.
* If the DB closes unexpectedly, it will repair itself next time, which takes time.
* The DB is shared under the MIT license.

## Usage
Copy "AsyncDB" folder to the root of your program.

```Python
from AsyncDB import AsyncDB

# Open/Create a DB
db = AsyncDB('Test.db')

# Write
db['foo'] = 'bar'

# Read
val = await db['foo']

# Del
del db['foo']

# Iterate
items = await db.items(item_from='start', item_to='end', max_len=1024)

# Close
await db.close()
```

There is no **restriction** of types of keys and values.

###<a name="CHN"></a>中文版
传统数据库面对高并发，采用多线程和锁。本作采用协程和异步IO，基于Python开发。

通过对B-Tree进行调整，能做到许多传统数据库做不到的优化。

## 注意事项
* Python 3.5.1以上版本，没有其余依赖。
* 只能在协程环境下使用。
* 所有key必须可以使用bisect排序，建议使用bisect.insort测试。
* 内置缓存，得到的结果有可能是之前结果的引用。
* ACID，软件可以在任意时刻崩溃，数据都是安全的，但对硬件断电之类的情况不做保证。
* 非正常关闭，下一次启动，程序会自动修复数据库，这可能会相当费时。
* 以MIT协议发布。

## 使用方法
复制AsyncDB文件夹到到程序根目录即可。

```Python
from AsyncDB import AsyncDB

# 打开或建立一个数据库
db = AsyncDB('Test.db')

# 写入
db['foo'] = 'bar'

# 读取
val = await db['foo']

# 删除
del db['foo']

# 遍历
items = await db.items(item_from='start', item_to='end', max_len=1024)

# 关闭
await db.close()
```

本数据库对key和value的类型没有**限制**。