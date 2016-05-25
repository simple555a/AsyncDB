# AsyncDB
传统数据库面对高并发，采用多线程和锁。本作采用协程和异步IO，基于Python开发。

When traditional databases encounter concurrency problems, multiple threads and locks is a reasonable solution and is
widely used. The DB tries to solve the problem by Python Coroutine and AsyncIO.

通过对B-Tree进行调整，能做到许多传统数据库做不到的优化。

The DB can achieve some optimizations that cannot be done with traditional databases, by modifying the B-Tree algorithm.

## 注意事项 Attention
* Python 3.5.1以上版本，没有其余依赖。

Python 3.5.1 or later is required and no other dependency.

* 只能在协程环境下使用。

Only compatible with coroutine environment.

* 所有key必须可以使用bisect排序，建议使用bisect.insort测试。

All keys must be "bisectable" i.e. can be sorted by bisect.insort.

* 内置缓存，得到的结果有可能是之前结果的引用。

There is a cache inside. The result can be a reference of the previous result.

* ACID，软件可以在任意时刻崩溃，数据都是安全的，但对硬件断电之类的情况不做保证。

The DB guarantees the ACID properties with software failures but hardware.

* 非正常关闭，下一次启动，程序会自动修复数据库，这可能会相当费时。

If the DB closes unexpectedly, it will repair itself next time, which takes time.

* 以MIT协议发布。

The DB is shared under the MIT license.

## 使用方法 Usage
复制AsyncDB文件夹到任意的解释器能import的目录。如不清楚，复制到根目录即可。

Copy "AsyncDB" folder to wherever Python interpreter can import. If you are not sure, just copy it to the root then.

```Python
from AsyncDB import AsyncDB

# 打开或建立一个数据库
# Open or Create a DB
db = AsyncDB('Test.db')

# 写入
# Write
db['foo'] = 'bar'

# 读取
# Read
val = await db['foo']

# 删除
# Del
del db['foo']

# 遍历
# Iterate
items = await db.items(item_from='start', item_to='end', max_len=1024)

# 关闭
# Close
await db.close()
```

真正意义上的**动态**数据库，对key和value的类型没有**限制**

There is no restriction of types of keys and values.