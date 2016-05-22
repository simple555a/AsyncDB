# AsyncDB
传统数据库面对高并发，采用多线程和锁。本作采用协程和异步IO，基于Python开发，正移植到Node.js。

通过对B-Tree进行调整，能做到许多传统数据库做不到的优化。比如，先发指令能侦测后发指令，从而取消无用IO。

这是最先进的数据库IO模型，快得像匹小野马！

## 注意事项
* Python 3.5.1以上版本，没有其余依赖

* 只能在协程环境下使用

* 所有key必须可以使用bisect排序，建议使用bisect.insort测试

* 内置缓存，得到的结果有可能是之前结果的引用

* 强ACID，软件可以在任意时刻崩溃，数据都是安全的，但对硬件断电之类的情况不做保证

* 非正常关闭，下一次启动，程序会自动修复数据库，这可能会相当费时

* MIT协议发布

## 使用方法
复制AsyncDB文件夹到任意的解释器能import的目录。如不清楚，复制到程序根目录即可。

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
# item_from和item_to默认为None，表示遍历整个数据库
items = await db.items(item_from='开始', item_to='结束', max_len=2048)

# 关闭
await db.close()
```

真正意义上的**动态**数据库，对key和value的类型没有**任何限制**
