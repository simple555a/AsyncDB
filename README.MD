# AsyncDB
传统数据库面对高并发，采用多线程和锁。本作采用协程和异步IO，基于Python开发，正移植到Node.js。

通过对B-Tree进行调整，能做到许多传统数据库做不到的优化。比如，先发指令能知道后发指令，从而取消无用IO。

这是最先进的数据库IO模型，快得像匹小野马！

## 注意事项
* Python 3.5以上版本

* 所有key必须可以使用bisect排序

* 强ACID，即使软件崩溃，数据也是安全的，但对硬件断电等情况不做保证。

* 数据库非正常关闭，下一次打开，程序会自动修复数据库，这可能会耗费时间。

## 使用方法
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
items = await db.items(item_from='开始', item_to='结束', max_len=2048)

# 关闭数据库
await db.close()
```