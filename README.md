## 注意：只能解析ROW格式的日志

### 操作参数说明：

* update：解析所有的更新
* updatekey：解析指定主键的更新
* insert：解析所有插入
* insertkey：解析指定主键的插入
* insertuniquekey：解析去除重复主键的插入
* delete：解析所有删除
* deletekey：解析指定主键的删除
* deletetoinsert：把删除的语句反转为插入语句
* all：解析包括update insert 和delete 所有语句
* allkey：解析指定主键的 update insert 和delete 语句

### 配置说明：

配置目录在项目的etc目录

```
[configs]
#数据库配置
host=127.0.0.1
user=root
passwd=root
port=3306
#解析binlog的文件排序 asc 升序 desc 降序
orderfile=asc
#binlog存放目录
binglogdir=data/binlog/
#binlog中间数据目录
sqlfiledir=data/binlogsql/
#sql结果保存目录
resultdir=data/result/
#解压binlog同时开启的进程数
binlogprocess=4
#解析sql同时开启的进程数
sqlprocess=4
#批量插入数量
insertmaxnum=500
#删除的时候是否开启全字段匹配
deletewhere=1

```

### 命令使用参数说明：

* -d 数据库名称 （必传参数 不支持传多库）
* -a 操作参数 （必传参数 详细见上面的操作参数说明 只能传单个值）
* -c 是否需要利用mysqlbinlog工具解析（默认False关闭）如果设置为True 记得在项目的data/binlog/目录下放入数据库的原生binlog 可以多进程解析加快解析速度
* -t 表名 （默认改库下的所有表）可以一次传入多个表，用空格分隔多表
* -k 主键参数 （默认False）当使用主键过滤的时候改参数为必传参数 多个数值用空格分隔
* -sp --start-position 解析binlog的时候传入的参数（默认空）
* -ep --stop-position 你懂的就不解释了
* -sd --start-datetime 你懂的就不解释了
* -ed --stop-datetime 你懂的就不解释了

### 例子：

cd 到项目目录

```

python run.py -d "mydb" -a "allkey" -c "True" -t "mytable1 mytable2 mytable3" -k "1 2 3"

```

上面的意思是 解析mydb库下面的关于 mytable1 到 mytable3 所有主键属于 1,2,3 的所有 update,insert和delete的语句 并且开启原生binlog的解析