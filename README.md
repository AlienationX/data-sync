# data-sync

> data import

db --> hive

1. 支持sqoop模式和datax模式
2. 失败后重试，sqoop模式直接删除hdfs失败时产生的临时文件
3. datax模式支持自动创建表
4. 支持表结构顺序变更自动增加字段，结构不一致邮件报警

> data export

hive --> db

1. 支持rename模式，备份表
2. 默认创建db的表结构
