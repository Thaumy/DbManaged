# DbManaged

由F#实现的函数式（伪）数据库托管库，由pilipala内核分离。

## 支持性

MySQL 8.0+  
PostgreSQL 14+

其中，在PostgreSQL下具有最佳性能。

## 功能特性

* 查询队列
* 延迟查询集合
* 连接池
* 方便的查询构造器
* 异步查询
* SQL规范化工具包(WIP)
* 经测试的并发安全性

## 如何使用

详见随附项目的单元测试，它包含使用本项目的所有最佳实践。

## 关于tag:v1.0.0

这是一个移植到JVM的MySql数据库管理器，提供了一系列简化数据库操作的API。
它允许你以类似于.NET的方式操作数据库，而不是使用JDBC游标。

需要Gradle依赖：  
`mysql:mysql-connector-java:8.0.17`
