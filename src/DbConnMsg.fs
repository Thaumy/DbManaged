namespace DbManaged

open fsharper.op.Alias

type DbConnMsg =
    { /// 主机（数据源）
      Host: string
      /// 端口
      Port: u16
      /// 用户名
      Usr: string
      /// 密码
      Pwd: string
      /// 数据库名
      Database: string
      /// 连接池大小
      Pooling: u16 }
