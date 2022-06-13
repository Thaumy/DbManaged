namespace DbManaged

open fsharper.op.Alias

type DbConnMsg =
    { /// 主机（数据源）
      host: string
      /// 端口
      port: u16
      /// 用户名
      usr: string
      /// 密码
      pwd: string
      /// 数据库名
      db: string }
