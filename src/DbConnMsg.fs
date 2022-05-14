namespace DbManaged

open fsharper.op.Alias

type DbConnMsg =
    { /// 主机（数据源）
      Host: string
      /// 端口
      Port: u16
      /// 用户名
      User: string
      /// 密码
      Password: string }
