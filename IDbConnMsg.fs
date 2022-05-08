namespace DbManaged

type IDbConnMsg =
    
    /// 主机（数据源）
    abstract member Host : string
    /// 端口
    abstract member Port : uint16
    /// 用户名
    abstract member User : string
    /// 密码
    abstract member Password : string
