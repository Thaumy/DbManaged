namespace DbManaged

open System

type IDbQueryQueue =
    
    inherit IDisposable

    /// 将sql语句的执行进行队列化托管，以平衡负载
    abstract member queueQuery : sql: string -> unit

    /// 强制执行队列中剩余的sql语句
    abstract member forceLeftQueuedQuery : unit -> unit
