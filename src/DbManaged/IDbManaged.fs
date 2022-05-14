namespace DbManaged

open System
open System.Data
open System.Threading.Tasks
open System.Data.Common
open fsharper.typ
open fsharper.op.Alias

/// PgSql数据库管理器
type IDbManaged =

    abstract member makeCmd : unit -> DbCommand
    
    abstract member executeQuery : f: (DbConnection -> 'r) -> Result'<'r, exn>
    abstract member executeQueryAsync : f: (DbConnection -> Task<'r>) -> Result'<Task<'r>, exn>

    /// 队列化托管有关查询的任务，以平衡负载
    abstract member queueQuery : f: (DbConnection -> 'r) -> unit
    /// 强制执行队列中剩余的sql语句
    abstract member forceLeftQueuedQuery : unit -> unit
