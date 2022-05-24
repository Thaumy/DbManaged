﻿namespace DbManaged

open System.Threading.Tasks
open System.Data.Common
open fsharper.typ

/// PgSql数据库管理器
type IDbManaged =

    abstract member mkCmd : unit -> DbCommand
    
    abstract member executeQuery : f: (DbConnection -> 'r) -> Result'<'r, exn>
    abstract member executeQueryAsync : f: (DbConnection -> Task<'r>) -> Task<Result'<'r,exn>>

    /// 延迟查询任务，以平衡负载
    /// 当查询任务不需要立即执行，且对执行顺序不敏感时适用
    abstract member delayQuery : f: (DbConnection -> 'r) -> unit
    /// 强制执行延迟集合中剩余的查询
    abstract member forceLeftDelayedQuery : unit -> unit
    
    /// 队列化托管查询任务，以平衡负载
    /// 当查询任务不需要立即执行，且对执行顺序敏感时适用
    abstract member queueQuery : f: (DbConnection -> 'r) -> unit
    /// 强制执行队列中剩余的查询
    abstract member forceLeftQueuedQuery : unit -> unit
