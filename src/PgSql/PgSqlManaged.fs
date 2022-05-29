namespace DbManaged.PgSql

open System
open System.Threading
open System.Data.Common
open System.Threading.Tasks
open System.Threading.Channels
open Npgsql
open fsharper.op
open fsharper.typ
open fsharper.op.Fmt
open fsharper.op.Alias
open fsharper.op.Async
open DbManaged

/// PgSql数据库管理器
type PgSqlManaged private (msg, database, d, n, min, max) as managed =
    let pool =
        new DbConnPool(msg, database, (fun s -> new NpgsqlConnection(s)), d, n, min, max)

    let sema = new SemaphoreSlim(0)

    let queuedQuery =
        Channel.CreateUnbounded<DbConnection -> obj>()

    let delayedQuery =
        Channel.CreateUnbounded<DbConnection -> obj>()

    /// 复用连接
    /// 该函数会在如下情况时真正回收连接并返回：
    /// * 连接池压力大于销毁阈值时
    /// * 查询队列为空时
    let rec reuseConn conn =
        //如果连接池压力小于阈值，调用回收后连接很有可能被销毁
        //为提高连接利用率，此时从延迟查询集合中取出查询任务复用该连接
        if pool.pressure < d then
            delayedQuery.Reader.TryRead()
            |> Option'.fromOkComma
            |> ifCanUnwrapOr
            <| fun q ->
                //为其他线程取得任务而出让调度权
                Thread.Yield() |> ignore
                q conn |> ignore
                reuseConn conn
            <| fun _ -> pool.recycleConnAsync conn |> ignore
        else
            pool.recycleConnAsync conn |> ignore


    do
        fun _ ->
            task {
                let! conn = pool.fetchConnAsync ()

                while true do
                    let! q = queuedQuery.Reader.ReadAsync()
                    q conn |> ignore
                    let! _ = sema.WaitAsync()
                    ()

            }
        |> Task.RunIgnore

    /// 以连接信息构造，并指定使用的数据库和连接池大小
    new(msg, database, poolSize: u32) = new PgSqlManaged(msg, database, 0.1, 0.7, u32 (f64 poolSize * 0.1), poolSize)
    /// 以连接信息构造，并指定连接池大小
    new(msg, poolSize) = new PgSqlManaged(msg, "", poolSize)
    /// 以连接信息构造，并指定使用的数据库
    new(msg, database) = new PgSqlManaged(msg, database, 100u)
    /// 以连接信息构造
    new(msg: DbConnMsg) = new PgSqlManaged(msg, "", 100u)

    member self.Dispose() =
        self.forceLeftQueuedQuery ()
        self.forceLeftDelayedQuery ()
        pool.Dispose()

    member self.mkCmd() = new NpgsqlCommand()

    member self.executeQuery f =
        let conn = pool.fetchConn ()
        let r = f conn
        //TODO 此处无法使用Async.Start，未能知晓原因
        //Task.RunIgnore(fun _ -> reuseConn conn)
        pool.recycleConnAsync conn |> ignore
        r

    member self.executeQueryAsync(f: DbConnection -> Task<'r>) =
        task {
            let! conn = pool.fetchConnAsync ()
            let! r = f conn
            //Task.RunIgnore(fun _ -> reuseConn conn)
            pool.recycleConnAsync conn |> ignore
            return r
        }

    member self.delayQuery f =
        fun c -> f c :> obj
        |> delayedQuery.Writer.WriteAsync
        |> asTask
        |> wait

    member self.forceLeftDelayedQuery() =

        delayedQuery.Reader.TryRead
        .> Option'.fromOkComma
        .> ifCanUnwrapOr
        <. fun q -> Option.Some(async { self.executeQuery q |> ignore }, ())
        <. fun _ -> Option.None
        |> Seq.unfold
        <| ()
        |> fun s -> Async.Parallel(s, i32 max)
        |> Async.Ignore
        |> Async.RunSynchronously

    member self.queueQuery f =
        sema.Release() |> ignore

        fun c -> f c :> obj
        |> queuedQuery.Writer.WriteAsync
        |> asTask
        |> wait

    member self.forceLeftQueuedQuery() =
        if sema.CurrentCount = 0 then
            ()
        else
            Thread.Yield() |> ignore
            self.forceLeftQueuedQuery ()

    (*
        Console.WriteLine "loop"

        if queuedQuery.Reader.TryPeek() |> fst then
            //if Monitor.IsEntered queuedQuery then
                //Monitor.Exit queuedQuery

            Thread.Yield() |> ignore
            self.forceLeftDelayedQuery ()
        else
            ()
*)

    interface IDisposable with
        member i.Dispose() = managed.Dispose()

    interface IDbManaged with

        member i.mkCmd() = managed.mkCmd ()

        member i.executeQuery f = managed.executeQuery f

        member i.executeQueryAsync f = managed.executeQueryAsync f

        member i.delayQuery f = managed.delayQuery f

        member i.forceLeftDelayedQuery() = managed.forceLeftDelayedQuery ()

        member i.queueQuery f = managed.queueQuery f

        member i.forceLeftQueuedQuery() = managed.forceLeftQueuedQuery ()
