namespace DbManaged.PgSql

open System
open System.Threading
open System.Data.Common
open System.Diagnostics
open System.Threading.Tasks
open System.Collections.Concurrent
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

    let queuedQuery = ConcurrentQueue<DbConnection -> unit>()
    let delayedQuery = ConcurrentBag<DbConnection -> unit>()

    let getQueuedQuery () =
        (*该实现使得每次仅有一个线程能够取得查询任务
        且在被获取的查询任务执行完毕前，其他线程均不能取得任务
        从而保证队列的按序执行*)
        if Monitor.TryEnter(queuedQuery) then
            match queuedQuery.TryDequeue() with
            | true, q -> Some q
            | _ ->
                Monitor.Exit(queuedQuery)
                None
        else
            None

    let getDelayedQuery () =
        delayedQuery.TryTake() |> Option'.fromOkComma

    /// 复用连接
    /// 该函数会在如下情况时真正回收连接并返回：
    /// * 连接池压力大于销毁阈值时
    /// * 查询队列为空时
    let reuseConn conn =
        //Thread.CurrentThread.Priority <- ThreadPriority.Lowest

        let rec loop () =
            //println $"loop {Thread.CurrentThread.ManagedThreadId}"
            //如果连接池压力小于阈值，调用回收后连接很有可能被销毁
            //为提高连接利用率，此时从延迟查询集合中取出查询任务复用该连接
            if pool.pressure < d then
                //优先考虑延迟查询集合任务，因为它能实现更高的并行性
                getDelayedQuery () |> ifCanUnwrapOr
                <| fun q ->
                    println $"delay {Thread.CurrentThread.ManagedThreadId} {Process.GetCurrentProcess().Threads.Count}"
                    Thread.Yield() |> ignore
                    q conn
                    loop ()
                <| (getQueuedQuery .> ifCanUnwrapOr
                    <. fun q ->
                        println $"queue {Thread.CurrentThread.ManagedThreadId} {Process.GetCurrentProcess().Threads.Count}"
                        q conn
                        Monitor.Exit(queuedQuery)
                        loop ()
                    <. fun _ -> pool.recycleConnAsync conn |> ignore)
            else
                pool.recycleConnAsync conn |> ignore

        loop ()

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
        Task.Run(fun _ -> reuseConn conn) |> ignore
        //pool.recycleConnAsync conn |> ignore
        r

    member self.executeQueryAsync f =
        task {
            let! conn = pool.fetchConnAsync ()
            let! r = f conn
            Task.Run(fun _ -> reuseConn conn) |> ignore
            //pool.recycleConnAsync conn |> ignore
            return r
        }

    member self.delayQuery f = f .> ignore |> delayedQuery.Add

    member self.forceLeftDelayedQuery() =

        getDelayedQuery .> ifCanUnwrapOr
        <. fun q -> Option.Some(async { self.executeQuery q }, ())
        <. fun _ -> Option.None
        |> Seq.unfold
        <| ()
        |> fun s -> Async.Parallel(s, i32 max)
        |> Async.Ignore
        |> Async.RunSynchronously

    member self.queueQuery f = f .> ignore |> queuedQuery.Enqueue

    member self.forceLeftQueuedQuery() =
        Monitor.Enter(queuedQuery)

        queuedQuery.TryDequeue
        .> Option'.fromOkComma
        .> ifCanUnwrapOr
        <. fun q -> Option.Some(async { managed.executeQuery q }, ())
        <. fun _ -> Option.None
        |> Seq.unfold
        <| ()
        |> Async.Sequential
        |> Async.Ignore
        |> Async.RunSynchronously

        Monitor.Exit(queuedQuery)

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
