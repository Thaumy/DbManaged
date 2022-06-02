namespace DbManaged.PgSql

open System
open System.Threading
open System.Data.Common
open System.Threading.Tasks
open System.Threading.Channels
open System.Threading.Tasks.Dataflow
open Npgsql
open fsharper.op
open fsharper.typ
open fsharper.op.Alias
open fsharper.op.Async
open DbManaged

/// PgSql数据库管理器
type PgSqlManaged private (msg, database, d, n, min, max) as managed =
    let pool =
        new DbConnPool(msg, database, (fun s -> new NpgsqlConnection(s)), d, n, min, max)

    let queueSema = new SemaphoreSlim(0)//用于队列查询任务的完成精确计数
    let queueQueryConn = pool.fetchConnAsync().Result//用于队列查询的专用连接

    let queuedQuery =
        fun q ->
            q queueQueryConn |> ignore
            queueSema.Wait()
        |> ActionBlock<DbConnection -> obj>

    let delayedQuery =
        Channel.CreateUnbounded<DbConnection -> obj>()

    let usedConn = Channel.CreateUnbounded<DbConnection>()//复用池

    let realPoolPressure () =
        pool.pressure
        - (f64 usedConn.Reader.Count
           / (f64 max * pool.occupancy))
        
#if DEBUG
    let outputManagedStatus () =
        async {
            let leftQueue = queuedQuery.InputCount.ToString("00")
            let leftDelay = delayedQuery.Reader.Count.ToString("00")
            let usedConn = usedConn.Reader.Count.ToString("00")
            let rp = realPoolPressure().ToString("0.00")
            Console.WriteLine $"    队列{leftQueue} 延迟{leftDelay} | 待重用{usedConn} | 真实压力{rp}"
        }
        |> Async.Start
#endif

    do
        let rec loop conn =
            if realPoolPressure () < d
               && delayedQuery.Reader.Count > 0 then
                let q =
                    delayedQuery.Reader.ReadAsync().AsTask().Result

                q conn |> ignore
                loop conn
            else
                pool.recycleConnAsync conn |> ignore

        async {
            //TODO 在通道可用时持续处理
            //TODO 任务取消的异常处理
            while true do
                let conn =
                    usedConn.Reader.ReadAsync().AsTask().Result

                if realPoolPressure () < d
                   && delayedQuery.Reader.Count > 0 then
                    Task.Run(fun _ -> loop conn) |> ignore
                else
                    pool.recycleConnAsync conn |> ignore
        }
        |> Async.Start

    /// 以连接信息构造，并指定使用的数据库和连接池大小
    new(msg, database, poolSize: u32) = new PgSqlManaged(msg, database, 0.2, 0.7, u32 (f64 poolSize * 0.1), poolSize)
    /// 以连接信息构造，并指定连接池大小
    new(msg, poolSize) = new PgSqlManaged(msg, "", poolSize)
    /// 以连接信息构造，并指定使用的数据库
    new(msg, database) = new PgSqlManaged(msg, database, 100u)
    /// 以连接信息构造
    new(msg: DbConnMsg) = new PgSqlManaged(msg, "", 100u)

    member self.Dispose() =
        usedConn.Writer.Complete()
        delayedQuery.Writer.Complete()
        queuedQuery.Complete()
        
        self.forceLeftDelayedQuery ()
        
        self.forceLeftQueuedQuery ()
        queueSema.Dispose()
        pool.recycleConnAsync queueQueryConn
        |> asTask
        |> wait
       
        pool.Dispose()

    member self.mkCmd() = new NpgsqlCommand()

    member self.executeQuery(f: DbConnection -> 'r) : 'r =
        let conn =
            usedConn.Reader.TryRead()
            |> Option'.fromOkComma
            |> unwrapOr
            <| pool.fetchConn

        let r = f conn

        usedConn.Writer.WriteAsync conn |> ignore
#if DEBUG
        outputManagedStatus ()
#endif
        r

    member self.executeQueryAsync(f: DbConnection -> Task<'r>) =
        task {
            let! conn =
                usedConn.Reader.TryRead()
                |> Option'.fromOkComma
                |> ifCanUnwrapOr
                <| fun c -> task { return c }
                <| pool.fetchConnAsync

            let! r = f conn

            usedConn.Writer.WriteAsync conn |> ignore
#if DEBUG
            outputManagedStatus ()
#endif
            return r
        }

    member self.delayQuery f =
        delayedQuery.Writer.WriteAsync(fun c -> f c :> obj)
        |> ignore

    member self.forceLeftDelayedQuery() =
        delayedQuery.Reader.TryRead
        .> Option'.fromOkComma
        .> ifCanUnwrapOr
        <. fun q ->
            Option.Some(
                async {
                    fun c -> task { return q c }
                    |> self.executeQueryAsync
                    |> wait
                },
                ()
            )
        <. fun _ -> Option.None
        |> Seq.unfold
        <| ()
        |> fun s -> Async.Parallel(s, i32 max)
        |> Async.Ignore
        |> Async.RunSynchronously

    member self.queueQuery f =
        queuedQuery.Post(fun c -> f c :> obj) |> ignore
        queueSema.Release() |> ignore

    member self.forceLeftQueuedQuery() =
        //基准测试表明，使用自旋锁的开销要显著低于线程切换的开销
        //故此处使用自旋
        if queueSema.CurrentCount = 0 then
            ()
        else
            Thread.Yield() |> ignore
            self.forceLeftQueuedQuery ()

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
