namespace DbManaged.PgSql

open System
open System.Threading
open System.Data.Common
open System.Threading.Tasks
open System.Threading.Channels
open System.Collections.Concurrent
open System.Text.RegularExpressions
open System.Threading.Tasks.Dataflow
open fsharper.op
open fsharper.typ
open fsharper.alias
open fsharper.op.Async
open pilipala.util.id
open Npgsql
open DbManaged

/// PgSql数据库管理器
type PgSqlManaged private (msg, d, n, min, max) as managed =
    let pool =
        new DbConnPool(msg.host, msg.port, msg.usr, msg.pwd, msg.db, (fun s -> new NpgsqlConnection(s)), d, n, min, max)

    let queueSema = new SemaphoreSlim(0) //用于队列查询任务的完成精确计数

    let queueQueryConn =
        pool.fetchConnAsync().Result

    let queuedQuery =
        fun q ->
            q queueQueryConn
            queueSema.Wait()
        |> ActionBlock<DbConnection -> unit>

    let palaflake =
        palaflake.Generator(0uy, u16 DateTime.Now.Year)

    let queryResult =
        ConcurrentDictionary<i64, obj>()

    let delayedQuery =
        Channel.CreateUnbounded<DbConnection -> unit>()

    let usedConn =
        Channel.CreateUnbounded<DbConnection>() //复用池

    let realPoolPressure () =
        pool.pressure
        - (f64 usedConn.Reader.Count
           / (f64 max * pool.occupancy))

#if Test
    let outputManagedStatus () =
        async {
            let leftQueue =
                queuedQuery.InputCount.ToString("00")

            let leftDelay =
                delayedQuery.Reader.Count.ToString("00")

            let usedConn =
                usedConn.Reader.Count.ToString("00")

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

                q conn

                loop conn
            else
                pool.recycleConnAsync conn |> ignore

        //用于处理延迟查询
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

    new(msg: DbConnMsg, pooling: u16) = new PgSqlManaged(msg, 0.2, 0.7, u32 (f64 pooling * 0.1), u32 pooling)

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

    member self.makeCmd() = new NpgsqlCommand()

    member self.executeQuery(f: DbConnection -> 'r) : 'r =
        let conn =
            usedConn.Reader.TryRead()
            |> Option'.fromOkComma
            |> unwrapOrEval
            <| pool.fetchConn

        let r = f conn

        usedConn.Writer.WriteAsync conn |> ignore
#if Test
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
#if Test
            outputManagedStatus ()
#endif
            return r
        }

    member self.delayQuery(f: DbConnection -> 'r) : Task<'r> =
        let qId = palaflake.Next()
        let sema = new SemaphoreSlim(0)

        fun c ->
            let result = f c

            queryResult.TryAdd(qId, result) |> mustTrue

            sema.Release() |> ignore
        |> delayedQuery.Writer.WriteAsync
        |> ignore

        fun () ->
            let ok, r = queryResult.TryGetValue qId
            mustTrue ok
            sema.Dispose()
            queryResult.TryRemove qId |> fst |> mustTrue
            coerce r
        |> sema.WaitAsync().Then

    member self.forceLeftDelayedQuery() =
        delayedQuery.Reader.TryRead
        .> Option'.fromOkComma
        .> ifCanUnwrapOr
        |> flip //infix
        <| fun q ->
            Option.Some(
                async {
                    fun c -> task { return q c }
                    |> self.executeQueryAsync
                    |> wait
                },
                ()
            )
        |> flip //infix
        <| fun _ -> Option.None
        |> Seq.unfold
        <| ()
        |> fun s -> Async.Parallel(s, i32 max)
        |> Async.Ignore
        |> Async.RunSynchronously

    member self.queueQuery(f: DbConnection -> 'r) : Task<'r> =
        let qId = palaflake.Next()
        let sema = new SemaphoreSlim(0)

        fun c ->
            let result = f c

            queryResult.TryAdd(qId, result) |> mustTrue

            sema.Release() |> ignore
        |> queuedQuery.Post
        |> ignore

        queueSema.Release() |> ignore

        fun () ->
            let ok, r = queryResult.TryGetValue qId
            mustTrue ok
            sema.Dispose()
            coerce r
        |> sema.WaitAsync().Then

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

        member i.makeCmd() = managed.makeCmd ()

        member i.executeQuery f = managed.executeQuery f

        member i.executeQueryAsync f = managed.executeQueryAsync f

        member i.delayQuery f = managed.delayQuery f

        member i.forceLeftDelayedQuery() = managed.forceLeftDelayedQuery ()

        member i.queueQuery f = managed.queueQuery f

        member i.forceLeftQueuedQuery() = managed.forceLeftQueuedQuery ()

        member self.normalizeSql sql =
            let mark = ":"
            Regex.Replace(sql, "<([0-9a-zA-Z_]*)>", $"{mark}$1")
