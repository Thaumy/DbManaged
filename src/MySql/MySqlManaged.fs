namespace DbManaged.MySql

open System
open System.Data.Common
open System.Threading.Tasks
open System.Collections.Concurrent
open MySql.Data.MySqlClient
open fsharper.op
open fsharper.typ
open fsharper.op.Alias
open fsharper.op.Async
open DbManaged

/// MySql数据库管理器
type MySqlManaged private (msg, database, d, n, min, max) as managed =
    let pool =
        new DbConnPool(msg, database, (fun s -> new MySqlConnection(s)), d, n, min, max)

    let queuedQuery = ConcurrentQueue<DbConnection -> unit>()
    let delayedQuery = ConcurrentBag<DbConnection -> unit>()

    let getQueuedQuery () =
        queuedQuery.TryDequeue() |> Option'.fromOkComma

    let getDelayedQuery () =
        delayedQuery.TryTake() |> Option'.fromOkComma

    /// 对延迟查询集合复用包装后的回收连接
    /// 该函数会在如下情况时真正回收连接并返回：
    /// * 连接池压力大于销毁阈值时
    /// * 查询队列为空时
    let recycleConn conn =
        async {
            let rec loop () =
                //如果连接池压力小于阈值，调用回收后连接很有可能被销毁
                //为提高连接利用率，此时从延迟查询集合中取出查询任务复用该连接
                if pool.pressure < 0.1 then
                    getDelayedQuery () |> ifCanUnwrapOr
                    <| fun query ->
                        query conn
                        loop ()
                    <| (getQueuedQuery .> ifCanUnwrapOr
                        <. fun query ->
                            lock queuedQuery (fun _ -> query conn)
                            loop ()
                        <. fun _ -> pool.recycleConnAsync conn |> ignore)
                else
                    pool.recycleConnAsync conn |> ignore

            loop ()
        }
        |> Async.Start
        
    /// 以连接信息构造，并指定使用的数据库和连接池大小
    new(msg, database, poolSize: u32) = new MySqlManaged(msg, database, 0.1, 0.7, u32 (f64 poolSize * 0.1), poolSize)
    /// 以连接信息构造，并指定连接池大小
    new(msg, poolSize) = new MySqlManaged(msg, "", poolSize)
    /// 以连接信息构造，并指定使用的数据库
    new(msg, database) = new MySqlManaged(msg, database, 100u)
    /// 以连接信息构造
    new(msg: DbConnMsg) = new MySqlManaged(msg, "", 100u)

    member self.Dispose() =
        self.forceLeftQueuedQuery ()
        self.forceLeftDelayedQuery ()
        pool.Dispose()

    member self.mkCmd() = new MySqlCommand()

    member self.executeQuery f =
        let conn = pool.fetchConn ()
        let result = f conn
        recycleConn conn
        result

    member self.executeQueryAsync(f: DbConnection -> Task<'a>) =
        (fun _ -> self.executeQuery f |> result)
        |> Task.Run<_>

    (*
    //TODO MySql控制器的异步连接建立性能非常差劲，未能知晓其原因
    member self.executeQueryAsync f =
        task {
            let! connResult = pool.getConnectionAsync ()

            //TODO 有待优化
            let ret =
                connResult
                >>= fun conn ->
                        task {
                            let! closureRet = f conn
                            recycleConn conn
                            return closureRet |> Ok
                        }
                        |> result

            return ret
        }
*)
    member self.delayQuery f = f .> ignore |> delayedQuery.Add

    member self.forceLeftDelayedQuery() =
        getDelayedQuery .> ifCanUnwrapOr
        <. fun q -> Option.Some(async { self.executeQuery q |> ignore }, ())
        <. fun _ -> Option.None
        |> Seq.unfold
        <| ()
        |> fun s -> Async.Parallel(s, i32 max)
        |> Async.Ignore
        |> Async.RunSynchronously

    member self.queueQuery f = f .> ignore |> queuedQuery.Enqueue

    member self.forceLeftQueuedQuery() =
        getQueuedQuery .> ifCanUnwrapOr
        <. fun q -> Option.Some(async { managed.executeQuery q |> ignore }, ())
        <. fun _ -> Option.None
        |> Seq.unfold
        <| ()
        |> Async.Sequential
        |> Async.Ignore
        |> Async.RunSynchronously

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
