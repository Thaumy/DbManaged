namespace DbManaged.MySql

open System
open System.Data.Common
open System.Threading.Tasks
open System.Collections.Concurrent
open MySql.Data.MySqlClient
open fsharper.op
open fsharper.typ
open fsharper.op.Async
open DbManaged
open DbManaged.AnySql

/// PgSql数据库管理器
type MySqlManaged private (pool: IDbConnPool) =

    let queuedQuery = ConcurrentQueue<DbConnection -> unit>()

    /// 以连接信息构造
    new(msg) =
        let pool =
            new DbConnPool<MySqlConnection>(msg, 32u, 0.3, 0.8)

        new MySqlManaged(pool)
    /// 以连接信息构造，并指定使用的数据库
    new(msg, database) =
        let pool =
            new DbConnPool<MySqlConnection>(msg, database, 0.3, 0.8)

        new MySqlManaged(pool)
    /// 以连接信息构造，并指定使用的数据库和连接池大小
    new(msg, database, poolSize) =
        let pool =
            new DbConnPool<MySqlConnection>(msg, database, poolSize, 0, 0.92)

        new MySqlManaged(pool)

    interface IDisposable with
        member self.Dispose() =
            (self :> IDbManaged).forceLeftQueuedQuery ()
            pool.Dispose()

    interface IDbManaged with

        member self.makeCmd() = new MySqlCommand()

        member self.executeQuery f =
            pool.getConnection ()
            >>= fun conn ->
                    let result = f conn
                    pool.recycleConnection conn
                    result |> Ok

        member self.executeQueryAsync f =
            (fun _ ->
                let ret = (self :> IDbManaged).executeQuery f

                ret |> unwrap |> result |> Ok)
            |> Task.Run<Result'<'b, exn>>
            
        //TODO MySql控制器的异步连接建立性能非常差劲，我仍未能知晓其原因
        (*
        member self.executeQueryAsync f =
            task {
                let! connResult = pool.getConnectionAsync ()

                //TODO 有待优化
                let ret =
                    connResult
                    >>= fun conn ->
                            task {
                                let! closureRet = f conn
                                pool.recycleConnectionAsync conn |> ignore
                                return closureRet |> Ok
                            }
                            |> result

                return ret
            }
        *)

        member self.queueQuery f = f .> ignore |> queuedQuery.Enqueue

        member self.forceLeftQueuedQuery() =

            let rec loop () =
                match queuedQuery.TryDequeue() with
                | true, q ->
                    (self :> IDbManaged).executeQuery q |> ignore
                    loop ()
                | _ -> ()

            loop ()
