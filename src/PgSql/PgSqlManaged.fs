namespace DbManaged.PgSql

open System
open System.Data.Common
open System.Collections.Concurrent
open Npgsql
open fsharper.op
open fsharper.typ
open fsharper.op.Async
open DbManaged
open DbManaged.AnySql

/// PgSql数据库管理器
type PgSqlManaged private (pool: IDbConnPool) =

    let queuedQuery = ConcurrentQueue<DbConnection -> unit>()

    /// 以连接信息构造
    new(msg) =
        let pool =
            new DbConnPool<NpgsqlConnection>(msg, 32u)

        new PgSqlManaged(pool)
    /// 以连接信息构造，并指定使用的数据库
    new(msg, database) =
        let pool =
            new DbConnPool<NpgsqlConnection>(msg, database, 32u)

        new PgSqlManaged(pool)
    /// 以连接信息构造，并指定使用的数据库和连接池大小
    new(msg, database, poolSize) =
        let pool =
            new DbConnPool<NpgsqlConnection>(msg, database, poolSize)

        new PgSqlManaged(pool)

    interface IDisposable with
        member self.Dispose() =
            (self :> IDbManaged).forceLeftQueuedQuery ()
            pool.Dispose()

    interface IDbManaged with

        member self.makeCmd() = new NpgsqlCommand()

        member self.executeQuery f =
            pool.getConnection ()
            >>= fun conn ->
                    let result = f conn
                    pool.recycleConnection conn
                    result |> Ok

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

        member self.queueQuery f = f .> ignore |> queuedQuery.Enqueue

        member self.forceLeftQueuedQuery() =

            let rec loop () =
                match queuedQuery.TryDequeue() with
                | true, q ->
                    (self :> IDbManaged).executeQuery q |> ignore
                    loop ()
                | _ -> ()

            loop ()
