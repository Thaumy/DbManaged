namespace DbManaged.MySql

open System
open System.Data
open System.Text
open System.Threading
open System.Threading.Tasks
open System.Data.Common
open System.Threading.Tasks
open MySql.Data.MySqlClient
open fsharper.op
open fsharper.typ
open fsharper.op.Alias
open fsharper.op.Coerce
open DbManaged
open DbManaged.ext
open DbManaged.AnySql
open DbManaged.MySql.ext
open System
open System.Collections.Concurrent
open System.Data
open System.Threading
open System.Data.Common
open System.Threading.Tasks
open DbManaged.AnySql
open Npgsql
open fsharper.op
open fsharper.typ
open fsharper.op.Async
open fsharper.op.Alias
open DbManaged
open DbManaged.ext
open DbManaged.PgSql.ext


/// PgSql数据库管理器
type MySqlManaged private (pool: IDbConnPool) =

    let queuedQuery = ConcurrentQueue<DbConnection -> unit>()

    /// 以连接信息构造
    new(msg) =
        let pool =
            new DbConnPool<MySqlConnection>(msg, 32u)

        new MySqlManaged(pool)
    /// 以连接信息构造，并指定使用的数据库
    new(msg, database) =
        let pool =
            new DbConnPool<MySqlConnection>(msg, database, 32u)

        new MySqlManaged(pool)
    /// 以连接信息构造，并指定使用的数据库和连接池大小
    new(msg, database, poolSize) =
        let pool =
            new DbConnPool<MySqlConnection>(msg, database, poolSize)

        new MySqlManaged(pool)

    interface IDisposable with
        member self.Dispose() =
            (self :> IDbManaged).forceLeftQueuedQuery ()
            pool.Dispose()

    interface IDbManaged with

        member self.makeCmd() = new MySqlCommand()

        member self.executeQuery f =
            pool.getConnection () >>= fun conn -> f conn |> Ok

        member self.executeQueryAsync f =
            task {
                let! result = pool.getConnectionAsync ()
                return result >>= fun conn -> f conn |> Ok
            }
            |> result

        member self.queueQuery f = f .> ignore |> queuedQuery.Enqueue

        member self.forceLeftQueuedQuery() =

            let rec loop () =
                match queuedQuery.TryDequeue() with
                | true, q ->
                    (self :> IDbManaged).executeQuery q |> ignore
                    loop ()
                | _ -> ()

            loop ()
