module internal DbManaged.DbConnPool

open System.Data
open System.Data.Common
open System.Threading.Tasks
open Npgsql
open DbManaged
open fsharper.op
open fsharper.types

/// 数据库连接池
[<AbstractClass>]
type internal IDbConnPool() =

    /// 取用 DbConnection
    abstract member getConnection : unit -> Result'<DbConnection, exn>

    /// TODO exp async api
    /// 异步取用 DbConnection
    abstract member getConnectionAsync : unit -> Task<Result'<DbConnection, exn>>

    /// 回收 DbConnection
    abstract member recycleConnection : DbConnection -> unit

    /// TODO exp async api
    /// 异步回收 DbConnection
    abstract member recycleConnectionAsync : DbConnection -> Task<unit>

type internal IDbConnPool with

    /// 创建一个 DbConnection, 并以其为参数执行闭包 f
    /// DbConnection 销毁权交由闭包 f
    member self.useConnection f =
        self.getConnection () >>= fun conn -> f conn |> Ok

    /// TODO exp async api
    member self.useConnectionAsync f =
        task {
            let! conn = self.getConnectionAsync ()
            return conn >>= fun conn -> f conn |> Ok
        }


    /// 托管一个 DbConnection, 并以其为参数执行闭包 f
    /// 闭包执行完成后该 DbConnection 会被销毁
    member self.hostConnection f =
        self.useConnection
        <| fun conn ->
            let result = f conn
            self.recycleConnection conn
            result

    /// TODO exp async api
    member self.hostConnectionAsync f =
        task {
            return!
                self.useConnectionAsync
                <| fun conn ->
                    let result = f conn
                    self.recycleConnection conn
                    result
        }
