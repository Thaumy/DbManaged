module internal DbManaged.DbConnPool

open System.Data
open System.Data.Common
open Npgsql
open DbManaged
open fsharper.op
open fsharper.types

/// 数据库连接池
[<AbstractClass>]
type internal IDbConnPool() =

    /// 回收 DbConnection
    abstract member recycleConnection : DbConnection -> unit

    /// 取用 DbConnection
    abstract member getConnection : unit -> Result'<DbConnection, exn>

type internal IDbConnPool with

    /// 创建一个 DbConnection, 并以其为参数执行闭包 f
    /// DbConnection 销毁权交由闭包 f
    member self.useConnection f =
        self.getConnection () >>= fun conn -> f conn |> Ok

    /// 托管一个 DbConnection, 并以其为参数执行闭包 f
    /// 闭包执行完成后该 DbConnection 会被销毁
    member self.hostConnection f =
        self.useConnection
        <| fun conn ->
            let result = f conn
            self.recycleConnection conn
            result
