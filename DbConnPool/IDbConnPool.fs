namespace DbManaged

open System.Data.Common
open fsharper.op
open fsharper.types

/// 数据库连接池
type internal IDbConnPool =

    /// 获取数据库连接
    abstract member getConnection : unit -> Result'<DbConnection, exn>

    /// 回收数据库连接
    abstract member recycleConnection : DbConnection -> unit

[<AutoOpen>]
module internal ext_IDbConnPool =
    
    type internal IDbConnPool with

        /// 创建一个数据库连接, 并以其为参数执行闭包 f
        /// 该连接的回收权交由闭包 f
        member self.useConnection f =
            self.getConnection () >>= fun conn -> f conn |> Ok

        /// 托管一个数据库连接, 并以其为参数执行闭包 f
        /// 闭包执行完成后该连接会被自动回收
        member self.hostConnection f =
            self.useConnection
            <| fun conn ->
                let result = f conn
                self.recycleConnection conn
                result
