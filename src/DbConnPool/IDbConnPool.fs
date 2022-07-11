namespace DbManaged

open System
open System.Data.Common
open System.Threading.Tasks
open fsharper.op
open fsharper.typ
open fsharper.op.Alias

/// 数据库连接池
type internal IDbConnPool =

    inherit IDisposable

    /// 连接池的当前大小（连接数）
    abstract member size : u32

    /// 池压力系数
    /// 池瞬时压力越大，该系数越接近1，反之接近0
    abstract member pressure : f64

    /// 池占用率
    /// 池冗余量越大，该系数越接近1，反之接近0
    abstract member occupancy : f64

    /// 获取数据库连接
    abstract member fetchConn : unit -> DbConnection

    /// 异步获取数据库连接
    abstract member fetchConnAsync : unit -> Task<DbConnection>

    /// 异步回收数据库连接
    abstract member recycleConnAsync : DbConnection -> ValueTask

[<AutoOpen>]
module internal ext_IDbConnPool =

    type internal IDbConnPool with

        /// 创建一个数据库连接, 并以其为参数执行闭包 f
        /// 该连接的回收权交由闭包 f
        member pool.useConnection f = pool.fetchConn () |> f

        /// 托管一个数据库连接, 并以其为参数执行闭包 f
        /// 闭包执行完成后该连接会被自动回收
        member pool.hostConnection f =
            pool.useConnection
            <| fun conn ->
                let result = f conn
                pool.recycleConnAsync conn |> ignore
                result

        /// TODO exp async api
        /// 异步使用数据库连接
        member pool.useConnectionAsync f =
            task {
                let! conn = pool.fetchConnAsync ()
                return f conn
            }

        /// TODO exp async api
        /// 异步托管数据库连接
        member pool.hostConnectionAsync f =
            task {
                return!
                    pool.useConnectionAsync
                    <| fun conn ->
                        let result = f conn
                        pool.recycleConnAsync conn |> ignore
                        result
            }
