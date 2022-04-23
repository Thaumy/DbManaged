namespace DbManaged.DbConnPoolAsync

open DbManaged.DbConnPool
open System.Data.Common
open System.Threading.Tasks
open fsharper.op
open fsharper.types

/// 数据库连接池
type internal IDbConnPoolAsync =
    inherit IDbConnPool
    
    /// TODO exp async api
    /// 异步获取数据库连接
    abstract member getConnectionAsync : unit -> Task<Result'<DbConnection, exn>>

    /// TODO exp async api
    /// 异步回收数据库连接
    abstract member recycleConnectionAsync : DbConnection -> Task<unit>

[<AutoOpen>]
module internal ext =

    type internal IDbConnPoolAsync with

        /// TODO exp async api
        /// 异步使用数据库连接
        member self.useConnectionAsync f =
            task {
                let! conn = self.getConnectionAsync ()
                return conn >>= fun conn -> f conn |> Ok
            }

        /// TODO exp async api
        /// 异步托管数据库连接
        member self.hostConnectionAsync f =
            task {
                return!
                    self.useConnectionAsync
                    <| fun conn ->
                        let result = f conn
                        self.recycleConnectionAsync conn |> ignore
                        result
            }
