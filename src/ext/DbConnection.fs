[<AutoOpen>]
module internal DbManaged.ext.DbConnection

open System.Threading.Tasks
open DbManaged.ext
open System.Data.Common
open fsharper.op.Async
open fsharper.op.Lazy
open System.Data.Common
open DbManaged.ext
open System
open System.Data
open System.Threading
open System.Data.Common
open System.Threading.Tasks
open Npgsql
open fsharper.op
open fsharper.typ
open fsharper.op.Alias
open fsharper.op.Async
open DbManaged
open DbManaged.ext


type DbConnection with

    member conn.CreateCommand(sql) =
        let cmd = conn.CreateCommand()
        cmd.CommandText <- sql
        cmd

    member conn.CreateCommand(sql, paras) =
        let cmd = conn.CreateCommand()
        cmd.CommandText <- sql
        cmd.Parameters.AddRange paras
        cmd

type DbConnection with

    /// 生成一个 DbCommand, 并以其为参数执行闭包 f
    /// DbCommand 需要手动销毁
    member conn.useCommand f =
        let cmd = conn.CreateCommand()
        f cmd
    /// 托管一个 DbCommand, 并以其为参数执行闭包 f
    /// 闭包执行完成后该 DbCommand 会被销毁
    member conn.hostCommand f =
        let cmd = conn.CreateCommand()
        let result = f cmd
        cmd.Dispose()
        result

