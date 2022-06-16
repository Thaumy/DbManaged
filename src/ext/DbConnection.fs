[<AutoOpen>]
module internal DbManaged.ext_DbConnection

open System.Data.Common

type DbConnection with

    member conn.CreateCommand(sql, paras) =
        let cmd = conn.CreateCommand()
        cmd.CommandText <- sql
        cmd.Parameters.AddRange paras
        cmd

    member conn.CreateCommand(sql) = conn.CreateCommand(sql, [||])

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

type internal DbConnection with

    /// 创建一个 DbTransaction, 并以其为参数执行闭包 f
    /// DbTransaction 需手动销毁
    member conn.useTransaction f =
        let tx = conn.BeginTransaction()
        f tx

    /// 托管一个 DbTransaction, 并以其为参数执行闭包 f
    /// 闭包执行完成后该 DbTransaction 会被销毁
    member conn.hostTransaction f =
        conn.useTransaction
        <| fun tx ->
            let result = f tx
            tx.Dispose()
            result

type internal DbConnection with

    //TODO exp async api
    member conn.useTransactionAsync f =
        task {
            let! tx = conn.BeginTransactionAsync()
            return f tx
        }

    //TODO exp async api
    member conn.hostTransactionAsync f =
        task {
            return!
                conn.useTransactionAsync
                <| fun tx ->
                    let result = f tx
                    tx.DisposeAsync() |> ignore
                    result
        }
