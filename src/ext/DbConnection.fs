[<AutoOpen>]
module internal DbManaged.ext.DbConnection

open System.Threading.Tasks
open DbManaged.ext
open System.Data.Common
open fsharper.op.Async
open fsharper.op.Lazy

type DbConnection with

    /// 创建一个 DbCommand, 并以其为参数执行闭包 f
    /// DbCommand 销毁权交由闭包 f
    member self.useCommand f =
        let cmd = self.CreateCommand()
        f cmd

    //TODO exp async api
    member self.useCommandAsync f =
        task {
            let! cmd = Task.Run self.CreateCommand
            return f cmd
        }

    /// 托管一个 DbCommand, 并以其为参数执行闭包 f
    /// 闭包执行完成后该 DbCommand 会被销毁
    member self.hostCommand f =
        self.useCommand
        <| fun cmd ->
            let result = f cmd
            cmd.Dispose()
            result

    //TODO exp async api
    member self.hostCommandAsync f = task { return self.hostCommand f }

type DbConnection with

    /// 执行任意查询
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    /// 低级操作：查询执行完成后应注意注销该连接以避免连接泄漏
    member self.executeAny sql =
        self.useCommand
        <| fun cmd ->
            cmd.CommandText <- sql

            cmd.useTransaction
            <| fun tx callback p ->
                let affected =
                    match cmd.ExecuteNonQuery() with
                    | n when p n -> //符合期望影响行数规则则提交
                        tx.Commit()
                        n
                    | _ -> //否则回滚
                        tx.Rollback()
                        0

                tx.Dispose() //资源释放
                cmd.Dispose()

                force callback //执行回调（可用于连接销毁）

                affected //实际受影响的行数

    /// 执行任意参数化查询
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    /// 低级操作：查询执行完成后应注意注销该连接以避免连接泄漏
    member self.executeAny(sql, para: #DbParameter array) =
        self.useCommand
        <| fun cmd ->
            cmd.CommandText <- sql
            cmd.Parameters.AddRange para //添加参数

            cmd.useTransaction
            <| fun tx callback p ->
                let affected =
                    match cmd.ExecuteNonQuery() with
                    | n when p n -> //符合期望影响行数规则则提交
                        tx.Commit()
                        n
                    | _ -> //否则回滚
                        tx.Rollback()
                        0

                tx.Dispose() //资源释放
                cmd.Dispose()

                force callback //执行回调（可用于连接销毁）

                affected //实际受影响的行数

type DbConnection with
    /// TODO exp async api
    member self.executeAnyAsync sql =

        self.useCommandAsync
        <| fun cmd ->
            cmd.CommandText <- sql

            cmd.useTransactionAsync
            <| fun tx callback p ->
                task {
                    let! n = cmd.ExecuteNonQueryAsync() //耗时操作

                    let affected =
                        if p n then
                            tx.CommitAsync() |> wait |> ignore //符合期望影响行数规则则提交
                            n
                        else
                            tx.RollbackAsync() |> wait |> ignore //否则回滚
                            0

                    tx.DisposeAsync() |> ignore //资源释放
                    cmd.DisposeAsync() |> ignore

                    force callback //执行回调（可用于连接销毁）

                    return affected
                }
        |> result
        |> result

    /// TODO exp async api
    member self.executeAnyAsync(sql, para: #DbParameter array) =

        self.useCommandAsync
        <| fun cmd ->
            cmd.CommandText <- sql
            cmd.Parameters.AddRange para //添加参数

            cmd.useTransactionAsync
            <| fun tx callback p ->
                task {
                    let! n = cmd.ExecuteNonQueryAsync() //耗时操作

                    let affected =
                        if p n then
                            tx.CommitAsync() |> wait |> ignore //符合期望影响行数规则则提交
                            n
                        else
                            tx.RollbackAsync() |> wait |> ignore //否则回滚
                            0

                    tx.DisposeAsync() |> ignore //资源释放
                    cmd.DisposeAsync() |> ignore

                    force callback //执行回调（可用于连接销毁）

                    return affected //实际受影响的行数

                }
        |> result
        |> result