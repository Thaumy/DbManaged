[<AutoOpen>]
module internal DbManaged.ext

open System.Threading.Tasks
open System.Data.Common
open fsharper.op.Lazy

type DbCommand with

    /// 创建一个 DbTransaction, 并以其为参数执行闭包 f
    /// DbTransaction 销毁权交由闭包 f
    member self.useTransaction f =
        let tx = self.Connection.BeginTransaction()
        self.Transaction <- tx
        f tx

    //TODO exp async api
    member self.useTransactionAsync f = task { return self.useTransaction f }

    /// 托管一个 DbTransaction, 并以其为参数执行闭包 f
    /// 闭包执行完成后该 DbTransaction 会被销毁
    member self.hostTransaction f =
        self.useTransaction
        <| fun tx ->
            let result = f tx
            tx.Dispose()
            result

    //TODO exp async api
    member self.hostTransactionAsync f = task { return self.hostTransaction f }


type DbConnection with

    /// 创建一个 DbCommand, 并以其为参数执行闭包 f
    /// DbCommand 销毁权交由闭包 f
    member self.useCommand f =
        let cmd = self.CreateCommand()
        f cmd

    //TODO exp async api
    member self.useCommandAsync f = task { return self.useCommand f }

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

    //TODO 实验性异步API
    member self.executeAnyAsync sql =
        let result =
            self.useCommandAsync
            <| fun cmd ->
                cmd.CommandText <- sql

                cmd.useTransactionAsync
                <| fun tx ->
                    fun callback p ->
                        task {
                            let affected =
                                match cmd.ExecuteNonQueryAsync().Result with
                                | n when p n -> //符合期望影响行数规则则提交
                                    tx.CommitAsync().Wait()
                                    n
                                | _ -> //否则回滚
                                    tx.RollbackAsync().Wait()
                                    0

                            tx.DisposeAsync().AsTask().Start() //资源释放
                            cmd.DisposeAsync().AsTask().Start()

                            force callback //执行回调（可用于连接销毁）

                            return affected //实际受影响的行数
                        }

        result.Result.Result


    /// 执行任意查询
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    /// 低级操作：查询执行完成后应注意注销该连接以避免连接泄漏
    member self.executeAny sql =
        self.useCommand
        <| fun cmd ->
            cmd.CommandText <- sql

            cmd.useTransaction
            <| fun tx ->
                fun callback p ->
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
            <| fun tx ->
                fun callback p ->
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
