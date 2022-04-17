[<AutoOpen>]
module internal DbManaged.ext

open System.Data.Common

type DbCommand with

    /// 创建一个 MySqlTransaction, 并以其为参数执行闭包 f
    /// MySqlTransaction 销毁权交由闭包 f
    member self.useTransaction f =
        let tx = self.Connection.BeginTransaction()
        self.Transaction <- tx
        f tx


    /// 托管一个 MySqlTransaction, 并以其为参数执行闭包 f
    /// 闭包执行完成后该 MySqlTransaction 会被销毁
    member self.hostTransaction f =
        self.useTransaction
        <| fun tx ->
            let result = f tx
            tx.Dispose()
            result


type DbConnection with

    /// 创建一个 MySqlCommand, 并以其为参数执行闭包 f
    /// MySqlCommand 销毁权交由闭包 f
    member self.useCommand f =
        let cmd = self.CreateCommand()
        f cmd

    /// 托管一个 MySqlCommand, 并以其为参数执行闭包 f
    /// 闭包执行完成后该 MySqlCommand 会被销毁
    member self.hostCommand f =
        self.useCommand
        <| fun cmd ->
            let result = f cmd
            cmd.Dispose()
            result


type DbConnection with

    /// 执行任意查询
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    member self.execute sql =
        self.useCommand
        <| fun cmd ->
            cmd.CommandText <- sql

            cmd.useTransaction
            <| fun tx ->
                fun p ->
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

                    affected //实际受影响的行数

    /// 执行任意参数化查询
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    member self.execute(sql, para: #DbParameter array) =
        self.useCommand
        <| fun cmd ->
            cmd.CommandText <- sql
            cmd.Parameters.AddRange para //添加参数

            cmd.useTransaction
            <| fun tx ->
                fun p ->
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

                    affected //实际受影响的行数
