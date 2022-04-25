[<AutoOpen>]
module internal DbManaged.ext.DbCommand

open System.Data.Common

type DbCommand with

    /// 创建一个 DbTransaction, 并以其为参数执行闭包 f
    /// DbTransaction 销毁权交由闭包 f
    member self.useTransaction f =
        let tx = self.Connection.BeginTransaction()
        self.Transaction <- tx
        f tx

    //TODO exp async api
    member self.useTransactionAsync f =
        task {
            let! tx = self.Connection.BeginTransactionAsync()
            self.Transaction <- tx
            return f tx
        }

    /// 托管一个 DbTransaction, 并以其为参数执行闭包 f
    /// 闭包执行完成后该 DbTransaction 会被销毁
    member self.hostTransaction f =
        self.useTransaction
        <| fun tx ->
            let result = f tx
            tx.Dispose()
            result

    //TODO exp async api
    member self.hostTransactionAsync f =
        task {
            return!
                self.useTransactionAsync
                <| fun tx ->
                    let result = f tx
                    tx.Dispose()
                    result
        }
