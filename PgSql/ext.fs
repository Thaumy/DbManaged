[<AutoOpen>]
module internal DbManaged.PgSql.ext

open System.Data.Common
open DbManaged
open Npgsql
open fsharper.op.Coerce
open fsharper.op.Lazy
open fsharper.types

type NpgsqlConnection with

    /// 将 table 中 whereKey 等于 whereKeyVal 的行的 setKey 更新为 setKeyVal
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    member self.executeUpdate(table: string, (setKey: string, setKeyVal), (whereKey: string, whereKeyVal)) =
        (self :> DbConnection).useCommand
        <| fun cmd' ->
            let cmd: NpgsqlCommand = coerce cmd'

            cmd.CommandText <-
                $"UPDATE {table} \
                         SET {setKey}=:setKeyVal \
                       WHERE {whereKey}=:whereKeyVal"

            [| NpgsqlParameter("setKeyVal", setKeyVal :> obj)
               NpgsqlParameter("whereKeyVal", whereKeyVal :> obj) |]
            |> cmd.Parameters.AddRange

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

    /// 将 table 中 key 等于 oldValue 的行的 key 更新为 newValue
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    member self.executeUpdate(table, key, newValue: 'V, oldValue: 'V) =
        (table, (key, newValue), (key, oldValue))
        |> self.executeUpdate

    /// 在 table 中插入一行
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    member self.executeInsert (table: string) pairs =
        self.useCommand
        <| fun cmd' ->
            let cmd: NpgsqlCommand = coerce cmd'

            let keys, values =
                pairs
                |> foldl
                    (fun (acc_k, acc_v) (k: string, v) ->

                        cmd.Parameters.AddWithValue(k, v :> obj) //添加参数
                        |> ignore

                        //acc_k 为VALUES语句前半部分
                        //acc_v 为VALUES语句后半部分
                        ($"{acc_k}{k},", $"{acc_v}:{k},"))
                    ("", "")

            cmd.CommandText <-
                $"INSERT INTO {table} \
                      ({keys.[0..^1]}) \
                      VALUES \
                      ({values.[0..^1]})"

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

    /// 删除 table 中 whereKey 等于 whereKeyVal 的行
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    member self.executeDelete (table: string) (whereKey: string, whereKeyVal) =
        self.useCommand
        <| fun cmd' ->
            let cmd: NpgsqlCommand = coerce cmd'

            cmd.CommandText <- $"DELETE FROM {table} WHERE {whereKey}=:Value"

            cmd.Parameters.AddWithValue("Value", whereKeyVal) //添加参数
            |> ignore

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
