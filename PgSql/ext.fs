[<AutoOpen>]
module internal DbManaged.PgSql.ext

open System.Data.Common
open DbManaged
open Npgsql
open fsharper.op.Coerce
open fsharper.types

type NpgsqlConnection with

    /// 将 table 中 whereKey 等于 whereKeyVal 的行的 setKey 更新为 setKeyVal
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    member self.executeUpdate(table: string, (setKey: string, setKeyVal), (whereKey: string, whereKeyVal)) =
        (self :> DbConnection).useCommand
        <| fun cmd' ->
            let cmd: NpgsqlCommand = coerce cmd'

            cmd.CommandText <-
                $"UPDATE `{table}` \
                         SET `{setKey}`=?setKeyVal \
                       WHERE `{whereKey}`=?whereKeyVal"

            cmd.Parameters.AddWithValue("setKeyVal", setKeyVal)
            |> ignore

            cmd.Parameters.AddWithValue("whereKeyVal", whereKeyVal)
            |> ignore

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

            let x =
                pairs
                |> foldl
                    (fun (a, b) (k: string, v) ->

                        cmd.Parameters.AddWithValue(k, v) //添加参数
                        |> ignore

                        //a 为VALUES语句前半部分
                        //b 为VALUES语句后半部分
                        (a + $"`{k}`,", b + $"?{v} ,"))
                    ("", "")

            cmd.CommandText <-
                $"INSERT INTO `{table}` \
                      ({(fst x).[0..^1]}) \
                      VALUES \
                      ({(snd x).[0..^1]})"

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

    /// 删除 table 中 whereKey 等于 whereKeyVal 的行
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    member self.executeDelete (table: string) (whereKey: string, whereKeyVal) =
        self.useCommand
        <| fun cmd' ->
            let cmd: NpgsqlCommand = coerce cmd'
            
            cmd.CommandText <- $"DELETE FROM `{table}` WHERE `{whereKey}`=?Value"

            cmd.Parameters.AddWithValue("Value", whereKeyVal) //添加参数
            |> ignore

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
