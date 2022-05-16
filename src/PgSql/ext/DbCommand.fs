[<AutoOpen>]
module DbManaged.PgSql.ext_DbCommand

open System.Data.Common
open fsharper.typ
open DbManaged
let private paraMark= ":"

type DbCommand with

    /// 将 table 中 whereKey 等于 whereKeyVal 的行的 setKey 更新为 setKeyVal
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    member cmd.update(table: string, (setKey: string, setVal: 's), (whereKey: string, whereVal: 'w)) =
        let sql =
            $"UPDATE {table} \
              SET    {setKey}   = {paraMark}setVal \
              WHERE  {whereKey} = {paraMark}whereVal"

        cmd.letQuery(sql).addParas(
            [ ("setVal", setVal :> obj)
              ("whereVal", whereVal :> obj) ]
        )
            .commitWhen
    /// 将 table 中 key 等于 oldValue 的行的 key 更新为 newValue
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    member cmd.update(table, key, newValue: 'v, oldValue: 'v) =
        (table, (key, newValue), (key, oldValue))
        |> cmd.update


    /// 在 table 中插入一行
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    member cmd.insert (table: string) pairs =

        let keys, values =
            pairs
            |> foldl
                (fun (acc_k, acc_v) (k: string, v) ->

                    cmd.addPara (k, v) //添加参数
                    |> ignore

                    //acc_k 为VALUES语句前半部分
                    //acc_v 为VALUES语句后半部分
                    ($"{acc_k}{k},", $"{acc_v}{paraMark}{k},"))
                ("", "")

        let sql =
            $"INSERT INTO {table} \
                     ({keys.withoutLast}) \
                     VALUES \
                     ({values.withoutLast})"

        cmd.letQuery(sql).commitWhen


    /// 删除 table 中 whereKey 等于 whereKeyVal 的行
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    member cmd.delete (table: string) (whereKey: string, whereVal) =
        let sql =
            $"DELETE FROM {table} WHERE {whereKey} = {paraMark}whereVal"

        cmd.letQuery(sql).addPara(
            "whereVal",
            whereVal
        )
            .commitWhen


type DbCommand with

    /// 参数化查询到第一个值
    member cmd.getFstVal(table: string, targetKey: string, (whereKey: string, whereVal: 'v)) =
        cmd
            .letQuery(
                $"SELECT {targetKey} FROM {table} WHERE {whereKey} = {paraMark}whereVal"
            )
            .addPara(
            "whereVal",
            whereVal
        )
            .commitForValue
