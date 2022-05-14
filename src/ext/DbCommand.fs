[<AutoOpen>]
module internal DbManaged.ext.DbCommand

open System.Data.Common
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

type DbCommand with

    member cmd.CreateParameter(k, v) =
        let p = cmd.CreateParameter()
        p.ParameterName <- k
        p.Value <- v
        p

type DbConnection with

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

type DbConnection with

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

type internal DbCommand with
    member cmd.letQuery sql =
        cmd.CommandText <- sql
        cmd

    member cmd.addPara(para: DbParameter) =
        cmd.Parameters.Add para |> ignore
        cmd

    member cmd.addParas(paras: DbParameter array) =
        cmd.Parameters.AddRange paras
        cmd

    member cmd.addPara(name: string, value: 't) =
        cmd.CreateParameter(name, value) |> cmd.addPara

    member cmd.addParas(paras: (string * 't) list) =
        paras
        |> foldMap (fun (k: string, v) -> List' [ cmd.CreateParameter(k, v) ])
        |> unwrap
        |> List.toArray
        |> cmd.addParas

    member cmd.useConn conn =
        cmd.Connection <- conn
        cmd

type internal DbCommand with
    member cmd.commit conn =
        //如果结果集为空，ExecuteScalar返回null
        cmd.useConn(conn).ExecuteNonQuery()

    member cmd.commitForValue conn = cmd.useConn(conn).ExecuteScalar()

    member cmd.commitForTable conn =
        cmd.useConn(conn).ExecuteReader().GetSchemaTable()

    member cmd.commitWhen p =
        fun callback conn ->
            (conn: DbConnection).useTransaction
            <| fun tx ->
                let affected =
                    match cmd.useConn(conn).ExecuteNonQuery() with
                    | n when p n -> //符合期望影响行数规则则提交
                        tx.Commit()
                        n
                    | _ -> //否则回滚
                        tx.Rollback()
                        0

                tx.Dispose() //资源释放
                cmd.Dispose()
                cmd.Transaction <- null

                callback () //执行回调（可用于连接销毁）

                affected

type internal DbCommand with
    member cmd.commitAsync conn =
        cmd.useConn(conn).ExecuteNonQueryAsync()

    member cmd.commitForValueAsync conn = cmd.useConn(conn).ExecuteScalarAsync()

    member cmd.commitForTableAsync conn =
        task {
            let! reader = cmd.useConn(conn).ExecuteReaderAsync()
            return! reader.GetSchemaTableAsync()
        }

    member cmd.commitWhenAsync p =
        fun callback conn ->
            (conn: DbConnection).useTransactionAsync
            <| fun tx ->
                task {
                    //TODO 有待重构
                    let! n = cmd.useConn(conn).ExecuteNonQueryAsync() //耗时操作

                    let affected =
                        if p n then
                            tx.CommitAsync() |> wait |> ignore //符合期望影响行数规则则提交
                            n
                        else
                            tx.RollbackAsync() |> wait |> ignore //否则回滚
                            0

                    tx.DisposeAsync() |> ignore //资源释放
                    cmd.DisposeAsync() |> ignore
                    cmd.Transaction <- null

                    callback () //执行回调（可用于连接销毁）

                    return affected
                }
            |> result


type DbCommand with

    /// TODO exp async api
    member cmd.queryAsync sql = cmd.letQuery(sql).commitWhenAsync
    /// TODO exp async api
    member cmd.queryAsync(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitWhenAsync

type DbCommand with

    /// 查询到表
    member cmd.select(sql) = cmd.letQuery(sql).commitForTable
    /// 参数化查询到表
    member cmd.select(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitForTable


    /// 执行任意查询
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    /// 低级操作：查询执行完成后应注意注销该连接以避免连接泄漏
    member cmd.query sql = cmd.letQuery(sql).commitWhen


    /// 执行任意参数化查询
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    /// 低级操作：查询执行完成后应注意注销该连接以避免连接泄漏
    member cmd.query(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitWhen


    /// 将 table 中 whereKey 等于 whereKeyVal 的行的 setKey 更新为 setKeyVal
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    member cmd.update(paraMark, table: string, (setKey: string, setKeyVal), (whereKey: string, whereKeyVal)) =
        let sql =
            $"UPDATE {table} \
              SET    {setKey}   = {paraMark}setKeyVal \
              WHERE  {whereKey} = {paraMark}whereKeyVal"

        cmd.letQuery(sql).addParas(
            [ ("setKeyVal", setKeyVal); ("whereKeyVal", whereKeyVal) ]
        )
            .commitWhen
    /// 将 table 中 key 等于 oldValue 的行的 key 更新为 newValue
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    member cmd.update(paraMark, table, key, newValue: 'v, oldValue: 'v) =
        (paraMark, table, (key, newValue), (key, oldValue))
        |> cmd.update


    /// 在 table 中插入一行
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    member cmd.insert paraMark (table: string) pairs =

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
    member cmd.delete paraMark (table: string) (whereKey: string, whereKeyVal) =
        let sql =
            $"DELETE FROM {table} WHERE {whereKey} = {paraMark}Value"

        cmd.letQuery(sql).addPara(
            "Value",
            whereKeyVal
        )
            .commitWhen


type DbCommand with

    /// 查询到第一个值
    member cmd.getFstVal sql =
        cmd.letQuery(sql).commitForValue
        .> Option'<obj>.fromNullable
    /// 参数化查询到第一个值
    member cmd.getFstVal(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitForValue
        .> Option'<obj>.fromNullable
    /// 参数化查询到第一个值
    member cmd.getFstVal(table: string, targetKey: string, (whereKey: string, whereKeyVal: 'v)) =
        cmd
            .letQuery(
                $"SELECT {targetKey} FROM {table} WHERE {whereKey} = :whereKeyVal"
            )
            .addPara(
            "whereKeyVal",
            whereKeyVal
        )
            .commitForValue
        .> Option'<obj>.fromNullable
    /// 查询到第一行
    member cmd.getFstRow sql =
        cmd.select sql
        .> fun result ->
            match result.Rows with
            //仅当行数非零时有结果
            | rows when rows.Count <> 0 -> Some rows.[0]
            | _ -> None
            |> Ok
    /// 参数化查询到第一行
    member cmd.getFstRow(sql, paras) =
        cmd.select (sql, paras)
        .> fun result ->
            match result.Rows with
            //仅当行数非零时有结果
            | rows when rows.Count <> 0 -> Some rows.[0]
            | _ -> None
            |> Ok


    /// 查询到指定列
    member cmd.getCol(sql, key: string) =
        cmd.select sql
        .> fun t -> Ok <| getColFromByKey (t, key)
    /// 参数化查询到指定列
    member cmd.getCol(sql, key: string, paras) =
        cmd.select (sql, paras)
        .> fun t -> getColFromByKey (t, key) |> Ok
    /// 查询到指定列
    member cmd.getCol(sql, index: u32) =
        cmd.select sql
        .> fun t -> Ok <| getColFromByIndex (t, index)
    /// 参数化查询到指定列
    member cmd.getCol(sql, index: u32, paras) =
        cmd.select (sql, paras)
        .> fun t -> getColFromByIndex (t, index) |> Ok
