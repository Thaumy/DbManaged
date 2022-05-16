[<AutoOpen>]
module DbManaged.ext_DbCommand

open System
open System.Data
open System.Data.Common
open System.Threading.Tasks
open fsharper.op
open fsharper.typ
open fsharper.op.Alias
open fsharper.op.Async

type internal DbCommand with

    member cmd.CreateParameter(k, v) =
        let p = cmd.CreateParameter()
        p.ParameterName <- k
        p.Value <- v
        p

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

    member cmd.addParas(paras: (string * #obj) list) =
        paras
        |> foldMap (fun (k: string, v) -> List' [ cmd.CreateParameter(k, v) ])
        |> unwrap
        |> List.toArray
        |> cmd.addParas

    member cmd.useConn conn =
        cmd.Connection <- conn
        cmd

    member cmd.useTx tx =
        cmd.Transaction <- tx
        cmd

type internal DbCommand with

    //提交并返回受影响的行数
    member cmd.commitForAffected conn =
        //如果结果集为空，ExecuteScalar返回null
        cmd.useConn(conn).ExecuteNonQuery()

    //提交并取得第一行第一列的值
    member cmd.commitForValue conn =
        cmd.useConn(conn).ExecuteScalar()
        |> Option'<obj>.fromNullable

    //提交并取得一个读取器
    member cmd.commitForReader conn = cmd.useConn(conn).ExecuteReader()

    //提交并取得第一行
    member cmd.commitForFstRow conn =
        let reader = cmd.commitForReader conn

        if reader.Read() then
            let table = reader.GetSchemaTable()

            //初始化列元信息
            for i in 0 .. reader.FieldCount - 1 do
                new DataColumn(reader.GetName(i), reader.[i].GetType())
                |> table.Columns.Add

            let row = table.NewRow()

            //逐个添加列值
            for i in 0 .. reader.FieldCount - 1 do
                row.[reader.GetName(i)] <- reader.[i]

            Some row
        else
            None

    //提交并取得第一列
    member cmd.commitForFstCol conn =
        let reader = cmd.commitForReader conn

        if reader.Read() then
            let rec loop () = //因为第一次读取过，所以采用do while形式
                if reader.Read() then
                    reader.[0] :: loop ()
                else
                    []

            Some <| reader.[0] :: loop ()
        else
            None

    //提交并取得一张表
    member cmd.commitForTable conn =
        let reader = cmd.commitForReader conn

        let table = reader.GetSchemaTable()
        table.Rows.Clear() //刚开始会多出两行

        let any = reader.Read()

        //初始化列元信息
        for i in 0 .. reader.FieldCount - 1 do
            new DataColumn(reader.GetName(i), reader.[i].GetType())
            |> table.Columns.Add

        if any then
            let rec loop () = //因为第一次读取过，所以采用do while形式
                let row = table.NewRow()

                for i in 0 .. reader.FieldCount - 1 do
                    row.[reader.GetName(i)] <- reader.[i]

                table.Rows.Add row

                if reader.Read() then loop () else ()

            loop ()
        else
            ()

        table

    //在受影响行数满足断言时提交
    member cmd.commitWhen p (conn: DbConnection) =
        conn.useTransaction
        <| fun tx ->
            let affected =
                match cmd.useConn(conn).useTx(tx).ExecuteNonQuery() with
                | n when p n -> //符合期望影响行数规则则提交
                    tx.Commit()
                    n
                | _ -> //否则回滚
                    tx.Rollback()
                    0

            tx.Dispose() //资源释放
            cmd.Dispose()
            cmd.Transaction <- null

            affected

type internal DbCommand with
    member cmd.commitForAffectedAsync conn =
        cmd.useConn(conn).ExecuteNonQueryAsync()

    member cmd.commitForValueAsync conn = cmd.useConn(conn).ExecuteScalarAsync()

    member cmd.commitWhenAsync p (conn: DbConnection) =
        conn.useTransactionAsync
        <| fun tx ->
            task {
                let! n = cmd.useConn(conn).useTx(tx).ExecuteNonQueryAsync() //耗时操作

                let release () =
                    tx
                        .DisposeAsync()
                        .AsTask()
                        .ContinueWith(fun _ -> cmd.DisposeAsync())
                    |> ignore

                let affected =
                    if p n then
                        //符合期望影响行数规则则提交
                        tx
                            .CommitAsync()
                            .ContinueWith(fun _ ->
                                release ()
                                n)
                    else
                        //否则回滚
                        tx
                            .RollbackAsync()
                            .ContinueWith(fun _ ->
                                release ()
                                0)

                return affected
            }
        |> result
        |> result

type DbCommand with

    /// TODO exp async api
    member cmd.queryAsync sql = cmd.letQuery(sql).commitWhenAsync
    /// TODO exp async api
    member cmd.queryAsync(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitWhenAsync

type DbCommand with

    /// 执行任意查询
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    /// 低级操作：查询执行完成后应注意注销该连接以避免连接泄漏
    member cmd.query sql = cmd.letQuery(sql).commitWhen
    /// 执行任意参数化查询
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    /// 低级操作：查询执行完成后应注意注销该连接以避免连接泄漏
    member cmd.query(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitWhen


    /// 查询到表
    member cmd.select(sql) = cmd.letQuery(sql).commitForTable
    /// 参数化查询到表
    member cmd.select(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitForTable

type DbCommand with

    /// 查询到第一个值
    member cmd.getFstVal sql = cmd.letQuery(sql).commitForValue
    /// 参数化查询到第一个值
    member cmd.getFstVal(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitForValue

    /// 查询到第一行
    member cmd.getFstRow sql = cmd.letQuery(sql).commitForFstRow
    /// 参数化查询到第一行
    member cmd.getFstRow(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitForFstRow

    /// 查询到第一列
    member cmd.getFstCol(sql) = cmd.letQuery(sql).commitForFstCol
    /// 参数化查询到第一列
    member cmd.getFstCol(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitForFstCol
