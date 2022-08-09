[<AutoOpen>]
module DbManaged.ext_DbCommand

open System
open System.Data
open System.Data.Common
open System.Threading.Tasks
open fsharper.op
open fsharper.typ
open fsharper.alias
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

//Sync
type internal DbCommand with

    //提交并返回受影响的行数
    member cmd.commitForAffected conn =
        //如果结果集为空，ExecuteScalar返回null
        cmd.useConn(conn).ExecuteNonQuery()

    //提交并取得第一行第一列的值
    member cmd.commitForScalar conn =
        cmd.useConn(conn).ExecuteScalar()
        |> Option'.fromNullable

    //提交并取得一个读取器
    member cmd.commitForReader conn = cmd.useConn(conn).ExecuteReader()

    //提交并取得第一行
    member cmd.commitForFstRow conn =
        use reader = cmd.commitForReader conn

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
        //由于一次性seq的通用性较低，且为seq添加缓存容易产生并发安全问题，故暂停惰性版本的实现
        (*
        let reader = cmd.commitForReader conn
        Seq.unfold
        <| fun (reader: DbDataReader) ->
            if reader.Read() then
                Option.Some(reader.[0], reader)
            else
                reader.Dispose()
                Option.None
        <| reader
        *)
        use reader = cmd.commitForReader conn

        let rec loop () =
            if reader.Read() then
                reader.[0] :: loop ()
            else
                []

        loop ()

    //提交并取得一张表
    member cmd.commitForTable conn =
        use reader = cmd.commitForReader conn

        if reader.Read() then
            let table = reader.GetSchemaTable()
            table.Rows.Clear() //TODO 刚开始会多出两行的原因

            //初始化列元信息
            for i in 0 .. reader.FieldCount - 1 do
                new DataColumn(reader.GetName(i), reader.[i].GetType())
                |> table.Columns.Add

            let rec loop () = //因为第一次读取过，所以采用do while形式
                let row = table.NewRow()

                for i in 0 .. reader.FieldCount - 1 do
                    row.[reader.GetName(i)] <- reader.[i]

                table.Rows.Add row

                if reader.Read() then loop () else table

            //之所以返回option，是因为reader在无行可读时不能取得列元信息，其实是实现上的妥协
            Some(loop ())
        else
            None

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

            affected

//Async
type internal DbCommand with

    member cmd.commitForAffectedAsync conn =
        cmd.useConn(conn).ExecuteNonQueryAsync()

    member cmd.commitForScalarAsync conn : Task<Option'<obj>> =
        cmd
            .useConn(conn)
            .ExecuteScalarAsync()
            .Then(fun (t: Task<_>) -> t.Result |> Option'.fromNullable)

    member cmd.commitForReaderAsync conn = cmd.useConn(conn).ExecuteReaderAsync()

    member cmd.commitForFstRowAsync conn =
        task {
            use! reader = cmd.commitForReaderAsync conn
            let! exist = reader.ReadAsync()

            let result =
                if exist then
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

            //该实现受限于task的尾调用限制
            return result
        }

    member cmd.commitForFstColAsync conn =
        task {
            use! reader = cmd.commitForReaderAsync conn

            let! result =
                task {
                    return
                        [ while reader.Read() do
                              reader.[0] ]
                }

            //受限于任务表达式的限制，此处不能采用递归实现
            return result
        }

    member cmd.commitForTableAsync conn =
        task {
            use! reader = cmd.commitForReaderAsync conn
            let! exist = reader.ReadAsync()

            let result =
                if exist then
                    let table = reader.GetSchemaTable()
                    table.Rows.Clear() //TODO 刚开始会多出两行的原因

                    //初始化列元信息
                    for i in 0 .. reader.FieldCount - 1 do
                        new DataColumn(reader.GetName(i), reader.[i].GetType())
                        |> table.Columns.Add

                    let closure () =
                        let row = table.NewRow()

                        for i in 0 .. reader.FieldCount - 1 do
                            row.[reader.GetName(i)] <- reader.[i]

                        table.Rows.Add row

                    //因为第一次读取过，所以采用do while形式
                    closure ()
                    //受限于任务表达式的限制，此处不能采用递归实现
                    while reader.Read() do
                        closure ()

                    //之所以返回option，是因为reader在无行可读时不能取得列元信息，其实是实现上的妥协
                    Some table
                else
                    None

            return result
        }

    member cmd.commitWhenAsync p (conn: DbConnection) =
        conn.useTransactionAsync
        <| fun tx ->
            task {
                let! n = cmd.useConn(conn).useTx(tx).ExecuteNonQueryAsync() //耗时操作

                let release () =
                    tx.DisposeAsync().AsTask().Then(cmd.DisposeAsync)
                    |> ignore

                let affected =
                    if p n then
                        //符合期望影响行数规则则提交
                        tx.CommitAsync().Then(release .> always n)
                    else
                        //否则回滚
                        tx.RollbackAsync().Then(release .> always 0)

                return affected
            }
        |> result
        |> result

type DbCommand with

    /// 参数化查询到表
    [<CompiledName("select")>]
    member cmd.select(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitForTable

    /// 查询到表
    [<CompiledName("select")>]
    member cmd.select(sql) = cmd.select (sql, [])

    /// 参数化查询到表
    [<CompiledName("selectAsync")>]
    member cmd.selectAsync(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(
            paras
        )
            .commitForTableAsync

    /// 异步查询到表
    [<CompiledName("selectAsync")>]
    member cmd.selectAsync(sql) = cmd.selectAsync (sql, [])

type DbCommand with

    /// 执行任意参数化查询
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    /// 低级操作：查询执行完成后应注意注销该连接以避免连接泄漏
    [<CompiledName("query")>]
    member cmd.query(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitWhen

    /// 执行任意查询
    /// 返回的闭包用于检测受影响的行数，当断言成立时闭包会提交事务并返回受影响的行数
    /// 低级操作：查询执行完成后应注意注销该连接以避免连接泄漏
    [<CompiledName("query")>]
    member cmd.query sql = cmd.query (sql, [])

    /// 异步执行任意查询
    [<CompiledName("queryAsync")>]
    member cmd.queryAsync(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitWhenAsync

    /// 异步执行任意参数化查询
    [<CompiledName("queryAsync")>]
    member cmd.queryAsync sql = cmd.queryAsync (sql, [])

type DbCommand with

    /// 参数化查询到第一个值
    [<CompiledName("getFstVal")>]
    member cmd.getFstVal(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitForScalar

    /// 查询到第一个值
    [<CompiledName("getFstVal")>]
    member cmd.getFstVal sql = cmd.getFstVal (sql, [])

    /// 异步参数化查询到第一个值
    [<CompiledName("getFstValAsync")>]
    member cmd.getFstValAsync(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(
            paras
        )
            .commitForScalarAsync

    /// 异步查询到第一个值
    [<CompiledName("getFstValAsync")>]
    member cmd.getFstValAsync sql = cmd.letQuery(sql).commitForScalarAsync

type DbCommand with

    /// 参数化查询到第一行
    [<CompiledName("getFstRow")>]
    member cmd.getFstRow(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitForFstRow

    /// 查询到第一行
    [<CompiledName("getFstRow")>]
    member cmd.getFstRow sql = cmd.getFstRow (sql, [])

    /// 异步参数化查询到第一行
    [<CompiledName("getFstRowAsync")>]
    member cmd.getFstRowAsync(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(
            paras
        )
            .commitForFstRowAsync

    /// 异步查询到第一行
    [<CompiledName("getFstRowAsync")>]
    member cmd.getFstRowAsync sql = cmd.getFstRowAsync (sql, [])

type DbCommand with

    /// 参数化查询到第一列
    [<CompiledName("getFstCol")>]
    member cmd.getFstCol(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(paras).commitForFstCol

    /// 查询到第一列
    [<CompiledName("getFstCol")>]
    member cmd.getFstCol(sql) = cmd.getFstCol (sql, [])


    /// 异步查询到第一列
    [<CompiledName("getFstColAsync")>]
    member cmd.getFstColAsync(sql, paras: (string * 't) list) =
        cmd.letQuery(sql).addParas(
            paras
        )
            .commitForFstColAsync

    /// 异步参数化查询到第一行
    [<CompiledName("getFstColAsync")>]
    member cmd.getFstColAsync(sql) = cmd.getFstColAsync (sql, [])
