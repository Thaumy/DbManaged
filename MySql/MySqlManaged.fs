namespace DbManaged.MySql

open System.Data
open System.Data.Common
open DbManaged
open MySql.Data.MySqlClient
open fsharper.op.Coerce
open fsharper.types
open fsharper.op
open DbManaged
open DbManaged.MySql.MySqlConnPool


/// MySql数据库管理器
type MySqlManaged private (pool) =

    /// 以连接信息构造
    new(msg) =
        let pool = MySqlConnPool(msg, "", 32u)
        MySqlManaged(pool)
    /// 以连接信息构造，并指定使用的数据库
    new(msg, schema) =
        let pool = MySqlConnPool(msg, schema, 32u)
        MySqlManaged(pool)
    /// 以连接信息构造，并指定使用的数据库和连接池大小
    new(msg, schema, poolSize) =
        let pool = MySqlConnPool(msg, schema, poolSize)
        MySqlManaged(pool)

    member self.getConnection = pool.getConnection

    // 所有查询均不负责类型转换
    interface IDbManaged with
        /// 查询到表
        member self.getTable sql =
            pool.hostConnection
            <| fun conn' ->
                let conn: MySqlConnection = coerce conn'
                let table = new DataTable()

                table
                |> (new MySqlDataAdapter(sql, conn)).Fill
                |> ignore

                table
        /// 参数化查询到表
        member self.getTable(sql, para) =
            pool.hostConnection
            <| fun conn' ->
                let conn: DbConnection = coerce conn'

                conn.hostCommand
                <| fun cmd' ->
                    let cmd: MySqlCommand = coerce cmd'

                    let table = new DataTable()

                    cmd.CommandText <- sql
                    cmd.Parameters.AddRange para //添加参数

                    (new MySqlDataAdapter(cmd)).Fill table |> ignore

                    table


        /// 查询到第一个值
        member self.getFstVal sql =
            pool.hostConnection
            <| fun conn ->
                conn.hostCommand
                <| fun cmd ->
                    cmd.CommandText <- sql

                    //如果结果集为空，ExecuteScalar返回null
                    match cmd.ExecuteScalar() with
                    | null -> None
                    | x -> Some x
        /// 参数化查询到第一个值
        member self.getFstVal(sql, para) =
            pool.hostConnection
            <| fun conn ->
                conn.hostCommand
                <| fun cmd ->
                    cmd.CommandText <- sql
                    cmd.Parameters.AddRange para

                    //如果结果集为空，ExecuteScalar返回null
                    match cmd.ExecuteScalar() with
                    | null -> None
                    | x -> Some x
        /// 从既有DataTable中查询到第一个 whereKey 等于 whereKeyVal 的行的 targetKey 值
        member self.getFstVal(table: string, targetKey: string, (whereKey: string, whereKeyVal: 'V)) =
            pool.hostConnection
            <| fun conn ->
                conn.hostCommand
                <| fun cmd ->
                    cmd.CommandText <- $"SELECT `{targetKey}` FROM `{table}` WHERE `{whereKey}`=?whereKeyVal"

                    cmd.Parameters.AddRange [| MySqlParameter("whereKeyVal", whereKeyVal) |]

                    //如果结果集为空，ExecuteScalar返回null
                    match cmd.ExecuteScalar() with
                    | null -> None
                    | x -> Some x
        /// 查询到第一行
        member self.getFstRow sql =
            (self :> IDbManaged).getTable sql
            >>= fun t ->
                    Ok
                    <| match t.Rows with
                       //仅当行数非零时有结果
                       | rows when rows.Count <> 0 -> Some rows.[0]
                       | _ -> None
        /// 参数化查询到第一行
        member self.getFstRow(sql, para) =
            (self :> IDbManaged).getTable (sql, para)
            >>= fun t ->
                    Ok
                    <| match t.Rows with
                       //仅当行数非零时有结果
                       | rows when rows.Count <> 0 -> Some rows.[0]
                       | _ -> None

        /// 从既有DataTable中取出第一个 whereKey 等于 whereKeyVal 的行
        member self.getFstRowFrom (table: DataTable) (whereKey: string) whereKeyVal =
            match table.Rows with
            | rows when rows.Count <> 0 ->

                [ for r in rows -> r ]
                |> filter (fun (row: DataRow) -> row.[whereKey].ToString() = whereKeyVal.ToString())
                |> head

            | _ -> None


        /// 查询到第一列
        member self.getFstCol sql =
            (self :> IDbManaged).getTable sql
            >>= ((self :> IDbManaged).getFstColFrom .|> Ok)
        /// 参数化查询到第一列
        member self.getFstCol(sql, para) =
            (self :> IDbManaged).getTable (sql, para)
            >>= ((self :> IDbManaged).getFstColFrom .|> Ok)
        /// 从既有DataTable中取出第一列
        member self.getFstColFrom(table: DataTable) =
            match table.Rows with
            | rows when rows.Count <> 0 ->

                //此处未考虑列数为0的情况
                [ for r in rows -> r ]
                |> map (fun (row: DataRow) -> row.[0])
                |> Some

            | _ -> None


        /// 查询到指定列
        member self.getCol(sql, key) =
            (self :> IDbManaged).getTable sql
            >>= fun t -> Ok <| (self :> IDbManaged).getColFrom t key
        /// 参数化查询到指定列
        member self.getCol(sql, key, para) =
            (self :> IDbManaged).getTable (sql, para)
            >>= fun t -> Ok <| (self :> IDbManaged).getColFrom t key
        /// 从既有DataTable中取出指定列
        member self.getColFrom (table: DataTable) (key: string) =
            match table.Rows with
            | rows when rows.Count <> 0 ->

                //此处未考虑列数为0的情况和取用失败的情况
                [ for r in rows -> r ]
                |> map (fun (row: DataRow) -> row.[key])
                |> Some

            | _ -> None



        //partial...


        /// 从连接池取用 MySqlConnection 并在其上调用同名方法
        member self.execute sql =
            self.getConnection ()
            >>= fun conn -> conn.execute sql |> Ok
        /// 从连接池取用 MySqlConnection 并在其上调用同名方法
        member self.execute(sql, para) =
            self.getConnection ()
            >>= fun conn -> conn.execute (sql, para) |> Ok


        /// 从连接池取用 MySqlConnection 并在其上调用同名方法
        member self.executeUpdate(table, (setKey, setKeyVal), (whereKey, whereKeyVal)) =
            self.getConnection ()
            >>= fun conn' ->
                    let conn: MySqlConnection = coerce conn'

                    (table, (setKey, setKeyVal), (whereKey, whereKeyVal))
                    |> conn.executeUpdate
                    |> Ok
        /// 从连接池取用 MySqlConnection 并在其上调用同名方法
        member self.executeUpdate(table, key, newValue, oldValue) =
            self.getConnection ()
            >>= fun conn' ->
                    let conn: MySqlConnection = coerce conn'

                    (table, key, newValue, oldValue)
                    |> conn.executeUpdate
                    |> Ok


        /// 从连接池取用 MySqlConnection 并在其上调用同名方法
        member self.executeInsert table pairs =
            self.getConnection ()
            >>= fun conn' ->
                    let conn: MySqlConnection = coerce conn'

                    conn.executeInsert table pairs |> Ok
        /// 从连接池取用 MySqlConnection 并在其上调用同名方法
        member self.executeDelete table (whereKey, whereKeyVal) =
            self.getConnection ()
            >>= fun conn' ->
                    let conn: MySqlConnection = coerce conn'

                    conn.executeDelete table (whereKey, whereKeyVal)
                    |> Ok
