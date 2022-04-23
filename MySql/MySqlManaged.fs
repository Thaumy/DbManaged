namespace DbManaged.MySql

open System
open System.Data
open System.Data.Common
open MySql.Data.MySqlClient
open fsharper.op.Coerce
open fsharper.types
open fsharper.op
open DbManaged
open DbManaged.MySql.MySqlConnPool

/// MySql数据库管理器
type MySqlManaged private (pool) =
    inherit IDbManaged()
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

    // 所有查询均不负责类型转换

    /// 查询到表
    override self.executeSelect sql =
        pool.hostConnection
        <| fun conn' ->
            let conn: MySqlConnection = coerce conn'
            let table = new DataTable()

            table
            |> (new MySqlDataAdapter(sql, conn)).Fill
            |> ignore

            table
    /// 参数化查询到表
    override self.executeSelect(sql, paras: (string * 't) list) =
        let paras' =
            foldMap (fun (k: string, v) -> List' [ MySqlParameter(k, v :> obj) ]) paras
            |> unwrap

        (self :> IDbManaged)
            .executeSelect (sql, paras'.toArray ())
    /// 参数化查询到表
    override self.executeSelect(sql, para: #DbParameter array) =
        pool.hostConnection
        <| fun conn' ->
            let conn: MySqlConnection = coerce conn'

            conn.hostCommand
            <| fun cmd' ->
                let cmd: MySqlCommand = coerce cmd'

                let table = new DataTable()

                cmd.CommandText <- sql
                cmd.Parameters.AddRange para //添加参数

                (new MySqlDataAdapter(cmd)).Fill table |> ignore

                table


    /// 查询到第一个值
    override self.getFstVal sql =
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
    override self.getFstVal(sql, paras: (string * 't) list) =
        let paras' =
            foldMap (fun (k: string, v) -> List' [ MySqlParameter(k, v :> obj) ]) paras
            |> unwrap

        (self :> IDbManaged)
            .getFstVal (sql, paras'.toArray ())
    /// 参数化查询到第一个值
    override self.getFstVal(sql, para: #DbParameter array) =
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
    /// 参数化查询到第一个值
    override self.getFstVal(table: string, targetKey: string, (whereKey: string, whereKeyVal: 'V)) =
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
    override self.getFstRow sql =
        (self :> IDbManaged).executeSelect sql
        >>= fun t ->
                Ok
                <| match t.Rows with
                   //仅当行数非零时有结果
                   | rows when rows.Count <> 0 -> Some rows.[0]
                   | _ -> None
    /// 参数化查询到第一行
    override self.getFstRow(sql, paras: (string * 't) list) =
        let paras' =
            foldMap (fun (k: string, v) -> List' [ MySqlParameter(k, v :> obj) ]) paras
            |> unwrap

        (self :> IDbManaged)
            .getFstRow (sql, paras'.toArray ())
    /// 参数化查询到第一行
    override self.getFstRow(sql, paras: #DbParameter array) =
        (self :> IDbManaged).executeSelect (sql, paras)
        >>= fun t ->
                Ok
                <| match t.Rows with
                   //仅当行数非零时有结果
                   | rows when rows.Count <> 0 -> Some rows.[0]
                   | _ -> None

    /// 查询到指定列
    override self.getCol(sql, key: string) =
        (self :> IDbManaged).executeSelect sql
        >>= fun t -> (self :> IDbManaged).getColFrom (t, key) |> Ok

    /// 参数化查询到指定列
    override self.getCol(sql, key: string, paras: (string * 't) list) =
        let paras' =
            foldMap (fun (k: string, v) -> List' [ MySqlParameter(k, v :> obj) ]) paras
            |> unwrap

        (self :> IDbManaged)
            .getCol (sql, key, paras'.toArray ())
    /// 参数化查询到指定列
    override self.getCol(sql, key: string, paras: #DbParameter array) =
        (self :> IDbManaged).executeSelect (sql, paras)
        >>= fun t -> Ok <| (self :> IDbManaged).getColFrom (t, key)


    /// 查询到指定列
    override self.getCol(sql, index: uint) =
        (self :> IDbManaged).executeSelect sql
        >>= fun t -> (self :> IDbManaged).getColFrom (t, index) |> Ok

    /// 参数化查询到指定列
    override self.getCol(sql, index: uint, paras: (string * 't) list) =
        let paras' =
            foldMap (fun (k: string, v) -> List' [ MySqlParameter(k, v :> obj) ]) paras
            |> unwrap

        (self :> IDbManaged)
            .getCol (sql, index, paras'.toArray ())
    /// 参数化查询到指定列
    override self.getCol(sql, index: uint, paras: #DbParameter array) =
        (self :> IDbManaged).executeSelect (sql, paras)
        >>= fun t -> Ok <| (self :> IDbManaged).getColFrom (t, index)


    //partial...

    //TODO
    override self.executeAnyAsync sql = raise (NotImplementedException())

    override self.executeAny sql =
        pool.getConnection ()
        >>= fun conn ->
                let result = conn.executeAny sql

                lazy (pool.recycleConnection conn) |> result |> Ok

    override self.executeAny(sql, paras: (string * 't) list) =
        let paras' =
            foldMap (fun (k: string, v) -> List' [ MySqlParameter(k, v :> obj) ]) paras
            |> unwrap

        (self :> IDbManaged)
            .executeAny (sql, paras'.toArray ())

    override self.executeAny(sql, paras) =
        pool.getConnection ()
        >>= fun conn ->
                let result = conn.executeAny (sql, paras)

                lazy (pool.recycleConnection conn) |> result |> Ok


    override self.executeUpdate(table, (setKey, setKeyVal), (whereKey, whereKeyVal)) =
        pool.getConnection ()
        >>= fun conn' ->
                let conn: MySqlConnection = coerce conn'

                let result =
                    conn.executeUpdate (table, (setKey, setKeyVal), (whereKey, whereKeyVal))

                lazy (pool.recycleConnection conn) |> result |> Ok

    override self.executeUpdate(table, key, newValue, oldValue) =
        pool.getConnection ()
        >>= fun conn' ->
                let conn: MySqlConnection = coerce conn'


                let result =
                    conn.executeUpdate (table, key, newValue, oldValue)

                lazy (pool.recycleConnection conn) |> result |> Ok



    override self.executeInsert table pairs =
        pool.getConnection ()
        >>= fun conn' ->
                let conn: MySqlConnection = coerce conn'

                let result = conn.executeInsert table pairs

                lazy (pool.recycleConnection conn) |> result |> Ok

    override self.executeDelete table (whereKey, whereKeyVal) =
        pool.getConnection ()
        >>= fun conn' ->
                let conn: MySqlConnection = coerce conn'

                let result =
                    conn.executeDelete table (whereKey, whereKeyVal)

                lazy (pool.recycleConnection conn) |> result |> Ok
