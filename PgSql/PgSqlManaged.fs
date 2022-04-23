namespace DbManaged.PgSql

open System.Data
open System.Data.Common
open Npgsql
open fsharper.types
open fsharper.op
open DbManaged
open DbManaged.ext
open DbManaged.PgSql.ext

/// PgSql数据库管理器
type PgSqlManaged private (pool: IDbConnPoolAsync) =

    /// 以连接信息构造
    new(msg) =
        let pool = PgSqlConnPool(msg, "", 32u)
        PgSqlManaged(pool)
    /// 以连接信息构造，并指定使用的数据库
    new(msg, database) =
        let pool = PgSqlConnPool(msg, database, 32u)
        PgSqlManaged(pool)
    /// 以连接信息构造，并指定使用的数据库和连接池大小
    new(msg, database, poolSize) =
        let pool = PgSqlConnPool(msg, database, poolSize)
        PgSqlManaged(pool)

    // 所有查询均不负责类型转换
    interface IDbManaged with

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
        member self.getFstVal(sql, paras: (string * 't) list) =
            let paras' =
                foldMap (fun (k: string, v) -> List' [ NpgsqlParameter(k, v :> obj) ]) paras
                |> unwrap

            (self :> IDbManaged)
                .getFstVal (sql, paras'.toArray ())
        /// 参数化查询到第一个值
        member self.getFstVal(sql, paras: #DbParameter array) =
            pool.hostConnection
            <| fun conn ->
                conn.hostCommand
                <| fun cmd ->
                    cmd.CommandText <- sql
                    cmd.Parameters.AddRange paras

                    //如果结果集为空，ExecuteScalar返回null
                    match cmd.ExecuteScalar() with
                    | null -> None
                    | x -> Some x
        /// 参数化查询到第一个值
        member self.getFstVal(table: string, targetKey: string, (whereKey: string, whereKeyVal: 'V)) =
            pool.hostConnection
            <| fun conn ->
                conn.hostCommand
                <| fun cmd ->
                    cmd.CommandText <- $"SELECT {targetKey} FROM {table} WHERE {whereKey}=:whereKeyVal"

                    cmd.Parameters.AddRange [| NpgsqlParameter("whereKeyVal", whereKeyVal) |]

                    //如果结果集为空，ExecuteScalar返回null
                    match cmd.ExecuteScalar() with
                    | null -> None
                    | x -> Some x


        /// 查询到第一行
        member self.getFstRow sql =
            (self :> IDbManaged).executeSelect sql
            >>= fun t ->
                    Ok
                    <| match t.Rows with
                       //仅当行数非零时有结果
                       | rows when rows.Count <> 0 -> Some rows.[0]
                       | _ -> None
        /// 参数化查询到第一行
        member self.getFstRow(sql, paras: (string * 't) list) =
            let paras' =
                foldMap (fun (k: string, v) -> List' [ NpgsqlParameter(k, v :> obj) ]) paras
                |> unwrap

            (self :> IDbManaged)
                .getFstRow (sql, paras'.toArray ())
        /// 参数化查询到第一行
        member self.getFstRow(sql, paras: #DbParameter array) =
            (self :> IDbManaged).executeSelect (sql, paras)
            >>= fun t ->
                    Ok
                    <| match t.Rows with
                       //仅当行数非零时有结果
                       | rows when rows.Count <> 0 -> Some rows.[0]
                       | _ -> None

        //TODO getCol样板代码还可以减少（根据索引类型）

        /// 查询到指定列
        member self.getCol(sql, key: string) =
            (self :> IDbManaged).executeSelect sql
            >>= fun t -> Ok <| getColFromByKey (t, key)
        /// 参数化查询到指定列
        member self.getCol(sql, key: string, paras: (string * 't) list) =
            let paras' =
                foldMap (fun (k: string, v) -> List' [ NpgsqlParameter(k, v :> obj) ]) paras
                |> unwrap

            (self :> IDbManaged)
                .getCol (sql, key, paras'.toArray ())
        /// 参数化查询到指定列
        member self.getCol(sql, key: string, paras: #DbParameter array) =
            (self :> IDbManaged).executeSelect (sql, paras)
            >>= fun t -> getColFromByKey (t, key) |> Ok


        /// 查询到指定列
        member self.getCol(sql, index: uint) =
            (self :> IDbManaged).executeSelect sql
            >>= fun t -> Ok <| getColFromByIndex (t, index)
        /// 参数化查询到指定列
        member self.getCol(sql, index: uint, paras: (string * 't) list) =
            let paras' =
                foldMap (fun (k: string, v) -> List' [ NpgsqlParameter(k, v :> obj) ]) paras
                |> unwrap

            (self :> IDbManaged)
                .getCol (sql, index, paras'.toArray ())
        /// 参数化查询到指定列
        member self.getCol(sql, index: uint, paras: #DbParameter array) =
            (self :> IDbManaged).executeSelect (sql, paras)
            >>= fun t -> getColFromByIndex (t, index) |> Ok

        //partial...

        /// 从连接池取用 NpgsqlConnection 并在其上调用同名方法
        member self.executeAny sql =
            pool.getConnection ()
            >>= fun conn ->
                    let result = conn.executeAny sql

                    lazy (pool.recycleConnection conn) |> result |> Ok
        /// 从连接池取用 NpgsqlConnection 并在其上调用同名方法
        member self.executeAny(sql, paras: (string * 't) list) =
            let paras' =
                foldMap (fun (k: string, v) -> List' [ NpgsqlParameter(k, v :> obj) ]) paras
                |> unwrap

            (self :> IDbManaged)
                .executeAny (sql, paras'.toArray ())
        /// 从连接池取用 NpgsqlConnection 并在其上调用同名方法
        member self.executeAny(sql, paras) =
            pool.getConnection ()
            >>= fun conn ->
                    let result = conn.executeAny (sql, paras)

                    lazy (pool.recycleConnection conn) |> result |> Ok

        /// 查询到表
        member self.executeSelect sql =
            pool.hostConnection
            <| fun conn' ->
                let conn: NpgsqlConnection = coerce conn'
                let table = new DataTable()

                table
                |> (new NpgsqlDataAdapter(sql, conn)).Fill
                |> ignore

                table
        /// 参数化查询到表
        member self.executeSelect(sql, paras: (string * 't) list) =
            let paras' =
                foldMap (fun (k: string, v) -> List' [ NpgsqlParameter(k, v :> obj) ]) paras
                |> unwrap

            (self :> IDbManaged)
                .executeSelect (sql, paras'.toArray ())
        /// 参数化查询到表
        member self.executeSelect(sql, paras: #DbParameter array) =
            pool.hostConnection
            <| fun conn' ->
                let conn: NpgsqlConnection = coerce conn'

                conn.hostCommand
                <| fun cmd' ->
                    let cmd: NpgsqlCommand = coerce cmd'

                    let table = new DataTable()

                    cmd.CommandText <- sql
                    cmd.Parameters.AddRange paras //添加参数

                    (new NpgsqlDataAdapter(cmd)).Fill table |> ignore

                    table

        /// 从连接池取用 NpgsqlConnection 并在其上调用同名方法
        member self.executeUpdate(table, (setKey, setKeyVal), (whereKey, whereKeyVal)) =
            pool.getConnection ()
            >>= fun conn' ->
                    let conn: NpgsqlConnection = coerce conn'

                    let result =
                        conn.executeUpdate (table, (setKey, setKeyVal), (whereKey, whereKeyVal))

                    lazy (pool.recycleConnection conn) |> result |> Ok
        /// 从连接池取用 NpgsqlConnection 并在其上调用同名方法
        member self.executeUpdate(table, key, newValue, oldValue) =
            pool.getConnection ()
            >>= fun conn' ->
                    let conn: NpgsqlConnection = coerce conn'

                    let result =
                        conn.executeUpdate (table, key, newValue, oldValue)

                    lazy (pool.recycleConnection conn) |> result |> Ok

        /// 从连接池取用 NpgsqlConnection 并在其上调用同名方法
        member self.executeInsert table pairs =
            pool.getConnection ()
            >>= fun conn' ->
                    let conn: NpgsqlConnection = coerce conn'

                    let result = conn.executeInsert table pairs

                    lazy (pool.recycleConnection conn) |> result |> Ok
        /// 从连接池取用 NpgsqlConnection 并在其上调用同名方法
        member self.executeDelete table (whereKey, whereKeyVal) =
            pool.getConnection ()
            >>= fun conn' ->
                    let conn: NpgsqlConnection = coerce conn'

                    let result =
                        conn.executeDelete table (whereKey, whereKeyVal)

                    lazy (pool.recycleConnection conn) |> result |> Ok

    interface IDbManagedAsync with
        /// TODO exp async api
        member self.executeAnyAsync sql =
            let r = pool.getConnectionAsync().Result

            r
            >>= fun conn ->
                    let result = conn.executeAnyAsync sql

                    lazy (pool.recycleConnectionAsync conn |> ignore)
                    |> result
                    |> Ok
        /// TODO exp async api
        member self.executeAnyAsync(sql, paras: (string * 't) list) =
            let paras' =
                foldMap (fun (k: string, v) -> List' [ NpgsqlParameter(k, v :> obj) ]) paras
                |> unwrap

            (self :> IDbManagedAsync)
                .executeAnyAsync (sql, paras'.toArray ())
        /// TODO exp async api
        member self.executeAnyAsync(sql, paras) =
            let r = pool.getConnectionAsync().Result

            r
            >>= fun conn ->
                    let result = conn.executeAnyAsync (sql, paras)

                    lazy (pool.recycleConnectionAsync conn |> ignore)
                    |> result
                    |> Ok
