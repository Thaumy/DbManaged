module internal DbManaged.PgSql.PgSqlConnPool

open System
open System.Data
open System.Data.Common
open Npgsql
open fsharper.types
open DbManaged
open DbManaged.DbConnPool

/// PgSql数据库连接池
type internal PgSqlConnPool(msg: DbConnMsg, database, size: uint) =
    inherit IDbConnPool()


    /// 连接列表
    /// 新连接总是在此列表首部
    let mutable ConnList: NpgsqlConnection list = []

    /// 尝试清理连接池
    let tryCleanConnPool () =
        ConnList <-
            filter
            <| fun (conn: NpgsqlConnection) ->
                match conn.State with
                //如果连接中断或是关闭（这都是不工作的状态）
                | ConnectionState.Broken
                | ConnectionState.Closed ->
                    conn.Dispose() //注销
                    false //移除
                | _ -> true //保留
            <| ConnList


    /// 取得空闲连接
    let getIdleConn () =
        filter
        <| fun (conn: NpgsqlConnection) ->
            match conn.State with
            | ConnectionState.Broken
            | ConnectionState.Closed ->
                conn.Dispose()
                false
            | _ -> true

        <| ConnList

        |> head

    /// 连接字符串
    let ConnStr =
        if database = "" then //TODO 等号右侧空格测试
            $"Host ={msg.Host};\
                      Port ={msg.Port};\
                    UserID ={msg.User};\
                  Password ={msg.Password};"
        else
            $"Host ={msg.Host};\
                  DataBase ={database};\
                      Port ={msg.Port};\
                    UserID ={msg.User};\
                  Password ={msg.Password};"

    /// 从连接池取用 NpgsqlConnection
    override this.getConnection() =
        let genConn () =
            let newConn = new NpgsqlConnection(ConnStr)

            ConnList <- newConn :: ConnList

            newConn.Open()
            newConn

        try
            match uint ConnList.Length with
            | len when //连接数较少时，新建
                len <= size / 2u
                ->
                //Console.Write "+"
                genConn ()
            | len when //连接数较多时，在循环复用的基础上新建
                len <= size
                ->
                match getIdleConn () with
                | Some c ->
                    //Console.Write "~"
                    c
                | None ->
                    //Console.Write "+"
                    genConn ()
            | _ -> //连接数过多时，清理后新建
                //Console.Write "-+"
                tryCleanConnPool ()
                genConn ()
            :> DbConnection
            |> Ok
        with
        | e -> Err e
