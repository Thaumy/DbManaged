module internal DbManaged.MySql.MySqlConnPool

open System.Data
open MySql.Data.MySqlClient
open DbManaged.MySql
open DbManaged.DbConnPool
open fsharper.op
open fsharper.types


/// MySql数据库连接池
type internal MySqlConnPool(msg: MySqlConnMsg, schema, size: uint) =
    inherit IDbConnPool()

    /// 连接列表
    /// 新连接总是在此列表首部
    let mutable ConnList: MySqlConnection list = []

    /// 尝试清理连接池
    let tryCleanConnPool () =
        ConnList <-
            filter
            <| fun (conn: MySqlConnection) ->
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
        <| fun (conn: MySqlConnection) ->
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
        if schema = "" then //TODO 等号右侧空格测试
            $"Host ={msg.Host};\
                     Port ={msg.Port};\
                  UserID ={msg.User};\
                Password ={msg.Password};\
         UseAffectedRows =TRUE;" //使UPDATE语句返回受影响的行数而不是符合查询条件的行数
        else
            $"Host ={msg.Host};\
                DataBase ={schema};\
                    Port ={msg.Port};\
                  UserID ={msg.User};\
                Password ={msg.Password};\
         UseAffectedRows =TRUE;"



    /// 从连接池取用 MySqlConnection
    override this.getConnection() =
        let genConn () =
            let newConn = new MySqlConnection(ConnStr)

            ConnList <- newConn :: ConnList

            newConn.Open()
            newConn

        try
            Ok
            <| match uint ConnList.Length with
               | len when //连接数较少时，新建
                   len <= size / 2u
                   ->
                   genConn ()
               | len when //连接数较多时，在循环复用的基础上新建
                   len <= size
                   ->
                   match getIdleConn () with
                   | Some c -> c
                   | None -> genConn ()
               | _ -> //连接数过多时，清理后新建
                   tryCleanConnPool ()
                   genConn ()

        with
        | e -> Err e
