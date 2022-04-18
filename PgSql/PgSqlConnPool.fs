module internal DbManaged.PgSql.PgSqlConnPool

open System
open System.Data
open System.Data.Common
open Npgsql
open fsharper.op
open fsharper.types
open DbManaged
open DbManaged.DbConnPool

/// PgSql数据库连接池
type internal PgSqlConnPool(msg: DbConnMsg, database, size: uint) =
    inherit IDbConnPool()

    /// 所有连接列表
    /// 新连接总是在此列表首部
    let mutable connList: NpgsqlConnection list = []

    /// 空闲连接列表
    /// 其中的任意连接均属于connList
    let mutable idleConnList: NpgsqlConnection list = []

    /// 尝试清理连接池
    let poolClean () =
        connList <-
            filter
            <| fun (conn: NpgsqlConnection) ->
                match conn.State with
                //如果连接中断或是关闭（这都是不工作的状态）
                | ConnectionState.Broken
                | ConnectionState.Closed ->
                    conn.Dispose() //注销
                    false //移除
                | _ -> true //保留
            <| connList


    /// 取得空闲连接
    let getIdleConn () =
        match idleConnList with
        | x :: xs ->
            idleConnList <- xs //移除头部连接
            Some x
        | [] -> None

    /// 连接字符串
    let connStr = //启用连接池，最大超时1秒
        $"\
                    Host = {msg.Host};\
                DataBase = {database};\
                    Port = {msg.Port};\
                  UserID = {msg.User};\
                Password = {msg.Password};\
                 Pooling = True;\
             MaxPoolSize = {size};\
       "
        + if database = "" then //TODO 等号右侧空格测试
              ""
          else
              $"DataBase ={database};"

    override this.recycleConnection conn =
        match conn.State with
        | ConnectionState.Open -> idleConnList <- (coerce conn) :: idleConnList
        | _ -> () //连接不可用，交由poolClean调用处统一清理

    /// 从连接池取用 NpgsqlConnection
    override this.getConnection() =
        let genConn () =
            let newConn = new NpgsqlConnection()

            connList <- newConn :: connList

            newConn.ConnectionString <- connStr
            newConn.Open()
            newConn

        try
            match uint connList.Length with
            | len when //连接数较少时，新建
                len < uint (float size * 0.6)
                ->
                //Console.Write $"+{connList.Length}:{idleConnList.Length} "
                genConn ()
            | len when //连接数较多时，在循环复用的基础上新建
                len < uint (float size * 0.8)
                ->
                poolClean ()

                match getIdleConn () with
                | Some c ->
                    //Console.Write $"~{connList.Length}:{idleConnList.Length} "
                    c
                | None ->
                    //Console.Write $"+{connList.Length}:{idleConnList.Length} "
                    genConn ()
            | _ -> //连接数过多时，清理后新建
                //Console.Write $"-+{connList.Length}:{idleConnList.Length} "
                poolClean ()
                genConn ()
            :> DbConnection
            |> Ok
        with
        | e -> Err e
