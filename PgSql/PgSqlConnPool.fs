module internal DbManaged.PgSql.PgSqlConnPool

open System
open System.Data
open System.Data.Common
open System.Threading.Tasks
open System.Threading.Channels
open System.Collections.Concurrent
open Microsoft.FSharp.Core
open Npgsql
open fsharper.op
open fsharper.op.Fmt
open fsharper.op.Async
open fsharper.types
open DbManaged
open DbManaged.DbConnPool

/// PgSql数据库连接池
type internal PgSqlConnPool(msg: DbConnMsg, database, size: uint) =
    inherit IDbConnPool()

    /// 连接字符串
    let connStr = //启用连接池，最大超时1秒
        $"\
                    Host = {msg.Host};\
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

    /// 空闲连接
    let freeConnections =
        Channel.CreateBounded<NpgsqlConnection>(int size)

    /// 忙碌连接
    let busyConnections =
        ConcurrentDictionary<int, NpgsqlConnection>()

    /// 添加忙碌连接
    let busyConnectionsAdd conn =
        busyConnections.TryAdd(conn.GetHashCode(), conn)


    let busyConnectionsRemove (conn: NpgsqlConnection) =
        let status, _ =
            busyConnections.TryRemove(conn.GetHashCode())

        status


    /// 添加空闲连接
    let freeConnectionsAdd conn = freeConnections.Writer.WriteAsync conn

    /// 取得空闲连接
    let freeConnectionsGet () =
        let ok, conn = freeConnections.Reader.TryRead()
        if ok then Some conn else None


    /// 生成新连接
    let genNewConn () =
        let conn = new NpgsqlConnection(connStr)

        conn.Open()
        conn


    override self.recycleConnection conn =

        if busyConnectionsRemove (coerce conn) then
            freeConnectionsAdd (coerce conn) |> ignore //添加到空闲列表
        else
            ()


    /// TODO exp async api
    override self.recycleConnectionAsync conn =
        lock self (fun _ -> Task.Run<unit>(fun _ -> self.recycleConnection conn))

    /// 从连接池取用 NpgsqlConnection
    override self.getConnection() =
        let freeCount = freeConnections.Reader.Count
        let busyCount = busyConnections.Count

        try
            let result =
                match freeCount + busyCount with
                | n when //连接数较少时，新建
                    n < int (float size * 0.6)
                    ->
                    let conn = genNewConn ()

                    print "1"
                    conn

                | _ -> //连接数较多时，在复用的原则上新建
                    let conn =
                        freeConnectionsGet()
                            .unwarpOr (fun _ ->
                                print "+"
                                genNewConn ())

                    print "2"
                    conn

            busyConnectionsAdd result |> ignore //加入忙碌列表

            println
                $" {freeConnections.Reader.Count
                    + busyConnections.Count}:{freeConnections.Reader.Count}/{busyConnections.Count} (f/b)"

            result :> DbConnection |> Ok
        with
        | e -> Err e

    /// TODO exp async api
    override self.getConnectionAsync() =
        lock self (fun _ -> Task.Run self.getConnection)
