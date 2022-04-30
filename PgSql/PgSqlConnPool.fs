namespace DbManaged.PgSql

open System.Data.Common
open System.Threading.Tasks
open System.Threading.Channels
open System.Collections.Concurrent
open Npgsql
open fsharper.op
open fsharper.types
open DbManaged

/// PgSql数据库连接池
type internal PgSqlConnPool(msg: DbConnMsg, database, size: uint) =

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
        + if database = "" then 
              ""
          else
              $"DataBase = {database};"

    /// 空闲连接表
    let freeConnections =
        Channel.CreateBounded<NpgsqlConnection>(int size)

    /// 忙碌连接表
    let busyConnections =
        ConcurrentDictionary<int, NpgsqlConnection>()

    /// 池压力系数
    /// 若池压力越大，该系数越接近1，反之接近0
    let getPoolPressureCoef () =
        let freeCount = float freeConnections.Reader.Count
        let busyCount = float busyConnections.Count

        //+1.0是为了防止算术错误
        busyCount / (freeCount + busyCount + 1.0) //init is 0

    /// 池占用率
    let getPoolOccRate () =
        let freeCount = float freeConnections.Reader.Count
        let busyCount = float busyConnections.Count

        (freeCount + busyCount) / float size //init is 0

    /// 添加到忙碌连接表
    let busyConnectionsTryAdd conn =
        busyConnections.TryAdd(conn.GetHashCode(), conn)

    /// 忙碌连接表移除
    let busyConnectionsTryRemove (conn: NpgsqlConnection) =
        busyConnections.TryRemove(conn.GetHashCode())

    /// 空闲连接表
    let freeConnectionsAdd conn = freeConnections.Writer.WriteAsync conn

    /// 尝试取得空闲连接
    let freeConnectionsGet () =
        freeConnections.Reader.ReadAsync().AsTask().Result

    /// 尝试取得空闲连接
    let rec freeConnectionsTryGet () =
        let ok, conn = freeConnections.Reader.TryRead()
        if ok then Some conn else None

    /// 生成新连接
    let rec connGen () =
        let conn = new NpgsqlConnection(connStr)

        conn.Open()
        conn

    /// 尝试生成新连接
    let rec connTryGen () =
        let count =
            uint (
                freeConnections.Reader.Count
                + busyConnections.Count
            )

        if count < size then
            Some(connGen ())
        else
            None

    let outputPoolStatus () =
        let occ = (getPoolOccRate ()).ToString("0.00")

        let pressure =
            (getPoolPressureCoef ()).ToString("0.00")

        let free =
            freeConnections.Reader.Count.ToString("00")

        let busy = busyConnections.Count.ToString("00")

        let freeAndBusy =
            (freeConnections.Reader.Count
             + busyConnections.Count)
                .ToString("00")

        printfn $"[占用 {occ}: {freeAndBusy} /{size}] [压力 {pressure}: 忙{busy} 闲{free}]"



    interface IDbConnPool with
        member self.recycleConnection conn =

            match busyConnectionsTryRemove (coerce conn) with
            | true, removed when removed.Equals conn ->

                if getPoolPressureCoef () < 0.3 then
                    conn.DisposeAsync() //增加池压力
                else
                    //从busyConnections移除了连接，且被移除的连接是目标连接（此处的Equals判断引用相等性）
                    freeConnectionsAdd (coerce conn) //加入空闲连接表

            | true, removed ->
                (*从busyConnections移除了连接，但被移除的连接不是目标连接
                    这意味着哈希冲突，此时需将removed返还到忙碌连接表，同时销毁目标连接
                    不进行回收的原因如下：
                    *使用该连接可能进一步引发哈希冲突*)
                busyConnectionsTryAdd removed |> ignore
                conn.DisposeAsync()
            | false, _ ->
                (*移除失败，这意味着下列情况之一：
                    1.曾经试图将这个连接加入busyConnections，但由于哈希冲突失败了
                    2.这个连接根本不属于连接池
                    对于这样的连接，直接进行销毁
                    不进行回收的原因如下：
                    *使用该连接可能进一步引发哈希冲突
                    *该连接不受连接池管制，可能引发安全性问题*)
                conn.DisposeAsync()
            |> ignore

        /// TODO exp async api


        /// 从连接池取用 NpgsqlConnection
        member self.getConnection() =
            try
                let result =
                    if getPoolOccRate () < 0.8 then
                        match getPoolPressureCoef () with
                        | p when //池压力较小，复用连接以提升池压力系数
                            p < 0.7
                            ->
                            freeConnectionsTryGet().unwrapOr connGen
                        | p when //池压力较大，新建连接以降低池压力系数
                            p < 0.8
                            ->
                            connGen ()
                        | _ -> //池压力过大，新建更多连接以降低池压力系数
                            Task.Run
                                (fun _ ->
                                    connTryGen()
                                        .whenCanUnwrap (fun c -> freeConnectionsAdd c |> ignore))
                            |> ignore

                            connGen ()
                    else
                        freeConnectionsGet ()
                (*加入忙碌列表，如果加入失败则表明该连接与已登记连接存在哈希冲突，
                此时不进行登记，在回收阶段会检测到该连接并将其销毁*)
                busyConnectionsTryAdd result |> ignore //添加到忙碌连接表

                Task.Run outputPoolStatus |> ignore

                result :> DbConnection |> Ok
            with
            | e -> Err e

    interface IDbConnPoolAsync with
        member self.recycleConnectionAsync conn =
            Task.Run<unit>(fun _ -> (self :> IDbConnPool).recycleConnection conn)

        /// TODO exp async api
        member self.getConnectionAsync() =
            Task.Run (self :> IDbConnPool).getConnection
