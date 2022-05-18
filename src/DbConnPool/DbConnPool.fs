namespace DbManaged.AnySql

open System
open System.Data.Common
open System.Threading.Tasks
open System.Threading.Channels
open System.Collections.Concurrent
open fsharper.op
open fsharper.typ
open fsharper.op.Eq
open fsharper.op.Alias
open DbManaged

/// PgSql数据库连接池
/// 对于不同的数据库，连接建立成本有所差异，应通过调节比例系数来达到最佳池性能平衡
type internal DbConnPool<'ConnType when 'ConnType :> DbConnection and 'ConnType: equality and 'ConnType: (new :
    unit -> 'ConnType)> public (msg: DbConnMsg, database, size: u32, d, n) as self =

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
        Channel.CreateBounded<'ConnType>(i32 size)

    /// 忙碌连接表
    let busyConnections = ConcurrentDictionary<int, 'ConnType>()

    /// 添加到忙碌连接表
    let busyConnectionsTryAdd conn =
        busyConnections.TryAdd(conn.GetHashCode(), conn)

    /// 从忙碌连接表移除
    let busyConnectionsTryRemove (conn: 'ConnType) =
        busyConnections.TryRemove(conn.GetHashCode())

    /// 添加到空闲连接表
    let mutable freeConnectionsAdd = freeConnections.Writer.WriteAsync

    /// 异步取得空闲连接
    let freeConnectionGetAsync () = freeConnections.Reader.ReadAsync()

    /// 取得空闲连接
    let freeConnectionGet () =
        freeConnectionGetAsync().AsTask().Result

    /// 尝试取得空闲连接
    let rec freeConnectionTryGet () =
        let ok, conn = freeConnections.Reader.TryRead()
        if ok then Some conn else None

    /// 生成新连接
    let genConn () =
        let conn = new 'ConnType()
        conn.ConnectionString <- connStr

        conn.Open()
        conn

    let genConnAsync () =
        let conn = new 'ConnType()
        conn.ConnectionString <- connStr

        task {
            let! _ = conn.OpenAsync()
            return conn
        }
        |> ValueTask<'ConnType>

    /// 尝试生成新连接
    let rec tryGenConn () =
        let count =
            u32 (
                freeConnections.Reader.Count
                + busyConnections.Count
            )

        if count < size then
            Some(genConn ())
        else
            None

    let outputPoolStatus () =
        fun _ ->
            let occ =
                (self :> IDbConnPool).occupancy.ToString "0.00"

            let pressure =
                (self :> IDbConnPool).pressure.ToString "0.00"

            let free =
                freeConnections.Reader.Count.ToString("00")

            let busy = busyConnections.Count.ToString("00")

            let freeAndBusy =
                (freeConnections.Reader.Count
                 + busyConnections.Count)
                    .ToString("00")

            printfn $"[占用 {occ}: {freeAndBusy} /{size}] [压力 {pressure}: 忙{busy} 闲{free}]"
        |> Task.Run
        |> ignore

    new(msg: DbConnMsg, size: u32, d, n) = new DbConnPool<'ConnType>(msg, "", size, d, n)

    interface IDisposable with
        /// 注销后不应进行新的查询
        member self.Dispose() =
            //对加入空闲连接表的请求进行拦截，注销要求加入的连接
            freeConnectionsAdd <- fun conn -> conn.DisposeAsync()

            let en =
                freeConnections
                    .Reader
                    .ReadAllAsync()
                    .GetAsyncEnumerator()

            let rec loop () =
                match en.MoveNextAsync().Result with
                | true ->
                    en.Current.DisposeAsync() |> ignore
                    loop ()
                | _ -> ()

            loop ()



    interface IDbConnPool with
        member self.pressure =
            let freeCount = f64 freeConnections.Reader.Count
            let busyCount = f64 busyConnections.Count

            //+1.0是为了防止算术错误
            busyCount / (freeCount + busyCount + 1.0) //init is 0

        member self.occupancy =
            let freeCount = f64 freeConnections.Reader.Count
            let busyCount = f64 busyConnections.Count

            (freeCount + busyCount) / f64 size //init is 0

        member self.recycleConnection conn =

            match busyConnectionsTryRemove (coerce conn) with
            | true, removed when refEq removed conn ->

                if (self :> IDbConnPool).pressure < d then
                    conn.DisposeAsync() //增加池压力
                else
                    //从busyConnections移除了连接，且被移除的连接是目标连接
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
                    2.这个连接根本不由连接池产生
                    对于这样的连接，直接进行销毁
                    不进行回收的原因如下：
                    *使用该连接可能进一步引发哈希冲突
                    *该连接不受连接池管制，可能引发安全性问题*)
                conn.DisposeAsync()
            |> ignore

        /// 从连接池取用 'ConnType
        member self.getConnection() =
            try
                let result =
                    //始终保留一部分连接以供其他用途
                    if (self :> IDbConnPool).occupancy < 0.8 then

                        match (self :> IDbConnPool).pressure with
                        | p when //池压力较小，复用连接以提升池压力系数
                            p < n
                            ->
                            freeConnectionTryGet().unwrapOr genConn
                        | _ -> //池压力较大，新建连接以降低池压力系数
                            (fun _ ->
                                (fun c -> freeConnectionsAdd c |> ignore)
                                |> tryGenConn().whenCanUnwrap)
                            |> Task.Run
                            |> ignore

                            freeConnectionTryGet().unwrapOr genConn
                    else
                        freeConnectionGet ()
                (*加入忙碌列表，如果加入失败则表明该连接与已登记连接存在哈希冲突，
                此时不进行登记，在回收阶段会检测到该连接并将其销毁*)
                busyConnectionsTryAdd result |> ignore //添加到忙碌连接表

                outputPoolStatus ()

                result :> DbConnection |> Ok
            with
            | e -> Err e

        member self.recycleConnectionAsync conn =
            Task.Run<unit>(fun _ -> (self :> IDbConnPool).recycleConnection conn)

        /// 异步从连接池取用 'ConnType
        member self.getConnectionAsync() =
            fun _ ->
                task {
                    try
                        let connTask =
                            //始终保留一部分连接以供其他用途
                            if (self :> IDbConnPool).occupancy < 0.8 then
                                match (self :> IDbConnPool).pressure with
                                | p when //池压力较小，复用连接以提升池压力系数
                                    p < 0.8
                                    ->
                                    match freeConnectionTryGet () with
                                    | Some c -> ValueTask<'ConnType>(c)
                                    | None -> genConnAsync ()

                                | _ -> //池压力较大，新建连接以降低池压力系数
                                    (fun _ ->
                                        (fun c -> freeConnectionsAdd c |> ignore)
                                        |> tryGenConn().whenCanUnwrap)
                                    |> Task.Run
                                    |> ignore

                                    match freeConnectionTryGet () with
                                    | Some c -> ValueTask<'ConnType>(c)
                                    | None -> genConnAsync ()
                            else
                                freeConnectionGetAsync ()

                        outputPoolStatus ()

                        let! conn = connTask
                        busyConnectionsTryAdd conn |> ignore //添加到忙碌连接表
                        return conn :> DbConnection |> Ok
                    with
                    | e -> return Err e
                }
            |> Task.Run<Result'<DbConnection, exn>>
(*
        member self.getConnectionAsync() =
                    Task.Run (self :> IDbConnPool).getConnection
*)
