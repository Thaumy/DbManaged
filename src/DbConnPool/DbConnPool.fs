namespace DbManaged

open System
open System.Threading
open System.Data.Common
open System.Threading.Tasks
open System.Threading.Channels
open System.Collections.Concurrent
open fsharper.op
open fsharper.typ
open fsharper.op.Eq
open fsharper.op.Alias
open fsharper.op.Async
open DbManaged

/// PgSql数据库连接池
/// 对于不同的数据库，连接建立成本有所差异，应通过调节比例系数来达到最佳池性能平衡
/// d为销毁连接系数，n为新建连接系数，min为最小连接数，max为最大连接数
type internal DbConnPool
    (
        host: string,
        port: u16,
        usr: string,
        pwd: string,
        db: string,
        DbConnectionConstructor: string -> DbConnection,
        d,
        n,
        min: u32,
        max: u32
    ) as pool =

    /// 连接字符串
    let connStr =
        $"Host = {host};\
          Port = {port};\
        UserID = {usr};\
      Password = {pwd};\
      Database = {db};\
       Pooling = False;"

    let connLeft = new SemaphoreSlim(i32 max)

    /// 空闲连接表
    let freeConns =
        Channel.CreateBounded<DbConnection>(i32 max)

    /// 忙碌连接表
    let busyConns =
        ConcurrentDictionary<i32, DbConnection>(i32 max, i32 max)

    /// 尝试添加到忙碌连接表
    let busyConnsTryAdd conn =
        busyConns.TryAdd(conn.GetHashCode(), conn)

    /// 尝试从忙碌连接表移除
    let busyConnsTryRm (conn: DbConnection) = busyConns.TryRemove(conn.GetHashCode())

    /// 添加到空闲连接表
    let mutable freeConnsAddAsync = freeConns.Writer.WriteAsync

    /// 取得空闲连接
    let rec getFreeConn () =
        match freeConns.Reader.TryRead() with
        | true, c -> c
        | _ -> getFreeConn ()

    let getFreeConnAsync () = freeConns.Reader.ReadAsync()

    /// 生成新连接
    let openConn () =
        connLeft.Wait()

        let conn = DbConnectionConstructor(connStr)
        conn.Open()

        conn

    let openConnAsync () =
        task {
            let! _ = connLeft.WaitAsync()

            let conn = DbConnectionConstructor(connStr)
            let! _ = conn.OpenAsync()

            return conn
        }

    let disposeConnAsync (conn: DbConnection) : Task =
        task {
            let! _ = conn.DisposeAsync()

            connLeft.Release() |> ignore
        }

    let tryReducePressure () =
        for _ in 1 .. 2 do
            fun _ ->
                task {
                    if connLeft.CurrentCount > 0 && pool.pressure > n then
                        let! conn = openConnAsync ()
                        conn |> freeConnsAddAsync |> ignore
                }
            |> Task.RunIgnore

    do
        //建立一部分连接以满足最小连接数
        for _ in 1 .. i32 min do
            openConn () |> freeConnsAddAsync |> ignore

#if DEBUG
    let outputPoolStatus () =
        async {
            let occupancy = pool.occupancy.ToString("0.00")
            let pressure = pool.pressure.ToString("0.00")

            let free = freeConns.Reader.Count.ToString("00")
            let busy = busyConns.Count.ToString("00")

            let total =
                (max - u32 connLeft.CurrentCount).ToString("00")

            printfn $"[占用 {occupancy}: {total} /{max}] [压力 {pressure}: 忙{busy} 闲{free}]"
        }
        |> Async.Start
#endif

    /// 注销后不应进行新的查询
    member self.Dispose() =
        //对加入空闲连接表的请求进行拦截，注销要求加入的连接
        freeConnsAddAsync <- disposeConnAsync .> ValueTask

        let en =
            freeConns
                .Reader
                .ReadAllAsync()
                .GetAsyncEnumerator()

        let rec loop () = //注销空闲的连接
            if en.MoveNextAsync().AsTask().Result then
                en.Current.Dispose()
                connLeft.Release() |> ignore

                loop ()
            else
                ()

        loop ()

    member self.pressure: f64 =
        //此值只能作为一个近似值使用，因为对下列计数的访问不是互斥的
        //只要有最低连接限制（非0），算术错误就不会发生
        f64 busyConns.Count
        / (f64 max - f64 connLeft.CurrentCount) //init is 0

    member self.occupancy: f64 =
        //此值只能作为一个近似值使用，因为对下列计数的访问不是互斥的
        f64 (max - u32 connLeft.CurrentCount) / f64 max //init is 0

    member self.recycleConnAsync conn =
        tryReducePressure ()

        match busyConnsTryRm conn with
        | true, removed when refEq removed conn ->
            //从busyConns移除了连接，且被移除的连接是目标连接

            //在以下任一情况满足时，连接池需要连接
            // *池压力大于销毁阈值，这意味着需要连接以减小池压力
            // *池连接总数低于最低要求，这意味着需要连接以满足最小连接数要求
            let isNeedConn =
                self.pressure > d
                || (max - u32 connLeft.CurrentCount) < min

            if isNeedConn then
                freeConnsAddAsync conn //加入空闲连接表
            else
                disposeConnAsync conn |> ValueTask

        | true, removed ->
            (*从busyConns移除了连接，但被移除的连接不是目标连接
                这意味着哈希冲突，此时需将removed返还到忙碌连接表，同时销毁目标连接
                不进行回收的原因如下：
                *使用该连接可能进一步引发哈希冲突*)
            let success = busyConnsTryAdd removed

            if not success then //如果添加失败，则表明存在哈希冲突，被移除的连接同样需要被销毁
                disposeConnAsync removed |> ignore

            disposeConnAsync conn |> ValueTask

        | false, _ ->
            (*移除失败，这意味着下列情况之一：
                1.曾经试图将这个连接加入busyConns，但由于哈希冲突失败了
                2.这个连接根本不由连接池产生
                对于这样的连接，直接进行销毁
                不进行回收的原因如下：
                *使用该连接可能进一步引发哈希冲突
                *该连接不受连接池管制，可能引发安全性问题*)
            disposeConnAsync conn |> ValueTask

    /// 从连接池取用连接
    member self.fetchConn() =
        //用近似值估计，因为精确值带来的锁开销是没有必要的
        //满足新建阈值，尝试新建连接以降低池压力
        tryReducePressure ()

        let conn = getFreeConn ()

        //尝试加入忙碌列表
        let success = busyConnsTryAdd conn
        //如果加入失败则表明该连接与已登记连接存在哈希冲突，直接销毁
        if not success then
            disposeConnAsync conn |> ignore
            self.fetchConn () //进行下一轮尝试
        else
#if DEBUG
            outputPoolStatus ()
#endif
            conn

    /// 异步从连接池取用连接
    member self.fetchConnAsync() =
        task {
            //用近似值估计，因为精确值带来的锁开销是没有必要的
            //满足降压条件，尝试新建连接以降低池压力
            tryReducePressure ()

            let! conn = getFreeConnAsync ()

            //尝试加入忙碌列表
            let success = busyConnsTryAdd conn
            //如果加入失败则表明该连接与已登记连接存在哈希冲突，直接销毁
            if not success then
                disposeConnAsync conn |> ignore
                return! self.fetchConnAsync () //进行下一轮尝试
            else
#if DEBUG
                outputPoolStatus ()
#endif
                return conn
        }

    interface IDisposable with

        member i.Dispose() = pool.Dispose()

    interface IDbConnPool with

        member i.size = max - u32 connLeft.CurrentCount

        member i.pressure = pool.pressure

        member i.occupancy = pool.occupancy

        member i.fetchConn() = pool.fetchConn ()

        member i.fetchConnAsync() = pool.fetchConnAsync ()

        member i.recycleConnAsync conn = pool.recycleConnAsync conn
