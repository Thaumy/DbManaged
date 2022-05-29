module dbm_test.PgSql.Set.queue

open System
open System.Threading
open System.Threading.Tasks
open fsharper.typ
open fsharper.op.Async
open fsharper.op.Coerce
open fsharper.op.Boxing
open DbManaged
open NUnit.Framework
open dbm_test
open dbm_test.PgSql.com
open dbm_test.PgSql.Set.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let queueQuery_test () =

    let test_name =
        "dbm_test.PgSql.Set.queue.queueQuery_test"

    for i in 1 .. 2000 do
        mkCmd()
            .query $"INSERT INTO {tab1} (index, test_name, time, content)\
                     VALUES ({i}, '{test_name}', CURRENT_TIMESTAMP, '_');"
        <| always true
        |> managed().queueQuery

    Thread.Sleep(3000) //wait for queue executing

    //影响行数校验
    let count =
        mkCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE test_name = '{test_name}';"
        |> managed().executeQuery
        |> unwrap

    Assert.AreEqual(2000, count)

    //执行顺序校验
    let col =
        mkCmd()
            .getFstCol $"SELECT index FROM {tab1} WHERE test_name = '{test_name}' ORDER BY time ASC;"
        |> managed().executeQuery

    let rec loop before list =
        match list with
        | [] -> true
        | x :: xs -> if x > before then loop x xs else false

    Assert.True(loop 0 (map coerce col))

[<Test>]
let forceLeftQueuedQuery_test () =

    let test_name =
        "dbm_test.PgSql.Set.queue.forceLeftQueuedQuery_test"

    fun _ ->
        for i in 1 .. 2000 do
            mkCmd()
                .query $"INSERT INTO {tab1} (index, test_name, time, content)\
                         VALUES ({i}, '{test_name}', CURRENT_TIMESTAMP, '_');"
            <| always true
            |> managed().queueQuery
    |> Task.Run
    |> wait

    managed().forceLeftQueuedQuery ()
    
    //影响行数校验
    let count =
        mkCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE test_name = '{test_name}';"
        |> managed().executeQuery
        |> unwrap

    Assert.AreEqual(2000, count)

    //执行顺序校验
    let col =
        mkCmd()
            .getFstCol $"SELECT index FROM {tab1} WHERE test_name = '{test_name}' ORDER BY time ASC;"
        |> managed().executeQuery

    let rec loop before list =
        match list with
        | [] -> true
        | x :: xs -> if x > before then loop x xs else false

    Assert.True(loop 0 (map coerce col))
