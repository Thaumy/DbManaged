module dbm_test.PgSql.Set.delay

open System
open System.Threading
open System.Threading.Tasks
open fsharper.typ
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open NUnit.Framework
open dbm_test
open dbm_test.PgSql.com
open dbm_test.PgSql.Set.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = initWithDelay ()

[<Test>]
let delayQuery_test () =
    //触发执行
    [| for i in 1 .. 10000 ->
           fun _ ->
               mkCmd().queryAsync $"SELECT {i}" <| always true
               |> managed().executeQueryAsync
           |> Task.Run<int> |]
    |> resultAll
    |> ignore

    let afterCount =
        mkCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE content = 'init_with_delay';"
        |> managed().executeQuery
        |> unwrap

    Assert.AreEqual(100, afterCount)

[<Test>]
let forceLeftDelayedQuery_test () =

    let test_name =
        "dbm_test.PgSql.Set.delay.forceLeftDelayedQuery_test"

    for i in 1 .. 2000 do
        mkCmd()
            .query $"INSERT INTO {tab1} (id, test_name, time, content)\
                     VALUES ({i}, '{test_name}', '{ISO8601Now()}', '_');"
        <| always true
        |> managed().delayQuery

    managed().forceLeftDelayedQuery ()

    let count =
        mkCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE test_name = '{test_name}';"
        |> managed().executeQuery
        |> unwrap

    Assert.AreEqual(2000, count)
