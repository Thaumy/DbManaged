module dbm_test.PgSql.Set.queue

open System
open System.Threading
open fsharper.typ
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
                     VALUES ({i}, '{test_name}', '{ISO8601Now()}', '_');"
        <| always true
        |> managed().queueQuery

    let beforeCount =
        mkCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE test_name = '{test_name}';"
        |> managed().executeQuery
        |> unwrap

    Assert.AreEqual(0, beforeCount)

    Thread.Sleep(2000) //wait for queue executing

    let afterCount =
        mkCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE test_name = '{test_name}';"
        |> managed().executeQuery
        |> unwrap

    Assert.AreEqual(2000, afterCount)

[<Test>]
let forceLeftQueuedQuery_test () =

    let test_name =
        "dbm_test.PgSql.Set.queue.forceLeftQueuedQuery_test"

    for i in 1 .. 2000 do
        mkCmd()
            .query $"INSERT INTO {tab1} (index, test_name, time, content)\
                     VALUES ({i}, '{test_name}', '{ISO8601Now()}', '_');"
        <| always true
        |> managed().queueQuery

    managed().forceLeftQueuedQuery ()

    let count =
        mkCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE test_name = '{test_name}';"
        |> managed().executeQuery
        |> unwrap

    Assert.AreEqual(2000, count)
