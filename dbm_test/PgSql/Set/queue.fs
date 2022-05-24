module dbm_test.PgSql.Set.queue

open System.Threading
open DbManaged
open NUnit.Framework
open dbm_test.PgSql.com
open dbm_test.PgSql.Set.init
open fsharper.typ
open fsharper.op.Boxing

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let queueQuery_test () =

    for i in 1 .. 100 do
        mkCmd()
            .query $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                 VALUES (1, 'a', 'aaa', 'aaaa');"
        <| always true
        |> managed().queueQuery

    let beforeCount =
        mkCmd()
            .getFstVal ($"SELECT COUNT(*) FROM {tab1};")
        |> managed().executeQuery
        |> unwrap2

    Assert.AreEqual(100, beforeCount)
    Thread.Sleep(200)

    let afterCount =
        mkCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE col4 = 'aaaa';"
        |> managed().executeQuery
        |> unwrap2

    Assert.AreEqual(100, afterCount)

[<Test>]
let forceLeftQueuedQuery_test () =

    for i in 1 .. 1000 do
        mkCmd()
            .query $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                 VALUES (1, 'a', 'aaa', 'aaaa');"
        <| always true
        |> managed().queueQuery

    managed().forceLeftQueuedQuery ()

    let count =
        mkCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE col4 = 'aaaa';"
        |> managed().executeQuery
        |> unwrap2

    Assert.AreEqual(1000, count)
