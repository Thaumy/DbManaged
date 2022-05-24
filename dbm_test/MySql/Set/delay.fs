module dbm_test.MySql.Set.delay

open System
open System.Threading
open DbManaged
open NUnit.Framework
open System.Threading.Tasks
open dbm_test.MySql.com
open dbm_test.MySql.Sync.init
open fsharper.typ
open fsharper.op.Boxing

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let delayQuery_test () =

    for i in 1 .. 1500 do
        mkCmd()
            .query $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                 VALUES (1, 'a', 'aaa', 'aaaa');"
        <| always true
        |> managed().delayQuery

    let beforeCount =
        mkCmd()
            .getFstVal ($"SELECT COUNT(*) FROM {tab1};")
        |> managed().executeQuery
        |> unwrap2

    Assert.AreEqual(100, beforeCount)

    Thread.Sleep(5000)

    let afterCount =
        mkCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE col4 = 'aaaa';"
        |> managed().executeQuery
        |> unwrap2

    Assert.AreEqual(1500, afterCount)

[<Test>]
let forceLeftDelayedQuery_test () =
    
    for i in 1 .. 1500 do
        mkCmd()
            .query $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                 VALUES (1, 'a', 'aaa', 'aaaa');"
        <| always true
        |> managed().delayQuery

    managed().forceLeftDelayedQuery ()

    let count =
        mkCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE col4 = 'aaaa';"
        |> managed().executeQuery
        |> unwrap2

    Assert.AreEqual(1500, count)
