module dbm_test.MySql.Set.delay

open System
open System.Threading
open System.Threading.Tasks
open fsharper.typ
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open NUnit.Framework
open dbm_test
open dbm_test.MySql.com
open dbm_test.MySql.Set.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = initNormal ()

[<Test>]
let delayQuery_test () =
    let delayedQueries =
        [| for i in 1..2000 do
               makeCmd()
                   .query $"INSERT INTO {tab1} (id, test_name, time, content)\
                     VALUES ({i}, 'init_with_delay', '{ISO8601Now()}', 'init_with_delay');"
               <| always true
               |> managed().delayQuery |]

    for q in delayedQueries do
        let rec loop () =
            if not q.IsCompleted then
                [| for i in 1..100 do
                       fun _ ->
                           makeCmd().queryAsync $"SELECT {i}" <| always true
                           |> managed().executeQueryAsync
                       |> Task.Run<int> |]
                |> resultAll
                |> ignore
                |> loop

        loop ()

    for r in delayedQueries |> resultAll do
        Assert.AreEqual(1, r)

    let afterCount =
        makeCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE content = 'init_with_delay';"
        |> managed().executeQuery
        |> unwrap

    Assert.AreEqual(2000, afterCount)

[<Test>]
let forceLeftDelayedQuery_test () =

    let test_name =
        "dbm_test.MySql.Set.delay.forceLeftDelayedQuery_test"

    let delayedQueries =
        [| for i in 1..2000 do
               makeCmd()
                   .query $"INSERT INTO {tab1} (id, test_name, time, content)\
                     VALUES ({i}, '{test_name}', '{ISO8601Now()}', '_');"
               <| always true
               |> managed().delayQuery |]

    managed().forceLeftDelayedQuery ()

    for r in delayedQueries |> resultAll do
        Assert.AreEqual(1, r)

    let count =
        makeCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE test_name = '{test_name}';"
        |> managed().executeQuery
        |> unwrap

    Assert.AreEqual(2000, count)
