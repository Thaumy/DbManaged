module dbm_test.MySql.Set.queue

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
open dbm_test.MySql.com
open dbm_test.MySql.Set.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = initNormal ()

[<Test>]
let queueQuery_test () =
    let queuedQueries =
        [| for i in 1 .. 2000 do
               mkCmd()
                   .query $"INSERT INTO {tab1} (id, test_name, time, content)\
                     VALUES ({i}, 'init_with_queue', NOW(), 'init_with_queue');"
               <| always true
               |> managed().queueQuery |]

    for q in queuedQueries do
        let rec loop () =
            if not q.IsCompleted then
                [| for i in 1 .. 100 do
                       fun _ ->
                           mkCmd().queryAsync $"SELECT {i}" <| always true
                           |> managed().executeQueryAsync
                       |> Task.Run<int> |]
                |> resultAll
                |> ignore
                |> loop

        loop ()

    for r in queuedQueries |> resultAll do
        Assert.AreEqual(1, r)

    //影响行数校验
    let count =
        mkCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE content = 'init_with_queue';"
        |> managed().executeQuery
        |> unwrap

    Assert.AreEqual(2000, count)

    //执行顺序校验
    let col =
        mkCmd()
            .getFstCol $"SELECT id FROM {tab1} WHERE content = 'init_with_queue' ORDER BY time ASC;"
        |> managed().executeQuery

    let rec loop before list =
        match list with
        | [] -> true
        | x :: xs -> if x > before then loop x xs else false

    Assert.True(loop 0 (map coerce col))

[<Test>]
let forceLeftQueuedQuery_test () =

    let test_name =
        "dbm_test.MySql.Set.queue.forceLeftQueuedQuery_test"

    let queuedQueries =
        [| for i in 1 .. 2000 do
               mkCmd()
                   .query $"INSERT INTO {tab1} (id, test_name, time, content)\
                     VALUES ({i}, '{test_name}', NOW(), '_');"
               <| always true
               |> managed().queueQuery |]

    managed().forceLeftQueuedQuery ()

    for r in queuedQueries |> resultAll do
        Assert.AreEqual(1, r)

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
            .getFstCol $"SELECT id FROM {tab1} WHERE test_name = '{test_name}' ORDER BY time ASC;"
        |> managed().executeQuery

    let rec loop before list =
        match list with
        | [] -> true
        | x :: xs -> if x > before then loop x xs else false

    Assert.True(loop 0 (map coerce col))
