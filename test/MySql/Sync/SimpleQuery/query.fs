module dbm_test.MySql.Sync.SimpleQuery.query

open System
open System.Threading.Tasks
open fsharper.typ.Ord
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged

open NUnit.Framework
open dbm_test
open dbm_test.MySql.com
open dbm_test.MySql.Sync.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let query_overload1_test () =

    let test_name =
        "dbm_test.MySql.Async.SimpleQuery.query.query_overload1_test"

    let tasks =
        [| for i in 1..2000 do
               fun _ ->
                   makeCmd()
                       .query $"INSERT INTO {tab1} (id, test_name, time, content)\
                                      VALUES ({i}, '{test_name}', '{ISO8601Now()}', '_');"
                   <| eq 1
                   |> managed().executeQuery
               |> Task.Run<int> |]

    for r in resultAll tasks do
        Assert.AreEqual(1, r)

    let count =
        makeCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE test_name = '{test_name}';"
        |> managed().executeQuery
        |> unwrap

    Assert.AreEqual(2000, count)

[<Test>]
let query_overload2_test () =

    let test_name =
        "dbm_test.MySql.Async.SimpleQuery.query.query_overload2_test"

    let tasks =
        [| for i in 1..2000 do
               fun _ ->
                   let paras: (string * obj) list =
                       [ ("id", i)
                         ("test_name", test_name)
                         ("time", Now())
                         ("content", "_") ]

                   let sql =
                       managed()
                           .normalizeSql $"INSERT INTO {tab1} (id,   test_name,  time,  content)\
                                         VALUES (<id>,<test_name>,<time>,<content>);"

                   makeCmd().query (sql, paras) <| eq 1
                   |> managed().executeQuery
               |> Task.Run<int> |]

    for r in resultAll tasks do
        Assert.AreEqual(1, r)

    let count =
        makeCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE test_name = '{test_name}';"
        |> managed().executeQuery
        |> unwrap

    Assert.AreEqual(2000, count)
