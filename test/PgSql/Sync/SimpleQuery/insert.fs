module dbm_test.PgSql.Sync.SimpleQuery.insert

open System
open System.Threading.Tasks
open fsharper.typ.Ord
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open DbManaged.PgSql
open NUnit.Framework
open dbm_test
open dbm_test.PgSql.com
open dbm_test.PgSql.Sync.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let insert_test () =

    let test_name =
        "dbm_test.PgSql.Async.SimpleQuery.insert.insert_test"

    let tasks =
        [| for i in 1 .. 2000 ->
               fun _ ->
                   let paras: (string * obj) list =
                       [ ("id", i)
                         ("test_name", test_name)
                         ("time", Now())
                         ("content", "_") ]

                   makeCmd().insert ($"{tab1}", paras) <| eq 1
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
