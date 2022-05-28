module dbm_test.PgSql.Async.SimpleQuery.query

open System
open System.Threading.Tasks
open fsharper.typ.Ord
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open DbManaged.PgSql.ext.String
open NUnit.Framework
open dbm_test
open dbm_test.PgSql.com
open dbm_test.PgSql.Async.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let query_overload1_test () =

    let test_name =
        "dbm_test.PgSql.Async.SimpleQuery.query.query_overload1_test"

    let tasks =
        [| for i in 1 .. 2000 do
               fun _ ->
                   mkCmd()
                       .queryAsync $"INSERT INTO {tab1} (index, test_name, time, content)\
                                      VALUES ({i}, '{test_name}', '{ISO8601Now()}', '_');"
                   <| eq 1
                   |> managed().executeQueryAsync
               |> Task.Run<int> |]

    for r in resultAll tasks do
        Assert.AreEqual(1, r)

    let count =
        mkCmd()
            .getFstValAsync $"SELECT COUNT(*) FROM {tab1} WHERE test_name = '{test_name}';"
        |> managed().executeQueryAsync
        |> result
        |> unwrap

    Assert.AreEqual(2000, count)

[<Test>]
let query_overload2_test () =

    let test_name =
        "dbm_test.PgSql.Async.SimpleQuery.query.query_overload2_test"

    let tasks =
        [| for i in 1 .. 2000 do
               fun _ ->
                   let paras: (string * obj) list =
                       [ ("index", i)
                         ("test_name", test_name)
                         ("time", Now())
                         ("content", "_") ]

                   let sql =
                       normalizeSql
                           $"INSERT INTO {tab1} (index,   test_name,  time,  content)\
                                         VALUES (<index>,<test_name>,<time>,<content>);"

                   mkCmd().queryAsync (sql, paras) <| eq 1
                   |> managed().executeQueryAsync
               |> Task.Run<int> |]

    for r in resultAll tasks do
        Assert.AreEqual(1, r)

    let count =
        mkCmd()
            .getFstValAsync $"SELECT COUNT(*) FROM {tab1} WHERE test_name = '{test_name}';"
        |> managed().executeQueryAsync
        |> result
        |> unwrap

    Assert.AreEqual(2000, count)
