module dbm_test.MySql.Async.SimpleQuery.query

open NUnit.Framework
open dbm_test.MySql.com
open dbm_test.MySql.Async.init
open fsharper.typ.Ord
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open DbManaged.MySql.ext.String

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let query_overload1_test () =

    for i in 1 .. 100 do
        let query =
            mkCmd()
                .queryAsync $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                                     VALUES (1, 'a', 'aaa', 'aaaa');"
            <| eq 1
            |> managed().executeQueryAsync
            |> result
            |> unwrap

        Assert.AreEqual(1, query)

    let count =
        mkCmd()
            .getFstValAsync $"SELECT COUNT(*) FROM {tab1};"
        |> managed().executeQueryAsync
        |> result
        |> unwrap

    Assert.AreEqual(200, count)

open Npgsql

[<Test>]
let query_overload2_test () =

    for i in 1 .. 100 do
        let paras: (string * obj) list =
            [ ("col1", 1)
              ("col2", 'a')
              ("col3", "aaa")
              ("col4", "aaaa") ]

        let sql =
            normalizeSql
                $"INSERT INTO {tab1} ( col1,  col2,  col3,  col4)\
                              VALUES (<col1>,<col2>,<col3>,<col4>);"

        let query =
            mkCmd().queryAsync (sql, paras) <| eq 1
            |> managed().executeQueryAsync
            |> result
            |> unwrap

        Assert.AreEqual(1, query)

    let count =
        mkCmd()
            .getFstValAsync $"SELECT COUNT(*) FROM {tab1};"
        |> managed().executeQueryAsync
        |> result
        |> unwrap

    Assert.AreEqual(200, count)
