module dbm_test.MySql.Sync.SimpleQuery.query

open NUnit.Framework
open dbm_test.MySql.com
open dbm_test.MySql.Sync.init
open fsharper.typ.Ord
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
                .query $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                                     VALUES (1, 'a', 'aaa', 'aaaa');"
            <| eq 1
            |> managed().executeQuery

        Assert.AreEqual(1, query |> unwrap)

    let count =
        mkCmd().getFstVal $"SELECT COUNT(*) FROM {tab1};"
        |> managed().executeQuery

    Assert.AreEqual(200, count |> unwrap2)

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
            mkCmd().query (sql, paras) <| eq 1
            |> managed().executeQuery

        Assert.AreEqual(1, query |> unwrap)

    let count =
        mkCmd().getFstVal $"SELECT COUNT(*) FROM {tab1};"
        |> managed().executeQuery

    Assert.AreEqual(200, count |> unwrap2)
