module dbm_test.PgSql.Async.SimpleQuery.select

open System.Data
open NUnit.Framework
open dbm_test.PgSql.com
open dbm_test.PgSql.Async.init
open fsharper.typ
open fsharper.op.Boxing
open DbManaged
open DbManaged.PgSql
open DbManaged.PgSql.ext.String

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()


[<Test>]
let select_overload1_test () =
    let result =
        
        mkCmd().select $"SELECT col1,col2 FROM {tab1}"
        |>managed().executeQuery
        |>unwrap

    for row in result.Rows do
        Assert.AreEqual(0, row.["col1"])
        Assert.AreEqual("i", row.["col2"])

[<Test>]
let select_overload2_test () =
    let result =
        let paras: (string * obj) list = [ ("col3", "init[050,100]") ]

        mkCmd().select (normalizeSql $"SELECT col1,col2 FROM {tab1} WHERE col3 = <col3>", paras)
        |>managed().executeQuery
        |> unwrap

    for row in result.Rows do
        Assert.AreEqual(0, row.["col1"])
        Assert.AreEqual("i", row.["col2"])
