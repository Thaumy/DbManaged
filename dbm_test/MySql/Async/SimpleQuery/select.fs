module dbm_test.MySql.Async.SimpleQuery.select

open System.Data
open NUnit.Framework
open dbm_test.MySql.com
open dbm_test.MySql.Async.init
open fsharper.typ
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open DbManaged.MySql
open DbManaged.MySql.ext.String

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()


[<Test>]
let select_overload1_test () =

        let table =
            mkCmd()
                .selectAsync $"SELECT col1,col2 FROM {tab1}"
            |> managed().executeQueryAsync
            |> result
            |> unwrap2

        for row in table.Rows do
            Assert.AreEqual(0, row.["col1"])
            Assert.AreEqual("i", row.["col2"])
    

[<Test>]
let select_overload2_test () =
    let table =
        let paras: (string * obj) list = [ ("col3", "init[050,100]") ]

        mkCmd()
            .selectAsync (normalizeSql $"SELECT col1,col2 FROM {tab1} WHERE col3 = <col3>", paras)
        |> managed().executeQueryAsync
        |> result
        |> unwrap2

    for row in table.Rows do
        Assert.AreEqual(0, row.["col1"])
        Assert.AreEqual("i", row.["col2"])
