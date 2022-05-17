module dbm_test.PgSql.Async.ComplexQuery.getFstRow

open NUnit.Framework
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open DbManaged.PgSql.ext.String
open dbm_test.PgSql.com
open dbm_test.PgSql.Async.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()


[<Test>]
let getFstRow_overload1_test () =
    let result =
        mkCmd()
            .getFstRowAsync $"SELECT col1,col2 FROM {tab1}"
        |> managed().executeQueryAsync
        |> waitResult
        |> unwrap2

    Assert.AreEqual(0, result.["col1"])
    Assert.AreEqual("i", result.["col2"])

[<Test>]
let getFstRow_overload2_test () =
    let result =
        let paras: (string * obj) list = [ ("col3", "init[050,100]") ]

        mkCmd()
            .getFstRowAsync (normalizeSql $"SELECT col1,col2 FROM {tab1} WHERE col3 = <col3>", paras)
        |> managed().executeQueryAsync
        |> result
        |> unwrap2

    Assert.AreEqual(0, result.["col1"])
    Assert.AreEqual("i", result.["col2"])
