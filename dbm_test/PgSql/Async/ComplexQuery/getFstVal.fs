module dbm_test.PgSql.Async.ComplexQuery.getFstVal

open NUnit.Framework
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open DbManaged.PgSql
open DbManaged.PgSql.ext.String
open dbm_test.PgSql.com
open dbm_test.PgSql.Async.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()


[<Test>]
let getFstVal_overload1_test () =
    let result =
        mkCmd().getFstValAsync $"SELECT col2 FROM {tab1}"
        |> managed().executeQueryAsync
        |> result
        |> unwrap

    Assert.AreEqual("i", result)

[<Test>]
let getFstVal_overload2_test () =
    let result =
        let paras: (string * obj) list = [ ("col3", "init[050,100]") ]

        mkCmd()
            .getFstValAsync (normalizeSql $"SELECT col2 FROM {tab1} WHERE col3 = <col3>", paras)
        |> managed().executeQueryAsync
        |> result
        |> unwrap

    Assert.AreEqual("i", result)

[<Test>]
let getFstVal_overload3_test () =
    let result =
        mkCmd()
            .getFstValAsync ($"{tab1}", "col2", ("col3", "init[050,100]"))
        |> managed().executeQueryAsync
        |> result
        |> unwrap

    Assert.AreEqual("i", result)
