module dbm_test.MySql.Async.ComplexQuery.getFstVal

open NUnit.Framework
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Boxing
open DbManaged
open DbManaged.MySql
open DbManaged.MySql.ext.String
open dbm_test.MySql.com
open dbm_test.MySql.Async.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()


[<Test>]
let getFstVal_overload1_test () =
    let result =
        mkCmd().getFstVal $"SELECT col2 FROM {tab1}"
        |> managed().executeQuery
        |> unwrap2

    Assert.AreEqual("i", result)

[<Test>]
let getFstVal_overload2_test () =
    let result =
        let paras: (string * obj) list = [ ("col3", "init[050,100]") ]

        mkCmd()
            .getFstVal (normalizeSql $"SELECT col2 FROM {tab1} WHERE col3 = <col3>", paras)
        |> managed().executeQuery
        |> unwrap2

    Assert.AreEqual("i", result)

[<Test>]
let getFstVal_overload3_test () =
    let result =
        mkCmd()
            .getFstVal ($"{tab1}", "col2", ("col3", "init[050,100]"))
        |> managed().executeQuery
        |> unwrap2

    Assert.AreEqual("i", result)
