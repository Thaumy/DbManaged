module dbm_test.MySql.Sync.ComplexQuery.getFstCol

open NUnit.Framework
open fsharper.typ
open fsharper.op.Boxing
open DbManaged
open DbManaged.MySql.ext.String
open dbm_test.MySql.com
open dbm_test.MySql.Sync.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let getFstCol_overload1_test () =
    let result =
        mkCmd()
            .getFstCol $"SELECT col3 FROM {tab1} WHERE col3 = 'init[001,050]'"
        |> managed().executeQuery
        |> unwrap

    for it in result do
        Assert.AreEqual("init[001,050]", it)

[<Test>]
let getFstCol_overload2_test () =
    let result =
        let paras: (string * obj) list = [ ("col3", "init[050,100]") ]

        mkCmd()
            .getFstCol (normalizeSql $"SELECT col3 FROM {tab1} WHERE col3 = <col3>", paras)
        |> managed().executeQuery
        |> unwrap

    for it in result do
        Assert.AreEqual("init[050,100]", it)
