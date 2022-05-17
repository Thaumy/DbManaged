module dbm_test.PgSql.Async.ComplexQuery.getFstCol

open NUnit.Framework
open fsharper.typ
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
let getFstCol_overload1_test () =
    let result =
        mkCmd()
            .getFstColAsync $"SELECT col3 FROM {tab1} WHERE col3 = 'init[001,050]'"
        |> managed().executeQueryAsync
        |> result
        |> unwrap

    for it in result do
        Assert.AreEqual("init[001,050]", it)

[<Test>]
let getFstCol_overload2_test () =
    let result =
        let paras: (string * obj) list = [ ("col3", "init[050,100]") ]

        mkCmd()
            .getFstColAsync (normalizeSql $"SELECT col3 FROM {tab1} WHERE col3 = <col3>", paras)
        |> managed().executeQueryAsync
        |> result
        |> unwrap

    for it in result do
        Assert.AreEqual("init[050,100]", it)
