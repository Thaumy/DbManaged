module dbm_test.PgSql.Sync.get.executeSelect

open NUnit.Framework
open dbm_test.PgSql
open dbm_test.PgSql.Sync.init
open fsharper.types
open fsharper.types.Ord
open fsharper.op.Boxing
open DbManaged.PgSql.ext.String

[<OneTimeSetUp>]
let OneTimeSetUp () = com.connect ()

[<SetUp>]
let SetUp () = init ()


[<Test>]
let executeSelect_overload1_test () =
    let result =
        com
            .managed
            .unwrap()
            .executeSelect $"SELECT col1,col2 FROM {com.tab1}"
        |> unwrap

    for row in result.Rows do
        Assert.AreEqual(0, row.["col1"])
        Assert.AreEqual("i", row.["col2"])

[<Test>]
let executeSelect_overload2_test () =
    let result =
        let paras: (string * obj) list = [ ("col3", "init[050,100]") ]

        com
            .managed
            .unwrap()
            .executeSelect (normalizeSql $"SELECT col1,col2 FROM {com.tab1} WHERE col3 = <col3>", paras)
        |> unwrap

    for row in result.Rows do
        Assert.AreEqual(0, row.["col1"])
        Assert.AreEqual("i", row.["col2"])

//overload2 is based on overload3
