module dbm_test.PgSql.Sync.get.getFstRow

open NUnit.Framework
open dbm_test.PgSql
open dbm_test.PgSql.Sync.init
open fsharper.types
open fsharper.types.Ord
open fsharper.op.Boxing

[<OneTimeSetUp>]
let OneTimeSetUp () = com.connect ()

[<SetUp>]
let SetUp () = init ()


[<Test>]
let getFstRow_overload1_test () =
    let result =
        com
            .managed
            .unwarp()
            .getFstRow "SELECT col1,col2 FROM sch1.tab1"
        |> unwarp2

    Assert.AreEqual(0, result.["col1"])
    Assert.AreEqual("i", result.["col2"])



[<Test>]
let getFstRow_overload2_test () =
    let result =
        let paras: (string * obj) list = [ ("col3", "init[050,100]") ]

        com
            .managed
            .unwarp()
            .getFstRow ("SELECT col1,col2 FROM sch1.tab1 WHERE col3 = :col3", paras)
        |> unwarp2

    Assert.AreEqual(0, result.["col1"])
    Assert.AreEqual("i", result.["col2"])


//overload2 is based on overload3
