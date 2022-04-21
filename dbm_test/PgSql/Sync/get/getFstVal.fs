module dbm_test.PgSql.Sync.get.getFstVal

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
let getFstVal_overload1_test () =
    let result =
        com
            .managed
            .unwarp()
            .getFstVal "SELECT col2 FROM sch1.tab1"
        |> unwarp2

    Assert.AreEqual("i", result)



[<Test>]
let getFstVal_overload2_test () =
    let result =
        let paras: (string * obj) list = [ ("col3", "init[050,100]") ]

        com
            .managed
            .unwarp()
            .getFstVal ("SELECT col2 FROM sch1.tab1 WHERE col3 = :col3", paras)
        |> unwarp2

    Assert.AreEqual("i", result)


//overload2 is based on overload3

[<Test>]
let getFstVal_overload4_test () =
    let result =
        com
            .managed
            .unwarp()
            .getFstVal ("sch1.tab1", "col2", ("col3", "init[050,100]"))
        |> unwarp2

    Assert.AreEqual("i", result)
