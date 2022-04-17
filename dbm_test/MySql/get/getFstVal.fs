module dbm_test.MySql.get.getFstVal

open NUnit.Framework
open dbm_test.MySql
open fsharper.types
open fsharper.types.Ord
open fsharper.op.Boxing

[<OneTimeSetUp>]
let OneTimeSetUp () = com.connect ()

[<SetUp>]
let SetUp () = com.init ()


[<Test>]
let getFstVal_overload1_test () =
    let result =
        com
            .managed
            .unwarp()
            .getFstVal $"SELECT col2 FROM {com.tab1}"
        |> unwarp2

    Assert.AreEqual("i", result)



[<Test>]
let getFstVal_overload2_test () =
    let result =
        let paras: (string * obj) list = [ ("col3", "init[050,100]") ]

        com
            .managed
            .unwarp()
            .getFstVal ($"SELECT col2 FROM {com.tab1} WHERE col3 = ?col3", paras)
        |> unwarp2

    Assert.AreEqual("i", result)


//overload2 is based on overload3

[<Test>]
let getFstVal_overload4_test () =
    let result =
        com
            .managed
            .unwarp()
            .getFstVal (com.tab1, "col2", ("col3", "init[050,100]"))
        |> unwarp2

    Assert.AreEqual("i", result)
