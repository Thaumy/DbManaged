module dbm_test.MySql.Sync.get.getFstVal

open NUnit.Framework
open dbm_test.MySql
open dbm_test.MySql.Sync.init
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Boxing
open DbManaged.MySql.ext.String

[<OneTimeSetUp>]
let OneTimeSetUp () = com.connect ()

[<SetUp>]
let SetUp () = init ()


[<Test>]
let getFstVal_overload1_test () =
    let result =
        com
            .managed
            .unwrap()
            .getFstVal $"SELECT col2 FROM {com.tab1}"
        |> unwrap2

    Assert.AreEqual("i", result)



[<Test>]
let getFstVal_overload2_test () =
    let result =
        let paras: (string * obj) list = [ ("col3", "init[050,100]") ]

        com
            .managed
            .unwrap()
            .getFstVal (normalizeSql $"SELECT col2 FROM {com.tab1} WHERE col3 = <col3>", paras)
        |> unwrap2

    Assert.AreEqual("i", result)


//overload2 is based on overload3

[<Test>]
let getFstVal_overload4_test () =
    let result =
        com
            .managed
            .unwrap()
            .getFstVal ($"{com.tab1}", "col2", ("col3", "init[050,100]"))
        |> unwrap2

    Assert.AreEqual("i", result)
