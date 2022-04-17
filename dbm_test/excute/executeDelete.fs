module dbm_test.excute.executeDelete

open NUnit.Framework
open dbm_test
open fsharper.types
open fsharper.types.Ord
open fsharper.op.Boxing

[<OneTimeSetUp>]
let OneTimeSetUp () = com.connect ()

[<SetUp>]
let SetUp () = com.init ()

[<Test>]
let executeDelete_test () =

    let query =
        com.managed.unwarp().executeDelete "sch1.tab1" ("col1", 0)

    Assert.AreEqual(100, query |> unwarp <| eq 100)

    let count =
        com
            .managed
            .unwarp()
            .getFstVal "SELECT COUNT(*) FROM sch1.tab1;"

    Assert.AreEqual(0, count |> unwarp2)
