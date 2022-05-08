module dbm_test.MySql.Async.excute.executeDelete

open NUnit.Framework
open dbm_test.MySql
open dbm_test.MySql.Async.init
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Boxing

[<OneTimeSetUp>]
let OneTimeSetUp () = com.connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let executeDelete_test () =

    let query =
        com.managed.unwrap().executeDelete $"{com.tab1}" ("col1", 0)

    Assert.AreEqual(100, query |> unwrap <| eq 100)

    let count =
        com
            .managed
            .unwrap()
            .getFstVal $"SELECT COUNT(*) FROM {com.tab1};"

    Assert.AreEqual(0, count |> unwrap2)
