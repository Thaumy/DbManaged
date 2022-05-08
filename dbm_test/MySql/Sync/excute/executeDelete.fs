module dbm_test.MySql.Sync.excute.executeDelete

open NUnit.Framework
open dbm_test.MySql.com
open dbm_test.MySql.Sync.init
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Boxing

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let executeDelete_test () =

    let query =
        managed.unwrap().executeDelete $"{tab1}" ("col1", 0)

    Assert.AreEqual(100, query |> unwrap <| eq 100)

    let count =
        managed
            .unwrap()
            .getFstVal $"SELECT COUNT(*) FROM {tab1};"

    Assert.AreEqual(0, count |> unwrap2)
