module dbm_test.PgSql.Async.excute.executeDelete

open NUnit.Framework
open dbm_test.PgSql
open dbm_test.PgSql.Async.init
open fsharper.types
open fsharper.types.Ord
open fsharper.op.Boxing

[<OneTimeSetUp>]
let OneTimeSetUp () = com.connect ()

[<SetUp>]
let SetUp () = init ()

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
