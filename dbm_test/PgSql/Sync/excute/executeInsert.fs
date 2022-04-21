module dbm_test.PgSql.Sync.excute.executeInsert

open NUnit.Framework
open dbm_test.PgSql
open dbm_test.PgSql.Sync.init
open fsharper.types
open fsharper.types.Ord
open fsharper.op.Boxing

[<OneTimeSetUp>]
let OneTimeSetUp () = com.connect ()

[<SetUp>]
let SetUp () =init ()

[<Test>]
let executeInsert_test () =

    for i in 1 .. 100 do
        let query =
            let pairs: (string * obj) list =
                [ ("col1", 3)
                  ("col2", "c")
                  ("col3", "ccc")
                  ("col4", "cccc") ]

            com.managed.unwarp().executeInsert "sch1.tab1" pairs

        Assert.AreEqual(1, query |> unwarp <| eq 1)

    let count =
        com
            .managed
            .unwarp()
            .getFstVal "SELECT COUNT(*) FROM sch1.tab1;"

    Assert.AreEqual(200, count |> unwarp2)
