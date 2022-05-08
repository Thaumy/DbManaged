module dbm_test.MySql.Async.excute.executeInsert

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
let executeInsert_test () =

    for i in 1 .. 100 do
        let query =
            let pairs: (string * obj) list =
                [ ("col1", 3)
                  ("col2", "c")
                  ("col3", "ccc")
                  ("col4", "cccc") ]

            com.managed.unwrap().executeInsert $"{com.tab1}" pairs

        Assert.AreEqual(1, query |> unwrap <| eq 1)

    let count =
        com
            .managed
            .unwrap()
            .getFstVal $"SELECT COUNT(*) FROM {com.tab1};"

    Assert.AreEqual(200, count |> unwrap2)
