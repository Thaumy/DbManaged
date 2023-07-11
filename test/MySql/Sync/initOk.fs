module dbm_test.MySql.Sync.initOk

open System
open fsharper.op.Boxing
open DbManaged
open NUnit.Framework
open dbm_test.MySql.com
open dbm_test.MySql.Sync.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let initOk_test () =

    let ts1_count =
        makeCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE test_name = 'init' AND content = 'ts1_insert';"
        |> managed().executeQuery
        |> unwrap

    Assert.AreEqual(1000, ts1_count)

    let ts2_count =
        makeCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE test_name = 'init' AND content = 'ts2_insert';"
        |> managed().executeQuery
        |> unwrap

    Assert.AreEqual(1000, ts2_count)
