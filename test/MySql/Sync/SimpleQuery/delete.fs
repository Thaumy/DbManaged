module dbm_test.MySql.Sync.SimpleQuery.delete

open System
open fsharper.typ.Ord
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open DbManaged.MySql
open NUnit.Framework
open dbm_test.MySql.com
open dbm_test.MySql.Sync.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let delete_test () =
    
    let query =
        mkCmd()
            .delete ($"{tab1}", "content", "ts1_insert")
        <| eq 1000
        |> managed().executeQuery

    Assert.AreEqual(1000, query)

    let ts1_count =
        mkCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE content = 'ts1_insert';"
        |> managed().executeQuery
        |> unwrap 

    Assert.AreEqual(0, ts1_count)

    let ts2_count =
        mkCmd()
            .getFstVal $"SELECT COUNT(*) FROM {tab1} WHERE content = 'ts2_insert';"
        |> managed().executeQuery
        |> unwrap 

    Assert.AreEqual(1000, ts2_count)
