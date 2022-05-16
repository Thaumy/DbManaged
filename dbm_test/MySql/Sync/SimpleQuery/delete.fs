module dbm_test.MySql.Sync.SimpleQuery.delete

open NUnit.Framework
open fsharper.typ.Ord
open fsharper.op.Boxing
open DbManaged
open DbManaged.MySql
open dbm_test.MySql.com
open dbm_test.MySql.Sync.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let delete_test () =

    let query =
        mkCmd().delete $"{tab1}" ("col1", 0) <| eq 100
        |> managed().executeQuery

    Assert.AreEqual(100, query |> unwrap)

    let count =
        mkCmd().getFstVal $"SELECT COUNT(*) FROM {tab1};"
        |> managed().executeQuery

    Assert.AreEqual(0, count |> unwrap2)
