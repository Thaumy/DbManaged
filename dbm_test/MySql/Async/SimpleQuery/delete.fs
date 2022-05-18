module dbm_test.MySql.Async.SimpleQuery.delete

open NUnit.Framework
open fsharper.typ.Ord
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open DbManaged.MySql
open dbm_test.MySql.com
open dbm_test.MySql.Async.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let delete_test () =

    let query =
        mkCmd().deleteAsync $"{tab1}" ("col1", 0)
        <| eq 100
        |> managed().executeQueryAsync
        |> result
        |> unwrap

    Assert.AreEqual(100, query)

    let count =
        mkCmd()
            .getFstValAsync $"SELECT COUNT(*) FROM {tab1};"
        |> managed().executeQueryAsync
        |> result
        |> unwrap

    Assert.AreEqual(0, count)
