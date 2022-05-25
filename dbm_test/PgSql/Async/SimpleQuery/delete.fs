module dbm_test.PgSql.Async.SimpleQuery.delete

open NUnit.Framework
open fsharper.typ.Ord
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open DbManaged.PgSql
open dbm_test.PgSql.com
open dbm_test.PgSql.Async.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let delete_test () =

    let query =
        mkCmd().deleteAsync ($"{tab1}", "col1", 0)
        <| eq 100
        |> managed().executeQueryAsync
        |> result

    Assert.AreEqual(100, query)

    let count =
        mkCmd()
            .getFstValAsync $"SELECT COUNT(*) FROM {tab1};"
        |> managed().executeQueryAsync
        |> result

    Assert.AreEqual(0, count)
