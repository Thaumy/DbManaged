module dbm_test.PgSql.Async.SimpleQuery.delete

open System
open fsharper.typ.Ord
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open DbManaged.PgSql
open NUnit.Framework
open dbm_test.PgSql.com
open dbm_test.PgSql.Async.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let delete_test () =
    
    let query =
        makeCmd()
            .deleteAsync ($"{tab1}", "content", "ts1_insert")
        <| eq 1000
        |> managed().executeQueryAsync
        |> result

    Assert.AreEqual(1000, query)

    let ts1_count =
        makeCmd()
            .getFstValAsync $"SELECT COUNT(*) FROM {tab1} WHERE content = 'ts1_insert';"
        |> managed().executeQueryAsync
        |> result
        |> unwrap

    Assert.AreEqual(0, ts1_count)

    let ts2_count =
        makeCmd()
            .getFstValAsync $"SELECT COUNT(*) FROM {tab1} WHERE content = 'ts2_insert';"
        |> managed().executeQueryAsync
        |> result
        |> unwrap

    Assert.AreEqual(1000, ts2_count)
