module dbm_test.PgSql.Sync.queue.queueQuery

open DbManaged
open NUnit.Framework
open dbm_test.PgSql.com
open dbm_test.PgSql.Sync.init
open fsharper.typ
open fsharper.op.Boxing

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let queueQuery_test () =

    for i in 1 .. 1000 do
        mkCmd()
            .query $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                 VALUES (1, 'a', 'aaa', 'aaaa');"
        <| always true
        |> managed().queueQuery

    managed().forceLeftQueuedQuery ()

    let count =
        mkCmd().getFstVal $"SELECT COUNT(*) FROM {tab1};"
        |> managed().executeQuery
        |> unwrap2

    Assert.AreEqual(1100, count)
