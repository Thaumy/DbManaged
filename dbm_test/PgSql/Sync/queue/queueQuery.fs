module dbm_test.PgSql.Sync.queue.queueQuery

open DbManaged
open NUnit.Framework
open dbm_test.PgSql
open dbm_test.PgSql.Sync.init
open fsharper.op.Boxing

[<OneTimeSetUp>]
let OneTimeSetUp () = com.connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let queueQuery_executeLeftQueuedQuery_test () =
    let m: IDbQueryQueue = downcast (com.managed.unwrap ())

    for i in 1 .. 1000 do
        m.queueQuery
            $"INSERT INTO {com.tab1} (col1, col2, col3, col4)\
                 VALUES (1, 'a', 'aaa', 'aaaa');"

    m.forceLeftQueuedQuery ()

    let count =
        com
            .managed
            .unwrap()
            .getFstVal $"SELECT COUNT(*) FROM {com.tab1};"

    Assert.AreEqual(1100, count |> unwrap2)
