module dbm_test.PgSql.Async.initOk

open System
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open dbm_test.PgSql.com
open dbm_test.PgSql.Async.init
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open DbManaged.PgSql.ext.String

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let initOk_test () =
   
    let ts1_count =
        mkCmd()
            .getFstValAsync $"SELECT COUNT(*) FROM {tab1} WHERE test_name = 'init' AND content = 'ts1_insert';"
        |> managed().executeQueryAsync
        |> result

    Assert.AreEqual(1000, ts1_count)

    let ts2_count =
        mkCmd()
            .getFstValAsync $"SELECT COUNT(*) FROM {tab1} WHERE test_name = 'init' AND content = 'ts2_insert';"
        |> managed().executeQueryAsync
        |> result

    Assert.AreEqual(1000, ts2_count)
