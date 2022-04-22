module dbm_test.PgSql.Async.excute.executeUpdate

open NUnit.Framework
open dbm_test.PgSql
open dbm_test.PgSql.Async.init
open fsharper.types
open fsharper.types.Ord
open fsharper.op.Boxing

[<OneTimeSetUp>]
let OneTimeSetUp () = com.connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let executeUpdate_overload1_test () =

    let query =
        com
            .managed
            .unwrap()
            .executeUpdate ($"{com.tab1}", ("col1", 114514), ("col3", "init[001,050]"))

    Assert.AreEqual(50, query |> unwrap <| eq 50)

[<Test>]
let executeUpdate_overload2_test () =

    let query =
        com
            .managed
            .unwrap()
            .executeUpdate ($"{com.tab1}", ("col1", 114514), ("col3", "init[050,100]"))

    Assert.AreEqual(50, query |> unwrap <| eq 50)
