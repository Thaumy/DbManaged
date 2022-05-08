module dbm_test.PgSql.excute.executeUpdate

open NUnit.Framework
open dbm_test.PgSql
open fsharper.types
open fsharper.types.Ord
open fsharper.op.Boxing

[<OneTimeSetUp>]
let OneTimeSetUp () = com.connect ()

[<SetUp>]
let SetUp () = com.init ()

[<Test>]
let executeUpdate_overload1_test () =

    let query =
        com
            .managed
            .unwarp()
            .executeUpdate ("sch1.tab1", ("col1", 114514), ("col3", "init[001,050]"))

    Assert.AreEqual(50, query |> unwarp <| eq 50)

[<Test>]
let executeUpdate_overload2_test () =

    let query =
        com
            .managed
            .unwarp()
            .executeUpdate ("sch1.tab1", ("col1", 114514), ("col3", "init[050,100]"))

    Assert.AreEqual(50, query |> unwarp <| eq 50)
