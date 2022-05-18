module dbm_test.PgSql.Async.SimpleQuery.update

open NUnit.Framework
open dbm_test.PgSql.com
open dbm_test.PgSql.Async.init
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open DbManaged.PgSql

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let update_overload1_test () =

    let query =
        mkCmd()
            .updateAsync ($"{tab1}", ("col1", 114514), ("col3", "init[001,050]"))
        <| eq 50
        |> managed().executeQueryAsync
        |> result
        |> unwrap

    Assert.AreEqual(50, query)

[<Test>]
let update_overload2_test () =

    let query =
        mkCmd()
            .updateAsync ($"{tab1}", ("col1", 114514), ("col3", "init[050,100]"))
        <| eq 50
        |> managed().executeQueryAsync
        |> result
        |> unwrap

    Assert.AreEqual(50, query)
