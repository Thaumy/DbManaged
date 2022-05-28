module dbm_test.MySql.Async.SimpleQuery.update

open NUnit.Framework
open dbm_test.MySql.com
open dbm_test.MySql.Async.init
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open DbManaged.MySql

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
