module dbm_test.PgSql.Async.SimpleQuery.update

open System
open System.Threading.Tasks
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Async
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
let update_overload1_test () =
    let test_name =
        "dbm_test.PgSql.Async.SimpleQuery.update.update_overload1_test"

    let tasks =
        [| for i in 1 .. 1000 do
               fun _ ->
                   mkCmd()
                       .updateAsync ($"{tab1}", ("content", test_name), ("index", i))
                   <| eq 2
                   |> managed().executeQueryAsync
               |> Task.Run<int> |]

    for r in resultAll tasks do
        Assert.AreEqual(2, r)

    let selects =
        [| for i in 1 .. 1000 do
               fun _ ->
                   mkCmd()
                       .selectAsync $"SELECT content FROM {tab1} WHERE index = '{i}';"
                   |> managed().executeQueryAsync
               |> Task.Run<Option'<_>> |]

    for r in resultAll selects do
        let table = r.unwrap ()

        Assert.AreEqual(2, table.Rows.Count)

        for row in table.Rows do
            Assert.AreEqual(test_name, row.["content"])

[<Test>]
let update_overload2_test () =

    let tasks =
        [| for i in 1 .. 1000 do
               fun _ ->
                   mkCmd()
                       .updateAsync ($"{tab1}", "index", i + 114514, i)
                   <| eq 2
                   |> managed().executeQueryAsync
               |> Task.Run<int> |]

    for r in resultAll tasks do
        Assert.AreEqual(2, r)

    let selects =
        [| for i in 1 .. 1000 do
               fun _ ->
                   mkCmd()
                       .getFstValAsync $"SELECT COUNT(*) FROM {tab1} WHERE index = '{i + 114514}';"
                   |> managed().executeQueryAsync
               |> Task.Run<Option'<_>> |]

    for r in resultAll selects do
        Assert.AreEqual(2, r.unwrap())
