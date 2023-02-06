module dbm_test.MySql.Async.SimpleQuery.update

open System
open System.Threading.Tasks
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Async
open DbManaged
open DbManaged.MySql
open NUnit.Framework
open dbm_test.MySql.com
open dbm_test.MySql.Async.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let update_overload1_test () =
    let test_name =
        "dbm_test.MySql.Async.SimpleQuery.update.update_overload1_test"

    let tasks =
        [| for i in 1..1000 do
               fun _ ->
                   makeCmd()
                       .updateAsync ($"{tab1}", ("content", test_name), ("id", i))
                   <| eq 2
                   |> managed().executeQueryAsync
               |> Task.Run<int> |]

    for r in resultAll tasks do
        Assert.AreEqual(2, r)

    let selects =
        [| for i in 1..1000 do
               fun _ ->
                   makeCmd()
                       .selectAsync $"SELECT content FROM {tab1} WHERE id = '{i}';"
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
        [| for i in 1..1000 do
               fun _ ->
                   makeCmd()
                       .updateAsync ($"{tab1}", "id", i + 114514, i)
                   <| eq 2
                   |> managed().executeQueryAsync
               |> Task.Run<int> |]

    for r in resultAll tasks do
        Assert.AreEqual(2, r)

    let selects =
        [| for i in 1..1000 do
               fun _ ->
                   makeCmd()
                       .getFstValAsync $"SELECT COUNT(*) FROM {tab1} WHERE id = '{i + 114514}';"
                   |> managed().executeQueryAsync
               |> Task.Run<Option'<_>> |]

    for r in resultAll selects do
        Assert.AreEqual(2, r.unwrap ())
