module dbm_test.MySql.Async.ComplexQuery.getFstRow

open System
open System.Threading.Tasks
open fsharper.typ
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
let getFstRow_overload1_test () =
    let tasks =
        [| for i in 1..1000 do
               fun _ ->
                   mkCmd()
                       .getFstRowAsync $"SELECT * FROM {tab1} WHERE id = {i};"
                   |> managed().executeQueryAsync
               |> Task.Run<Option'<_>> |]

    for r in resultAll tasks do
        let row = r.unwrap ()
        Assert.Contains(row.["content"], [| "ts1_insert"; "ts2_insert" |])

[<Test>]
let getFstRow_overload2_test () =
    let tasks =
        [| for i in 1..1000 do
               fun _ ->
                   let paras: (string * obj) list =
                       [ ("id", i) ]

                   let sql =
                       managed()
                           .normalizeSql $"SELECT * FROM {tab1} WHERE id = <id>;"

                   mkCmd().getFstRowAsync (sql, paras)
                   |> managed().executeQueryAsync
               |> Task.Run<Option'<_>> |]

    for r in resultAll tasks do
        let row = r.unwrap ()
        Assert.Contains(row.["content"], [| "ts1_insert"; "ts2_insert" |])

[<Test>]
let getFstRow_overload3_test () =
    let tasks =
        [| for i in 1..1000 do
               fun _ ->
                   mkCmd().getFstRowAsync (tab1, "id", i)
                   |> managed().executeQueryAsync
               |> Task.Run<Option'<_>> |]

    for r in resultAll tasks do
        let row = r.unwrap ()
        Assert.Contains(row.["content"], [| "ts1_insert"; "ts2_insert" |])
