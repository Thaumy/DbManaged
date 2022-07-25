module dbm_test.MySql.Async.ComplexQuery.getFstVal

open System
open System.Threading.Tasks
open fsharper.op.Async
open DbManaged
open DbManaged.MySql

open NUnit.Framework
open dbm_test.MySql.com
open dbm_test.MySql.Async.init
open fsharper.typ
open fsharper.typ.Option'

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()


[<Test>]
let getFstVal_overload1_test () =
    let tasks =
        [| for i in 1..1000 do
               fun _ ->
                   mkCmd()
                       .getFstValAsync $"SELECT content FROM {tab1} WHERE id = {i};"
                   |> managed().executeQueryAsync
               |> Task.Run<Option'<_>> |]

    for r in resultAll tasks do
        Assert.Contains(r.unwrap (), [| "ts1_insert"; "ts2_insert" |])

[<Test>]
let getFstVal_overload2_test () =
    let tasks =
        [| for i in 1..1000 do
               fun _ ->
                   let paras: (string * obj) list =
                       [ ("id", i) ]

                   let sql =
                       managed()
                           .normalizeSql $"SELECT content FROM {tab1} WHERE id = <id>;"

                   mkCmd().getFstValAsync (sql, paras)
                   |> managed().executeQueryAsync
               |> Task.Run<Option'<_>> |]

    for r in resultAll tasks do
        Assert.Contains(r.unwrap (), [| "ts1_insert"; "ts2_insert" |])

[<Test>]
let getFstVal_overload3_test () =

    let tasks =
        [| for i in 1..1000 do
               fun _ ->
                   mkCmd().getFstValAsync (tab1, "content", "id", i)
                   |> managed().executeQueryAsync
               |> Task.Run<Option'<_>> |]

    for r in resultAll tasks do
        Assert.Contains(r.unwrap (), [| "ts1_insert"; "ts2_insert" |])
