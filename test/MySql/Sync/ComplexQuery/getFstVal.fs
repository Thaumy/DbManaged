module dbm_test.MySql.Sync.ComplexQuery.getFstVal

open System
open System.Threading.Tasks
open fsharper.op.Async
open DbManaged
open DbManaged.MySql

open NUnit.Framework
open dbm_test.MySql.com
open dbm_test.MySql.Sync.init
open fsharper.typ

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()


[<Test>]
let getFstVal_overload1_test () =
    let tasks =
        [| for i in 1..1000 do
               fun _ ->
                   makeCmd()
                       .getFstVal $"SELECT content FROM {tab1} WHERE id = {i};"
                   |> managed().executeQuery
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

                   makeCmd().getFstVal (sql, paras)
                   |> managed().executeQuery
               |> Task.Run<Option'<_>> |]

    for r in resultAll tasks do
        Assert.Contains(r.unwrap (), [| "ts1_insert"; "ts2_insert" |])

[<Test>]
let getFstVal_overload3_test () =

    let tasks =
        [| for i in 1..1000 do
               fun _ ->
                   makeCmd().getFstVal (tab1, "content", "id", i)
                   |> managed().executeQuery
               |> Task.Run<Option'<_>> |]

    for r in resultAll tasks do
        Assert.Contains(r.unwrap (), [| "ts1_insert"; "ts2_insert" |])
