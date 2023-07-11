module dbm_test.PgSql.Sync.ComplexQuery.getFstRow

open System
open System.Threading.Tasks
open fsharper.typ
open fsharper.op.Async
open DbManaged
open DbManaged.PgSql

open NUnit.Framework
open dbm_test.PgSql.com
open dbm_test.PgSql.Sync.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let getFstRow_overload1_test () =
    let tasks =
        [| for i in 1..1000 do
               fun _ ->
                   makeCmd()
                       .getFstRow $"SELECT * FROM {tab1} WHERE id = {i};"
                   |> managed().executeQuery
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

                   makeCmd().getFstRow (sql, paras)
                   |> managed().executeQuery
               |> Task.Run<Option'<_>> |]

    for r in resultAll tasks do
        let row = r.unwrap ()
        Assert.Contains(row.["content"], [| "ts1_insert"; "ts2_insert" |])

[<Test>]
let getFstRow_overload3_test () =
    let tasks =
        [| for i in 1..1000 do
               fun _ ->
                   makeCmd().getFstRow (tab1, "id", i)
                   |> managed().executeQuery
               |> Task.Run<Option'<_>> |]

    for r in resultAll tasks do
        let row = r.unwrap ()
        Assert.Contains(row.["content"], [| "ts1_insert"; "ts2_insert" |])
