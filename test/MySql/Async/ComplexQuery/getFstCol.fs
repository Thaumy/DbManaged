module dbm_test.MySql.Async.ComplexQuery.getFstCol

open System
open System.Threading.Tasks
open fsharper.op.Async
open DbManaged

open NUnit.Framework
open dbm_test.MySql.com
open dbm_test.MySql.Async.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let getFstCol_overload1_test () =
    let tasks =
        [| for i in 1..1000 do
               fun _ ->
                   mkCmd()
                       .getFstColAsync $"SELECT content FROM {tab1} WHERE id = {i};"
                   |> managed().executeQueryAsync
               |> Task.Run<obj list> |]

    for r in resultAll tasks do

        Assert.AreEqual(2, r.Length)

        for it in r do
            Assert.Contains(it, [| "ts1_insert"; "ts2_insert" |])

[<Test>]
let getFstCol_overload2_test () =

    let tasks =
        [| for i in 1..1000 do
               fun _ ->
                   let paras: (string * obj) list =
                       [ ("id", i) ]

                   let sql =
                       managed()
                           .normalizeSql $"SELECT content FROM {tab1} WHERE id = <id>;"

                   mkCmd().getFstColAsync (sql, paras)
                   |> managed().executeQueryAsync
               |> Task.Run<obj list> |]

    for r in resultAll tasks do

        Assert.AreEqual(2, r.Length)

        for it in r do
            Assert.Contains(it, [| "ts1_insert"; "ts2_insert" |])
