module dbm_test.PgSql.Sync.ComplexQuery.getFstCol

open System
open System.Threading.Tasks
open fsharper.op.Async
open fsharper.op.Boxing
open DbManaged
open DbManaged.PgSql.ext.String
open NUnit.Framework
open dbm_test.PgSql.com
open dbm_test.PgSql.Sync.init

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let getFstCol_overload1_test () =
    let tasks =
        [| for i in 1 .. 1000 do
               fun _ ->
                   mkCmd()
                       .getFstCol $"SELECT content FROM {tab1} WHERE index = {i};"
                   |> managed().executeQuery
               |> Task.Run<obj list> |]

    for r in resultAll tasks do
        Assert.AreEqual(2, r.Length)

        for it in r do
            Assert.Contains(it, [| "ts1_insert"; "ts2_insert" |])

[<Test>]
let getFstCol_overload2_test () =

    let tasks =
        [| for i in 1 .. 1000 do
               fun _ ->
                   let paras: (string * obj) list = [ ("index", i) ]

                   let sql =
                       normalizeSql $"SELECT content FROM {tab1} WHERE index = <index>;"

                   mkCmd().getFstCol (sql, paras)
                   |> managed().executeQuery
               |> Task.Run<obj list> |]

    for r in resultAll tasks do

        Assert.AreEqual(2, r.Length)

        for it in r do
            Assert.Contains(it, [| "ts1_insert"; "ts2_insert" |])
