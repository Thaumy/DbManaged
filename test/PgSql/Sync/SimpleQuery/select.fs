module dbm_test.PgSql.Sync.SimpleQuery.select

open System.Data
open System.Threading.Tasks
open fsharper.typ
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
let select_overload1_test () =

    let tasks =
        [| for i in 1 .. 1000 do
               fun _ ->
                   mkCmd()
                       .select $"SELECT test_name, content FROM {tab1} WHERE index = {i};"
                   |> managed().executeQuery
               |> Task.Run<Option'<_>> |]

    for r in resultAll tasks do
        let table = r.unwrap ()

        Assert.AreEqual(2, table.Rows.Count)

        for row in table.Rows do
            Assert.AreEqual("init", row.["test_name"])
            Assert.Contains(row.["content"], [| "ts1_insert"; "ts2_insert" |])


[<Test>]
let select_overload2_test () =

    let tasks =
        [| for i in 1 .. 1000 do
               fun _ ->
                   let paras: (string * obj) list = [ ("index", i) ]

                   let sql =
                       normalizeSql $"SELECT test_name, content FROM {tab1} WHERE index = <index>;"

                   mkCmd().select (sql, paras)
                   |> managed().executeQuery
               |> Task.Run<Option'<_>> |]

    for r in resultAll tasks do
        let table = r.unwrap ()

        Assert.AreEqual(2, table.Rows.Count)

        for row in table.Rows do
            Assert.AreEqual("init", row.["test_name"])
            Assert.Contains(row.["content"], [| "ts1_insert"; "ts2_insert" |])
