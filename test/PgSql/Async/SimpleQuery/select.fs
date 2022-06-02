module dbm_test.PgSql.Async.SimpleQuery.select

open System.Data
open System.Threading.Tasks
open fsharper.typ
open fsharper.op.Async
open DbManaged
open DbManaged.PgSql.ext.String
open NUnit.Framework
open dbm_test.PgSql.com
open dbm_test.PgSql.Async.init

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
                       .selectAsync $"SELECT test_name, content FROM {tab1} WHERE id = {i};"
                   |> managed().executeQueryAsync
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
                   let paras: (string * obj) list = [ ("id", i) ]

                   let sql =
                       normalizeSql $"SELECT test_name, content FROM {tab1} WHERE id = <id>;"

                   mkCmd().selectAsync (sql, paras)
                   |> managed().executeQueryAsync
               |> Task.Run<Option'<_>> |]

    for r in resultAll tasks do
        let table = r.unwrap ()

        Assert.AreEqual(2, table.Rows.Count)

        for row in table.Rows do
            Assert.AreEqual("init", row.["test_name"])
            Assert.Contains(row.["content"], [| "ts1_insert"; "ts2_insert" |])
