module dbm_test.PgSql.Sync.SimpleQuery.insert

open NUnit.Framework
open dbm_test.PgSql.com
open dbm_test.PgSql.Sync.init
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Boxing
open DbManaged
open DbManaged.PgSql

[<OneTimeSetUp>]
let OneTimeSetUp () = connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let insert_test () =

    for i in 1 .. 100 do
        let query =
            let paras: (string * obj) list =
                [ ("col1", 3)
                  ("col2", "c")
                  ("col3", "ccc")
                  ("col4", "cccc") ]

            mkCmd().insert ($"{tab1}", paras) <| eq 1
            |> managed().executeQuery
            |> unwrap

        Assert.AreEqual(1, query)

    let count =
        mkCmd().getFstVal $"SELECT COUNT(*) FROM {tab1};"
        |> managed().executeQuery
        |> unwrap2

    Assert.AreEqual(200, count)
