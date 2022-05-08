module dbm_test.MySql.execute.execute

open NUnit.Framework
open dbm_test.MySql
open fsharper.types
open fsharper.types.Ord
open fsharper.op.Boxing

[<OneTimeSetUp>]
let OneTimeSetUp () = com.connect ()

[<SetUp>]
let SetUp () = com.init ()

[<Test>]
let executeAny_overload1_test () =

    for i in 1 .. 100 do
        let query =
            com
                .managed
                .unwarp()
                .executeAny $"INSERT INTO {com.tab1} (col1, col2, col3, col4)\
                 VALUES (1, 'a', 'aaa', 'aaaa');"

        Assert.AreEqual(1, query |> unwarp <| eq 1)

    let count =
        com
            .managed
            .unwarp()
            .getFstVal $"SELECT COUNT(*) FROM {com.tab1};"

    Assert.AreEqual(200, count |> unwarp2)

open Npgsql

[<Test>]
let executeAny_overload2_test () =

    for i in 1 .. 100 do
        let paras: (string * obj) list =
            [ ("col1", 1)
              ("col2", 'a')
              ("col3", "aaa")
              ("col4", "aaaa") ]

        let query =
            com
                .managed
                .unwarp()
                .executeAny (
                    $"INSERT INTO {com.tab1} ( col1,  col2,  col3,  col4)\
                                      VALUES (?col1, ?col2, ?col3, ?col4);",
                    paras
                )

        Assert.AreEqual(1, query |> unwarp <| eq 1)

    let count =
        com
            .managed
            .unwarp()
            .getFstVal $"SELECT COUNT(*) FROM {com.tab1};"

    Assert.AreEqual(200, count |> unwarp2)
