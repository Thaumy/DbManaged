module dbm_test.MySql.Async.execute.execute

open NUnit.Framework
open dbm_test.MySql
open dbm_test.MySql.Async.init
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Boxing
open DbManaged.MySql.ext.String

[<OneTimeSetUp>]
let OneTimeSetUp () = com.connect ()

[<SetUp>]
let SetUp () = init ()

[<Test>]
let executeAny_overload1_test () =

    for i in 1 .. 100 do
        let query =
            com
                .managed
                .unwrap()
                .executeAny $"INSERT INTO {com.tab1} (col1, col2, col3, col4)\
                 VALUES (1, 'a', 'aaa', 'aaaa');"

        Assert.AreEqual(1, query |> unwrap <| eq 1)

    let count =
        com
            .managed
            .unwrap()
            .getFstVal $"SELECT COUNT(*) FROM {com.tab1};"

    Assert.AreEqual(200, count |> unwrap2)

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
                .unwrap()
                .executeAny (
                    normalizeSql
                        $"INSERT INTO {com.tab1} ( col1,  col2,  col3,  col4)\
                                      VALUES (<col1>,<col2>,<col3>,<col4>);",
                    paras
                )

        Assert.AreEqual(1, query |> unwrap <| eq 1)

    let count =
        com
            .managed
            .unwrap()
            .getFstVal $"SELECT COUNT(*) FROM {com.tab1};"

    Assert.AreEqual(200, count |> unwrap2)
