module dbm_test.MySql.com

open DbManaged
open DbManaged.MySql
open fsharper.op.Boxing
open fsharper.types.Ord
open fsharper.types

let tab1 = "tab1"
let mutable msg = None
let mutable managed: Option'<IDbManaged> = None

let connect () =
    msg <-
        Some
            { Host = "localhost"
              Port = 3306us
              User = "root"
              Password = "65a1561425f744e2b541303f628963f8" }

    managed <- Some <| MySqlManaged(unwarp msg, "dbm_test")

let init () =

    managed
        .unwarp()
        .executeAny "drop table if exists tab1;"
    |> unwarp
    <| (fun _ -> true)
    |> ignore

    managed
        .unwarp()
        .executeAny "create table tab1\
                     (\
                         col1 int         null,\
                         col2 char        null,\
                         col3 varchar(32) null,\
                         col4 text        null\
                     );"
    |> unwarp
    <| (fun _ -> true)
    |> ignore

    for _ in 1 .. 50 do
        managed
            .unwarp()
            .executeAny "INSERT INTO tab1 (col1, col2, col3, col4)\
                 VALUES (0, 'i', 'init[001,050]', 'initinit');"
        |> unwarp
        <| eq 1
        |> ignore

    for _ in 1 .. 50 do
        managed
            .unwarp()
            .executeAny "INSERT INTO tab1 (col1, col2, col3, col4)\
                 VALUES (0, 'i', 'init[050,100]', 'initinit');"
        |> unwarp
        <| eq 1
        |> ignore
