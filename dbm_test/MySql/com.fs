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
    match msg with
    | None ->
        msg <-
            Some
                { Host = "localhost"
                  Port = 3306us
                  User = "root"
                  Password = "65a1561425f744e2b541303f628963f8" }
    | _ -> ()

    match managed with
    | None -> managed <- Some <| MySqlManaged(unwrap msg, "dbm_test", 32u)
    | _ -> ()

let init () =

    managed
        .unwrap()
        .executeAny "drop table if exists tab1;"
    |> unwrap
    <| (fun _ -> true)
    |> ignore

    managed
        .unwrap()
        .executeAny "create table tab1\
                     (\
                         col1 int         null,\
                         col2 char        null,\
                         col3 varchar(32) null,\
                         col4 text        null\
                     );"
    |> unwrap
    <| (fun _ -> true)
    |> ignore

    for _ in 1 .. 50 do
        managed
            .unwrap()
            .executeAny $"INSERT INTO tab1 (col1, col2, col3, col4)\
                 VALUES (0, 'i', 'init[001,050]', 'initinit');"
        |> unwrap
        <| eq 1
        |> ignore

    for _ in 1 .. 50 do
        managed
            .unwrap()
            .executeAny $"INSERT INTO tab1 (col1, col2, col3, col4)\
                 VALUES (0, 'i', 'init[050,100]', 'initinit');"
        |> unwrap
        <| eq 1
        |> ignore
