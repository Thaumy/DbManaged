module dbm_test.PgSql.com

open DbManaged
open DbManaged.PgSql
open fsharper.op.Boxing
open fsharper.types.Ord
open fsharper.types

let tab1 = "sch1.tab1"
let mutable msg = None
let mutable managed: Option'<IDbManaged> = None

let connect () =
    match msg with
    | None ->
        msg <-
            Some
                { Host = "localhost"
                  Port = 5432us
                  User = "postgres"
                  Password = "65a1561425f744e2b541303f628963f8" }
    | _ -> ()

    match managed with
    | None -> managed <- Some <| PgSqlManaged(unwarp msg, "dbm_test", 32u)
    | _ -> ()

let init () =

    managed
        .unwarp()
        .executeAny "drop table if exists sch1.tab1;"
    |> unwarp
    <| (fun _ -> true)
    |> ignore

    managed
        .unwarp()
        .executeAny $"create table {tab1}\
             (\
                 col1 integer,\
                 col2 char,\
                 col3 varchar,\
                 col4 text\
             );"
    |> unwarp
    <| (fun _ -> true)
    |> ignore

    for _ in 1 .. 50 do
        managed
            .unwarp()
            .executeAny $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                 VALUES (0, 'i', 'init[001,050]', 'initinit');"
        |> unwarp
        <| eq 1
        |> ignore

    for _ in 1 .. 50 do
        managed
            .unwarp()
            .executeAny $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                 VALUES (0, 'i', 'init[050,100]', 'initinit');"
        |> unwarp
        <| eq 1
        |> ignore
