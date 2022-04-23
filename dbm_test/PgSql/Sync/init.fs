module dbm_test.PgSql.Sync.init

open DbManaged
open DbManaged.PgSql
open dbm_test.PgSql.com
open fsharper.op.Boxing
open fsharper.types.Ord
open fsharper.types
open fsharper.op.Fmt


let init () =

    managed
        .unwrap()
        .executeAny $"drop table if exists {tab1};"
    |> unwrap
    <| (fun _ -> true)
    |> ignore

    managed
        .unwrap()
        .executeAny $"create table {tab1}\
             (\
                 col1 integer,\
                 col2 char,\
                 col3 varchar,\
                 col4 text\
             );"
    |> unwrap
    <| (fun _ -> true)
    |> ignore

    for _ in 1 .. 50 do
        managed
            .unwrap()
            .executeAny $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                 VALUES (0, 'i', 'init[001,050]', 'initinit');"
        |> unwrap
        <| eq 1
        |> ignore

    for _ in 1 .. 50 do
        managed
            .unwrap()
            .executeAny $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                 VALUES (0, 'i', 'init[050,100]', 'initinit');"
        |> unwrap
        <| eq 1
        |> ignore
