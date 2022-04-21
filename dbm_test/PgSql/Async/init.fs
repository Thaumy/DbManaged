module dbm_test.PgSql.Async.init

open System.Threading
open DbManaged
open DbManaged.PgSql
open dbm_test.PgSql.com
open fsharper.op.Boxing
open fsharper.types.Ord
open fsharper.types
open fsharper.op.Async

let init () =

    managed
        .unwarp()
        .executeAnyAsync "drop table if exists sch1.tab1;"
    |> unwarp
    <| (fun _ -> true)
    |> wait

    managed
        .unwarp()
        .executeAnyAsync $"create table {tab1}\
             (\
                 col1 integer,\
                 col2 char,\
                 col3 varchar,\
                 col4 text\
             );"
    |> unwarp
    <| (fun _ -> true)
    |> wait

    for i in 1 .. 50 do
        managed
            .unwarp()
            .executeAnyAsync $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                 VALUES (0, 'i', 'init[001,050]', 'initinit');"
        |> unwarp
        <| eq 1
        |> wait

    for i in 1 .. 50 do
        managed
            .unwarp()
            .executeAnyAsync $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                 VALUES (0, 'i', 'init[050,100]', 'initinit');"
        |> unwarp
        <| eq 1
        |> ignore
