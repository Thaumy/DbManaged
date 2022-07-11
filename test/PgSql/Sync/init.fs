module dbm_test.PgSql.Sync.init

open System
open System.Threading.Tasks
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Async
open DbManaged
open dbm_test
open dbm_test.PgSql.com

let init () =
    mkCmd().query $"drop table if exists {tab1};"
    <| always true
    |> managed().executeQuery
    |> ignore

    mkCmd()
        .query $"create table {tab1}
                 (
                     id     integer,
                     test_name varchar(256),
                     time      timestamptz,
                     content   text
                 );"
    <| always true
    |> managed().executeQuery
    |> ignore

    let as1 =
        [| for i in 1 .. 1000 ->
               fun _ ->
                   mkCmd()
                       .query $"INSERT INTO {tab1} (id, test_name, time, content)\
                                     VALUES ({i}, 'init', '{ISO8601Now()}', 'ts1_insert');"
                   <| eq 1
                   |> managed().executeQuery
               |> Task.Run |]

    let as2 =
        [| for i in 1 .. 1000 ->
               fun _ ->
                   mkCmd()
                       .query $"INSERT INTO {tab1} (id, test_name, time, content)\
                                VALUES ({i}, 'init', '{ISO8601Now()}', 'ts2_insert');"
                   <| eq 1
                   |> managed().executeQuery
               |> Task.Run |]

    for result in resultAll as1 do
        if result <> 1 then
            raise InitErrException

    for result in resultAll as2 do
        if result <> 1 then
            raise InitErrException
