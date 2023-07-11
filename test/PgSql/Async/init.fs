module dbm_test.PgSql.Async.init

open System
open System.Threading.Tasks
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Async
open DbManaged
open dbm_test
open dbm_test.PgSql.com

let init () =
    makeCmd().queryAsync $"drop table if exists {tab1};"
    <| always true
    |> managed().executeQueryAsync
    |> wait

    makeCmd()
        .queryAsync $"create table {tab1}
                 (
                     id     integer,
                     test_name varchar(256),
                     time      timestamptz,
                     content   text
                 );"
    <| always true
    |> managed().executeQueryAsync
    |> wait

    let as1 =
        [| for i in 1 .. 1000 ->
               fun _ ->
                   makeCmd()
                       .queryAsync $"INSERT INTO {tab1} (id, test_name, time, content)\
                                     VALUES ({i}, 'init', '{ISO8601Now()}', 'ts1_insert');"
                   <| eq 1
                   |> managed().executeQueryAsync
               |> Task.Run<int> |]

    let as2 =
        [| for i in 1 .. 1000 ->
               fun _ ->
                   makeCmd()
                       .queryAsync $"INSERT INTO {tab1} (id, test_name, time, content)\
                                     VALUES ({i}, 'init', '{ISO8601Now()}', 'ts2_insert');"
                   <| eq 1
                   |> managed().executeQueryAsync
               |> Task.Run<int> |]

    for result in resultAll as1 do
        if result <> 1 then
            raise InitErrException

    for result in resultAll as2 do
        if result <> 1 then
            raise InitErrException
