module dbm_test.MySql.Async.init

open System
open System.Threading.Tasks
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Async
open DbManaged
open dbm_test
open dbm_test.MySql.com

let init () =
    mkCmd().queryAsync $"drop table if exists {tab1};"
    <| always true
    |> managed().executeQueryAsync
    |> wait

    mkCmd()
        .queryAsync $"create table {tab1}
                 (
                     id        int          null,
                     test_name varchar(256) null,
                     time      datetime(6)  null,
                     content   text         null
                 )"
    <| always true
    |> managed().executeQueryAsync
    |> wait

    let as1 =
        [| for i in 1..1000 ->
               fun _ ->
                   mkCmd()
                       .queryAsync $"INSERT INTO {tab1} (id, test_name, time, content)\
                                     VALUES ({i}, 'init', '{ISO8601Now()}', 'ts1_insert');"
                   <| eq 1
                   |> managed().executeQueryAsync
               |> Task.Run<int> |]

    let as2 =
        [| for i in 1..1000 ->
               fun _ ->
                   mkCmd()
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
