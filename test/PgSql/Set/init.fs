module dbm_test.PgSql.Set.init

open System
open System.Threading.Tasks
open fsharper.typ
open fsharper.typ.Ord
open fsharper.op.Async
open DbManaged
open dbm_test
open dbm_test.PgSql.com

let ddl_prepare () =
    
    makeCmd().query $"drop table if exists {tab1};"
    <| always true
    |> managed().executeQuery
    |> ignore

    makeCmd()
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

let dml_prepare () =

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

let initNormal () =

    ddl_prepare ()
    dml_prepare ()

(*
let initWithQueue () =
    
    ddl_prepare ()

    for i in 1 .. 100 do
        makeCmd()
            .query $"INSERT INTO {tab1} (id, test_name, time, content)\
                     VALUES ({i}, 'init_with_queue', CURRENT_TIMESTAMP, 'init_with_queue');"
        <| always true
        |> managed().queueQuery

    dml_prepare ()

let initWithDelay () =
    
    ddl_prepare ()

    for i in 1 .. 100 do
        makeCmd()
            .query $"INSERT INTO {tab1} (id, test_name, time, content)\
                     VALUES ({i}, 'init_with_delay', '{ISO8601Now()}', 'init_with_delay');"
        <| always true
        |> managed().delayQuery

    dml_prepare ()
*)