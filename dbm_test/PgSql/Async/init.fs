module dbm_test.PgSql.Async.init

open System.Threading
open System.Threading.Tasks
open DbManaged
open DbManaged.PgSql
open dbm_test.PgSql.com
open fsharper.op.Boxing
open fsharper.types.Ord
open fsharper.types
open fsharper.op.Async
open fsharper.op.Fmt

let init () =

    managed
        .unwrap()
        .executeAnyAsync $"drop table if exists {tab1};"
    |> unwrap
    <| (fun _ -> true)
    |> wait

    managed
        .unwrap()
        .executeAnyAsync $"create table {tab1}\
             (\
                 col1 integer,\
                 col2 char,\
                 col3 varchar,\
                 col4 text\
             );"
    |> unwrap
    <| (fun _ -> true)
    |> wait

    println "INTO CONCURRENT DO"

    let ts1 =
        [| for i in 1 .. 50 do
               managed
                   .unwrap()
                   .executeAnyAsync $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                 VALUES (0, 'i', 'init[001,050]', 'initinit');"
               |> unwrap
               <| eq 1
               :> Task |]

    let ts2 =
        [| for i in 1 .. 50 do
               managed
                   .unwrap()
                   .executeAnyAsync $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                         VALUES (0, 'i', 'init[050,100]', 'initinit');"
               |> unwrap
               <| eq 1
               :> Task |]

    (*let ts3 =
        [| for i in 1 .. 500 do
               managed
                   .unwrap()
                   .executeAnyAsync $"UPDATE {tab1} SET col1 = 0 WHERE col1 = 0;"
               |> unwrap
               <| eq 1
               :> Task |]

    waitAll ts3*)
    
    waitAll ts1
    waitAll ts2


    println "EXIT CONCURRENT DO"
