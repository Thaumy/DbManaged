module dbm_test.PgSql.Async.init

open System.Threading.Tasks
open fsharper.typ
open fsharper.op.Fmt
open fsharper.typ.Ord
open fsharper.op.Async
open dbm_test.PgSql.com
open DbManaged

let init () =

    mkCmd().query $"drop table if exists {tab1};"
    <| always true
    |> managed().executeQuery
    |> ignore

    mkCmd()
        .query $"create table {tab1}\
                        (\
                            col1 int         null,\
                            col2 char        null,\
                            col3 varchar(32) null,\
                            col4 text        null\
                        );"
    <| always true
    |> managed().executeQuery
    |> ignore

    println "INTO CONCURRENT DO"

    let ts1 =
        Task.Run
            (fun _ ->
                [| for i in 1 .. 50 do
                       mkCmd()
                           .queryAsync $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                                         VALUES (0, 'i', 'init[001,050]', 'initinit');"
                       <| eq 1
                       |> managed().executeQueryAsync
                       :> Task |]
                |> waitAll)

    let ts2 =
        Task.Run
            (fun _ ->
                [| for i in 1 .. 50 do
                       mkCmd()
                           .queryAsync $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                                         VALUES (0, 'i', 'init[050,100]', 'initinit');"
                       <| eq 1
                       |> managed().executeQueryAsync
                       :> Task |]
                |> waitAll)

    let ts3 =
        Task.Run
            (fun _ ->
                [| for i in 1 .. 4000 do
                       mkCmd().queryAsync $"SELECT * FROM {tab1}" <| eq 1
                       |> managed().executeQueryAsync
                       :> Task |]
                |> waitAll)

    wait ts1
    wait ts2
    wait ts3


    println "EXIT CONCURRENT DO"
