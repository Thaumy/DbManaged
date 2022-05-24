module dbm_test.PgSql.Set.init

open DbManaged
open dbm_test.PgSql.com
open fsharper.typ.Ord
open fsharper.typ

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

    for _ in 1 .. 50 do
        mkCmd()
            .query $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                 VALUES (0, 'i', 'init[001,050]', 'initinit');"
        <| eq 1
        |> managed().executeQuery
        |> ignore

    for _ in 1 .. 50 do
        mkCmd()
            .query $"INSERT INTO {tab1} (col1, col2, col3, col4)\
                 VALUES (0, 'i', 'init[050,100]', 'initinit');"
        <| eq 1
        |> managed().executeQuery
        |> ignore
