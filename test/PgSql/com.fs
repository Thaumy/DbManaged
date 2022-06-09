module dbm_test.PgSql.com

open fsharper.typ
open fsharper.op.Boxing
open DbManaged
open DbManaged.PgSql
open dbm_test

let tab1 = "sch1.tab1"
let mutable private msgResult = Err ManagedNotInitException
let mutable private managedResult: Result'<IDbManaged, exn> = Err ManagedNotInitException
let managed () = managedResult.unwrap ()
let mkCmd () = managed().mkCmd ()

let connect () =
    match msgResult with
    | Err _ ->
        msgResult <-
            Ok
                { Host = "localhost"
                  Port = 5432us
                  Usr = "postgres"
                  Pwd = "65a1561425f744e2b541303f628963f8"
                  Database = "dbm_test"
                  Pooling = 80us }
    | _ -> ()

    match managedResult with
    | Err _ -> managedResult <- Ok <| new PgSqlManaged(unwrap msgResult)
    | _ -> ()
