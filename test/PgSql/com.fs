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
let makeCmd () = managed().makeCmd ()

let connect () =
    match msgResult with
    | Err _ ->
        msgResult <-
            Ok
                { host = "localhost"
                  port = 5432us
                  usr = "postgres"
                  pwd = "65a1561425f744e2b541303f628963f8"
                  db = "dbm_test" }
    | _ -> ()

    match managedResult with
    | Err _ -> managedResult <- Ok <| new PgSqlManaged(unwrap msgResult, 80us)
    | _ -> ()
