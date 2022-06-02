module dbm_test.PgSql.com

open fsharper.typ
open fsharper.op.Boxing
open DbManaged
open DbManaged.PgSql
open dbm_test

let tab1 = "sch1.tab1"
let size = 80u
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
                  User = "postgres"
                  Password = "65a1561425f744e2b541303f628963f8" }
    | _ -> ()

    match managedResult with
    | Err _ ->
        managedResult <-
            Ok
            <| new PgSqlManaged(unwrap msgResult, "dbm_test", size)
    | _ -> ()
