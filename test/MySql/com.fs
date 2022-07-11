module dbm_test.MySql.com

open fsharper.typ
open fsharper.op.Boxing
open DbManaged
open DbManaged.MySql
open dbm_test

let tab1 = "tab1"

let mutable private msgResult =
    Err ManagedNotInitException

let mutable private managedResult: Result'<IDbManaged, exn> =
    Err ManagedNotInitException

let managed () = managedResult.unwrap ()
let mkCmd () = managed().mkCmd ()

let connect () =
    match msgResult with
    | Err _ ->
        msgResult <-
            Ok
                { host = "localhost"
                  port = 3306us
                  usr = "root"
                  pwd = "65a1561425f744e2b541303f628963f8"
                  db = "dbm_test" }
    | _ -> ()

    match managedResult with
    | Err _ -> managedResult <- Ok <| new MySqlManaged(unwrap msgResult, 80us)
    | _ -> ()
