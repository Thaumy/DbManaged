module dbm_test.MySql.com

open fsharper.typ
open fsharper.op.Boxing
open DbManaged
open DbManaged.MySql
open dbm_test

let tab1 = "tab1"
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
                  Port = 3306us
                  User = "root"
                  Password = "65a1561425f744e2b541303f628963f8" }
    | _ -> ()

    match managedResult with
    | Err _ ->
        managedResult <-
            Ok
            <| new MySqlManaged(unwrap msgResult, "dbm_test", size)
    | _ -> ()
