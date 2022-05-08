module dbm_test.PgSql.com

open DbManaged
open DbManaged.PgSql
open fsharper.op.Boxing
open fsharper.typ
open dbm_test

let tab1 = "sch1.tab1"
let size = 100u
let mutable msg = Err ConnMsgNotInitException
let mutable managed: Result'<IDbManagedAsync, exn> = Err ManagedNotInitException

let connect () =
    match msg with
    | Err _ ->
        msg <-
            Ok
                { Host = "localhost"
                  Port = 5432us
                  User = "postgres"
                  Password = "65a1561425f744e2b541303f628963f8" }
    | _ -> ()

    match managed with
    | Err _ ->
        managed <-
            Ok
            <| new PgSqlManaged(unwrap msg, "dbm_test", size)
    | _ -> ()
