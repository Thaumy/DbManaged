module dbm_test.PgSql.com

open DbManaged
open DbManaged.MySql
open DbManaged.PgSql
open fsharper.op.Boxing
open fsharper.types.Ord
open fsharper.types
open fsharper.op.Async

let tab1 = "sch1.tab1"
let size = 100u
let mutable msg = None
let mutable managed: Option'<IDbManagedAsync> = None

let connect () =
    match msg with
    | None ->
        msg <-
            Some
                { Host = "localhost"
                  Port = 5432us
                  User = "postgres"
                  Password = "65a1561425f744e2b541303f628963f8" }
    | _ -> ()

    match managed with
    | None -> managed <- Some <| PgSqlManaged(unwrap msg, "dbm_test", size)
    | _ -> ()
