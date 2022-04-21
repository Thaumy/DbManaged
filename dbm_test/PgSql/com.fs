module dbm_test.PgSql.com

open DbManaged
open DbManaged.PgSql
open fsharper.op.Boxing
open fsharper.types.Ord
open fsharper.types
open fsharper.op.Async

let tab1 = "sch1.tab1"
let mutable msg = None
let mutable managed: Option'<IDbManaged> = None

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
    | None -> managed <- Some <| PgSqlManaged(unwarp msg, "dbm_test", 32u)
    | _ -> ()