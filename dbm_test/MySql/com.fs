module dbm_test.MySql.com

open DbManaged
open DbManaged.MySql
open fsharper.op.Boxing
open fsharper.typ

let tab1 = "tab1"
let size = 100u
let mutable msg = None
let mutable managed: Option'<IDbManagedAsync> = None

let connect () =
    match msg with
    | None ->
        msg <-
            Some
                { Host = "localhost"
                  Port = 3306us
                  User = "root"
                  Password = "65a1561425f744e2b541303f628963f8" }
    | _ -> ()

    match managed with
    | None -> managed <- Some <| MySqlManaged(unwrap msg, "dbm_test", size)
    | _ -> ()
