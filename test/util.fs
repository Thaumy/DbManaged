[<AutoOpen>]
module dbm_test.util

open System

let Now () = DateTime.Now

let ISO8601Now () =
    Now().ToString("yyyy-MM-ddTHH:mm:sszzz")
