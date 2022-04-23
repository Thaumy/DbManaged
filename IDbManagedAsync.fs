namespace DbManaged

open System.Data
open System.Data.Common
open System.Threading.Tasks
open fsharper.types

type IDbManagedAsync =
    inherit IDbManaged
    //TODO exp async api
    abstract member executeAnyAsync : sql: string -> Result'<(int -> bool) -> Task<int>, exn>
    //TODO exp async api
    abstract member executeAnyAsync :
        sql: string * paras: (string * 't) list -> Result'<(int -> bool) -> Task<int>, exn>
    //TODO exp async api
    abstract member executeAnyAsync :
        sql: string * paras: #DbParameter array -> Result'<(int -> bool) -> Task<int>, exn>
