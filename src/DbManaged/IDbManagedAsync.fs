namespace DbManaged
 
open System.Data.Common
open System.Threading.Tasks
open fsharper.typ
open fsharper.op.Alias

type IDbManagedAsync =
    
    inherit IDbManaged 
    
    //TODO exp async api
    abstract member executeAnyAsync : sql: string -> Result'<(i32 -> bool) -> Task<int>, exn>
    //TODO exp async api
    abstract member executeAnyAsync :
        sql: string * paras: (string * 't) list -> Result'<(i32 -> bool) -> Task<int>, exn>
    //TODO exp async api
    abstract member executeAnyAsync :
        sql: string * paras: #DbParameter array -> Result'<(i32 -> bool) -> Task<int>, exn>
