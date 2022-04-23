namespace DbManaged

open System.Data
open System.Data.Common
open System.Threading.Tasks
open fsharper.types

/// PgSql数据库管理器
[<AbstractClass>]
type IDbManaged() =

    /// 所有查询均不负责类型转换

    /// 查询到第一个值
    abstract member getFstVal : sql: string -> Result'<Option'<obj>, exn>
    /// 参数化查询到第一个值
    abstract member getFstVal : sql: string * paras: (string * 't) list -> Result'<Option'<obj>, exn>
    /// 参数化查询到第一个值
    abstract member getFstVal : sql: string * paras: #DbParameter array -> Result'<Option'<obj>, exn>
    /// 参数化查询到第一个值
    abstract member getFstVal :
        table: string * targetKey: string * targetKey_whereKey: (string * 'V) -> Result'<Option'<obj>, exn>
    /// 从既有DataTable中取出第一个值
    member self.getValFrom(table: DataTable) =
        match table.Rows with
        | rows when rows.Count <> 0 -> Some rows.[0].[0]
        | _ -> None

    /// 查询到第一行
    abstract member getFstRow : sql: string -> Result'<Option'<DataRow>, exn>
    /// 参数化查询到第一行
    abstract member getFstRow : sql: string * paras: (string * 't) list -> Result'<Option'<DataRow>, exn>
    /// 参数化查询到第一行
    abstract member getFstRow : sql: string * paras: #DbParameter array -> Result'<Option'<DataRow>, exn>
    /// 从既有DataTable中取出第一个 whereKey 等于 whereKeyVal 的行
    member self.getRowFrom (table: DataTable) (whereKey: string) whereKeyVal =
        match table.Rows with
        | rows when rows.Count <> 0 ->

            [ for r in rows -> r ]
            |> filter (fun (row: DataRow) -> row.[whereKey].ToString() = whereKeyVal.ToString())
            |> head

        | _ -> None

    /// 查询到指定列
    abstract member getCol : sql: string * key: string -> Result'<Option'<obj list>, exn>
    /// 参数化查询到指定列
    abstract member getCol : sql: string * key: string * paras: (string * 't) list -> Result'<Option'<obj list>, exn>
    /// 参数化查询到指定列
    abstract member getCol : sql: string * key: string * paras: #DbParameter array -> Result'<Option'<obj list>, exn>
    /// 从既有DataTable中取出指定列
    member self.getColFrom(table: DataTable, key: string) =
        match table.Rows with
        | rows when rows.Count <> 0 ->

            //此处未考虑列数为0的情况和取用失败的情况
            [ for r in rows -> r ]
            |> map (fun (row: DataRow) -> row.[key])
            |> Some

        | _ -> None

    /// 查询到指定列
    abstract member getCol : sql: string * index: uint -> Result'<Option'<obj list>, exn>
    /// 参数化查询到指定列
    abstract member getCol : sql: string * index: uint * paras: (string * 't) list -> Result'<Option'<obj list>, exn>
    /// 参数化查询到指定列
    abstract member getCol : sql: string * index: uint * paras: #DbParameter array -> Result'<Option'<obj list>, exn>
    /// 从既有DataTable中取出指定列
    member self.getColFrom(table: DataTable, index: uint) =
        match table.Rows with
        | rows when rows.Count <> 0 ->

            //TODO 此处未考虑列数为0的情况和取用失败的情况
            [ for r in rows -> r ]
            |> map (fun (row: DataRow) -> row.[int index])
            |> Some

        | _ -> None


    //TODO exp async api
    abstract member executeAnyAsync : sql: string -> Result'<(int -> bool) -> Task<int>, exn>

    /// 从连接池取用 DbConnection 并在其上调用同名方法
    abstract member executeAny : sql: string -> Result'<(int -> bool) -> int, exn>
    /// 从连接池取用 DbConnection 并在其上调用同名方法
    abstract member executeAny : sql: string * paras: (string * 't) list -> Result'<(int -> bool) -> int, exn>
    /// 从连接池取用 DbConnection 并在其上调用同名方法
    abstract member executeAny : sql: string * paras: #DbParameter array -> Result'<(int -> bool) -> int, exn>

    /// 查询到表
    abstract member executeSelect : sql: string -> Result'<DataTable, exn>
    /// 参数化查询到表
    abstract member executeSelect : sql: string * paras: (string * 't) list -> Result'<DataTable, exn>
    /// 参数化查询到表
    abstract member executeSelect : sql: string * paras: #DbParameter array -> Result'<DataTable, exn>

    /// 从连接池取用 DbConnection 并在其上调用同名方法
    abstract member executeUpdate :
        table: string * setKey_setKeyVal: (string * 'd) * whereKey_whereKeyVal: (string * 'e) ->
        Result'<(int -> bool) -> int, exn>
    /// 从连接池取用 DbConnection 并在其上调用同名方法
    abstract member executeUpdate :
        table: string * key: string * newValue: 'c * oldValue: 'c -> Result'<(int -> bool) -> int, exn>

    /// 从连接池取用 DbConnection 并在其上调用同名方法
    abstract member executeInsert : table: string -> pairs: (string * 't) list -> Result'<(int -> bool) -> int, exn>

    /// 从连接池取用 DbConnection 并在其上调用同名方法
    abstract member executeDelete :
        table: string -> whereKey: string * whereKeyVal: 'a -> Result'<(int -> bool) -> int, exn>
