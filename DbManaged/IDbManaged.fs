namespace DbManaged

open System.Data
open System.Data.Common
open fsharper.typ

/// PgSql数据库管理器
type IDbManaged =

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


    /// 查询到第一行
    abstract member getFstRow : sql: string -> Result'<Option'<DataRow>, exn>
    /// 参数化查询到第一行
    abstract member getFstRow : sql: string * paras: (string * 't) list -> Result'<Option'<DataRow>, exn>
    /// 参数化查询到第一行
    abstract member getFstRow : sql: string * paras: #DbParameter array -> Result'<Option'<DataRow>, exn>


    /// 查询到指定列
    abstract member getCol : sql: string * key: string -> Result'<Option'<obj list>, exn>
    /// 参数化查询到指定列
    abstract member getCol : sql: string * key: string * paras: (string * 't) list -> Result'<Option'<obj list>, exn>
    /// 参数化查询到指定列
    abstract member getCol : sql: string * key: string * paras: #DbParameter array -> Result'<Option'<obj list>, exn>


    /// 查询到指定列
    abstract member getCol : sql: string * index: uint -> Result'<Option'<obj list>, exn>
    /// 参数化查询到指定列
    abstract member getCol : sql: string * index: uint * paras: (string * 't) list -> Result'<Option'<obj list>, exn>
    /// 参数化查询到指定列
    abstract member getCol : sql: string * index: uint * paras: #DbParameter array -> Result'<Option'<obj list>, exn>

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

    abstract member executeUpdate :
        table: string * setKey_setKeyVal: (string * 'd) * whereKey_whereKeyVal: (string * 'e) ->
        Result'<(int -> bool) -> int, exn>

    abstract member executeUpdate :
        table: string * key: string * newValue: 'c * oldValue: 'c -> Result'<(int -> bool) -> int, exn>

    abstract member executeInsert : table: string -> pairs: (string * 't) list -> Result'<(int -> bool) -> int, exn>

    abstract member executeDelete :
        table: string -> whereKey: string * whereKeyVal: 'a -> Result'<(int -> bool) -> int, exn>
