namespace DbManaged

open System
open System.Data
open fsharper.types


/// PgSql数据库管理器
type IDbManaged =

    /// 所有查询均不负责类型转换

    /// 查询到表
    abstract member getTable : sql: string -> Result'<DataTable, exn>
    /// 参数化查询到表
    abstract member getTable : sql: string * para: Array -> Result'<DataTable, exn>

    /// 查询到第一个值
    abstract member getFstVal : sql: string -> Result'<Option'<obj>, exn>
    /// 参数化查询到第一个值
    abstract member getFstVal : sql: string * para: Array -> Result'<Option'<obj>, exn>
    /// 从既有DataTable中查询到第一个 whereKey 等于 whereKeyVal 的行的值
    abstract member getFstVal :
        table: string * targetKey: string * targetKey_whereKey: (string * 'V) -> Result'<Option'<obj>, exn>

    /// 查询到第一行
    abstract member getFstRow : sql: string -> Result'<Option'<DataRow>, exn>
    /// 参数化查询到第一行
    abstract member getFstRow : sql: string * para: Array -> Result'<Option'<DataRow>, exn>
    /// 从既有DataTable中取出第一个 whereKey 等于 whereKeyVal 的行
    abstract member getFstRowFrom : table: DataTable -> whereKey: string -> whereKeyVal: 'a -> Option'<DataRow>

    /// 查询到第一列
    abstract member getFstCol : sql: string -> Result'<Option'<obj list>, exn>
    /// 参数化查询到第一列
    abstract member getFstCol : sql: string * para: Array -> Result'<Option'<obj list>, exn>
    /// 从既有DataTable中取出第一列
    abstract member getFstColFrom : table: DataTable -> Option'<obj list>

    /// 查询到指定列
    abstract member getCol : sql: string * key: string -> Result'<Option'<obj list>, exn>
    /// 参数化查询到指定列
    abstract member getCol : sql: string * key: string * para: Array -> Result'<Option'<obj list>, exn>
    /// 从既有DataTable中取出指定列
    abstract member getColFrom : table: DataTable -> key: string -> Option'<obj list>

    /// 从连接池取用 NpgsqlConnection 并在其上调用同名方法
    abstract member execute : sql: string -> Result'<(int -> bool) -> int, exn>
    /// 从连接池取用 NpgsqlConnection 并在其上调用同名方法
    abstract member execute : sql: string * para: Array -> Result'<(int -> bool) -> int, exn>

    /// 从连接池取用 NpgsqlConnection 并在其上调用同名方法
    abstract member executeUpdate :
        table: string * setKey_setKeyVal: (string * 'd) * whereKey_whereKeyVal: (string * 'e) ->
        Result'<(int -> bool) -> int, exn>

    /// 从连接池取用 NpgsqlConnection 并在其上调用同名方法
    abstract member executeUpdate :
        table: string * key: string * newValue: 'c * oldValue: 'c -> Result'<(int -> bool) -> int, exn>

    /// 从连接池取用 NpgsqlConnection 并在其上调用同名方法
    abstract member executeInsert : table: string -> pairs: (string * 'b) list -> Result'<(int -> bool) -> int, exn>
    /// 从连接池取用 NpgsqlConnection 并在其上调用同名方法
    abstract member executeDelete :
        table: string -> whereKey: string * whereKeyVal: 'a -> Result'<(int -> bool) -> int, exn>
