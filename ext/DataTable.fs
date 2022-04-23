[<AutoOpen>]
module internal DbManaged.ext.DataTable

open System.Data
open fsharper.types

/// 从DataTable中取出第一个值
let getFstValFrom (table: DataTable) =
    if table.Rows.Count <> 0 && table.Columns.Count <> 0 then
        Some table.Rows.[0].[0]
    else
        None
/// 从DataTable中取出指定列
let getColFromByKey (table: DataTable, key: string) =
    if table.Columns.Contains(key) then
        Some [ for r in table.Rows do
                   r.[key] ]
    else
        None
/// 从DataTable中取出指定列
let getColFromByIndex (table: DataTable, index: uint) =
    if table.Columns.Count > 0
       && int index < table.Columns.Count then
        Some [ for r in table.Rows do
                   r.[int index] ]
    else
        None
/// 从DataTable中取出第一个 whereKey 等于 whereKeyVal 的行
let getRowFrom (table: DataTable) (whereKey: string) whereKeyVal =
    if
        table.Rows.Count <> 0
        && table.Columns.Contains("whereKey")
    then
        [ for r in table.Rows -> r ]
        |> filterOne (fun (row: DataRow) -> row.[whereKey].ToString() = whereKeyVal.ToString())
        |> Some
    else
        None
