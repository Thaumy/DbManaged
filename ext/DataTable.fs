[<AutoOpen>]
module internal DbManaged.ext.DataTable

open System.Data
open fsharper.types

type DataTable with

    /// 从DataTable中取出第一个值
    member self.getFstVal() =
        if self.Rows.Count <> 0 && self.Columns.Count <> 0 then
            Some self.Rows.[0].[0]
        else
            None

    /// 从DataTable中取出指定列
    member self.getColByKey(key: string) =
        if self.Columns.Contains(key) then
            Some [ for r in self.Rows do
                       r.[key] ]
        else
            None

    /// 从DataTable中取出指定列
    member self.getColByIndex(index: uint) =
        if self.Columns.Count > 0
           && int index < self.Columns.Count then
            Some [ for r in self.Rows do
                       r.[int index] ]
        else
            None

    /// 从DataTable中取出第一个 whereKey 等于 whereKeyVal 的行
    member self.getRow (whereKey: string) whereKeyVal =
        if
            self.Rows.Count <> 0
            && self.Columns.Contains("whereKey")
        then
            [ for r in self.Rows -> r ]
            |> filterOne (fun (row: DataRow) -> row.[whereKey].ToString() = whereKeyVal.ToString())
            |> Some
        else
            None
