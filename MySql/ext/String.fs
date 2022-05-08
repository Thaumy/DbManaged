module DbManaged.MySql.ext.String

open System.Text.RegularExpressions

/// 将私有格式sql规范化到标准sql
let normalizeSql (s: string) =
    let mark = "?"
    Regex.Replace(s, "<([0-9a-zA-Z]*)>", $"{mark}$1")
