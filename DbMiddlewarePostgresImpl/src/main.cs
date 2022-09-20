using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using DbManaged;
using DbManaged.PgSql;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;

namespace DbMiddlewarePostgresImpl;

using DbMiddleware;

public class PostgresDatabase : IDatabase
{
    private readonly PgSqlManaged _dbManaged;

    public PostgresDatabase(string usr, string pwd, string host, int port, string database)
    {
        var msg = new DbConnMsg(host, (ushort)port, usr, pwd, database);
        _dbManaged = new PgSqlManaged(msg, 32);
    }

    public object? QueryForFirstValue(string sql, (string key, object value)[]? paras = null)
    {
        var arr =
            paras == null ? Array.Empty<Tuple<string, object>>() : new Tuple<string, object>[paras.Length];

        if (paras != null)
            for (var i = 0; i < paras.Length; i++)
                arr[i] = new Tuple<string, object>(paras[i].key, paras[i].value);

        var executor =
            DbManaged.ext_DbCommand.getFstVal(_dbManaged.mkCmd(), sql, ListModule.OfArray(arr));

        var result = _dbManaged.executeQuery(executor);

        return result.IsNone ? null : result.unwrap();
    }

    public Dictionary<string, object>? QueryForFirstRow
        (string sql, (string key, object value)[]? paras = null)
    {
        var arr =
            paras == null ? Array.Empty<Tuple<string, object>>() : new Tuple<string, object>[paras.Length];

        if (paras != null)
            for (var i = 0; i < paras.Length; i++)
                arr[i] = new Tuple<string, object>(paras[i].key, paras[i].value);

        var executor =
            DbManaged.ext_DbCommand.getFstRow(_dbManaged.mkCmd(), sql, ListModule.OfArray(arr));

        var result = _dbManaged.executeQuery(executor);

        if (result.IsNone)
            return null;

        var dataRow = result.unwrap();
        var kvs =
            from DataColumn col
                in dataRow.Table.Columns
            select
                new KeyValuePair<string, object>(col.ColumnName.ToString(), dataRow[col]);

        return new Dictionary<string, object>(kvs);
    }

    public List<object> QueryForFirstColumn
        (string sql, (string key, object value)[]? paras = null)
    {
        var arr =
            paras == null ? Array.Empty<Tuple<string, object>>() : new Tuple<string, object>[paras.Length];

        if (paras != null)
            for (var i = 0; i < paras.Length; i++)
                arr[i] = new Tuple<string, object>(paras[i].key, paras[i].value);

        var executor =
            DbManaged.ext_DbCommand.getFstCol(_dbManaged.mkCmd(), sql, ListModule.OfArray(arr));

        var result = _dbManaged.executeQuery(executor);

        return result.ToList();
    }

    public List<Dictionary<string, object>> QueryForTable
        (string sql, (string key, object value)[]? paras = null)
    {
        var arr =
            paras == null ? Array.Empty<Tuple<string, object>>() : new Tuple<string, object>[paras.Length];

        if (paras != null)
            for (var i = 0; i < paras.Length; i++)
                arr[i] = new Tuple<string, object>(paras[i].key, paras[i].value);

        var executor =
            DbManaged.ext_DbCommand.select(_dbManaged.mkCmd(), sql, ListModule.OfArray(arr));

        var result = _dbManaged.executeQuery(executor);

        if (result.IsNone)
            return new List<Dictionary<string, object>>();

        var dataTable = result.unwrap();

        var list = new List<Dictionary<string, object>>(
            from DataRow row
                in dataTable.Rows
            select
                new Dictionary<string, object>(
                    from DataColumn col
                        in dataTable.Columns
                    select
                        new KeyValuePair<string, object>(col.ColumnName.ToString(), row[col])
                )
        );

        return list;
    }

    public int Query(string sql, int expectedAffect = -1, (string key, object value)[]? paras = null)
    {
        var arr =
            paras == null ? Array.Empty<Tuple<string, object>>() : new Tuple<string, object>[paras.Length];

        if (paras != null)
            for (var i = 0; i < paras.Length; i++)
                arr[i] = new Tuple<string, object>(paras[i].key, paras[i].value);

        var needAffPred =
            DbManaged.ext_DbCommand.query(_dbManaged.mkCmd(), sql, ListModule.OfArray(arr));

        var affPred =
            FSharpFunc<int, bool>.FromConverter(n => expectedAffect < 0 || n == expectedAffect);

        var executor = needAffPred.Invoke(affPred);

        var result = _dbManaged.executeQuery(executor);

        return result;
    }
}