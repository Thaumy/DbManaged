namespace DbMiddleware;

public interface IDatabase
{
    object? QueryForFirstValue(string sql, (string key, object value)[]? paras);

    Dictionary<string, object>? QueryForFirstRow
        (string sql, (string key, object value)[]? paras);

    List<object> QueryForFirstColumn
        (string sql, (string key, object value)[]? paras);

    List<Dictionary<string, object>> QueryForTable
        (string sql, (string key, object value)[]? paras);

    int Query(string sql, int expectedAffect, (string key, object value)[]? paras);
}