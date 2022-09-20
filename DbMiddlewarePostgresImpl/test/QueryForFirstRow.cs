namespace DbMiddlewarePostgresImpl.test;

using NUnit.Framework;

public class QueryForFirstRow
{
    [SetUp]
    public void Setup()
    {
        Init.Do();
    }

    [Test]
    public void Test1()
    {
        for (var i = 0; i < 1000; i++)
        {
            var row = Init.Db.QueryForFirstRow(
                $"SELECT * FROM {Util.Tab1} WHERE id = :id;",
                new[] { ("id", (object)i) });

            Assert.Contains(row["content"], new[] { "ts1_insert", "ts2_insert" });
        }
    }
}