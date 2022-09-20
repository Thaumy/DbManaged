namespace DbMiddlewarePostgresImpl.test;

using NUnit.Framework;

public class QueryForFirstValue
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
            var result = Init.Db.QueryForFirstValue(
                $"SELECT content FROM {Util.Tab1} WHERE id = :id;",
                new[] { ("id", (object)i) });

            Assert.Contains(result, new[] { "ts1_insert", "ts2_insert" });
        }
    }
}