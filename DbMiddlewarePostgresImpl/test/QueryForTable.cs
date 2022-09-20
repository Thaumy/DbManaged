namespace DbMiddlewarePostgresImpl.test;

using NUnit.Framework;

public class QueryForTable
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
            var table = Init.Db.QueryForTable(
                $"SELECT test_name, content FROM {Util.Tab1} WHERE id = :id;",
                new[] { ("id", (object)i) });

            foreach (var row in table)
            {
                Assert.AreEqual("init", row["test_name"]);
                Assert.Contains(row["content"], new[] { "ts1_insert", "ts2_insert" });
            }
        }
    }
}