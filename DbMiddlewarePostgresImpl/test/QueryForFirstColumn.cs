namespace DbMiddlewarePostgresImpl.test;

using NUnit.Framework;

public class QueryForFirstColumn
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
            var cols = Init.Db.QueryForFirstColumn(
                $"SELECT content FROM {Util.Tab1} WHERE id = :id;",
                new[] { ("id", (object)i) });

            Assert.AreEqual(2, cols.Count);

            foreach (var col in cols)
                Assert.Contains(col, new[] { "ts1_insert", "ts2_insert" });
        }
    }
}