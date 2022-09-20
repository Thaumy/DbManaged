namespace DbMiddlewarePostgresImpl.test;

using NUnit.Framework;
using DbMiddlewarePostgresImpl.test;
using static Util;

public class InitOk
{
    [SetUp]
    public void Setup()
    {
        Init.Do();
    }

    [Test]
    public void Test1()
    {
        var count1 = Init.Db.QueryForFirstValue(
            $"SELECT COUNT(*) FROM {Tab1} WHERE test_name = 'init' AND content = 'ts1_insert';");
        Assert.AreEqual(1000, count1);

        var count2 = Init.Db.QueryForFirstValue(
            $"SELECT COUNT(*) FROM {Tab1} WHERE test_name = 'init' AND content = 'ts2_insert';");
        Assert.AreEqual(1000, count2);
    }
}