namespace DbMiddlewarePostgresImpl.test;

using DbMiddlewarePostgresImpl;
using static Util;

public static class Init
{
    public static PostgresDatabase Db;

    public static void Do()
    {
        Db = new PostgresDatabase(
            "postgres",
            "65a1561425f744e2b541303f628963f8",
            "localhost",
            5432,
            "dbm_mw_test"
        );
        Db.Query($"drop table if exists {Tab1};");
        Db.Query($@"create table {Tab1}
                       (
                           id        integer,
                           test_name varchar(256),
                           time      timestamptz,
                           content   text
                       );");
        for (var i = 0; i < 1000; i++)
            Db.Query(@$"INSERT INTO {Tab1} (id, test_name, time, content)
                           VALUES ({i}, 'init', '{Util.Iso8601Now()}', 'ts1_insert');");

        for (var i = 0; i < 1000; i++)
            Db.Query(@$"INSERT INTO {Tab1} (id, test_name, time, content)
                           VALUES ({i}, 'init', '{Util.Iso8601Now()}', 'ts2_insert');");
    }
}