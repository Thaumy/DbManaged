package MySqlManager

import sun.security.ec.point.ProjectivePoint
import java.sql.*


class MySqlManager() {
    private lateinit var ConnectionString: String
    private lateinit var ConnectionPool: MutableList<Connection>

    val Connection: Connection
        get() {
            /* 在连接数超出时检查无用连接并进行清理 */
            if (ConnectionPool.count() > 32) {
                for (i in ConnectionPool.count() downTo 0) {
                    /* 如果连接中断或是关闭（这都是不工作的状态） *///JDBC貌似没有给出数据库连接的状态枚举信息
                    if (ConnectionPool[i].isClosed) {
                        ConnectionPool[i].close();/* 注销并移除连接池 *///JDBC没有注销和关闭的概念区分，仅有关闭
                        ConnectionPool.removeAt(i);
                    }
                }
            }

            ConnectionPool.add(DriverManager.getConnection(ConnectionString))
            //ConnectionPool.last().Open();//JDBC中貌似没有打开数据库连接的操作？
            return ConnectionPool.last();

        }

    init {
        Class.forName("com.mysql.jdbc.Driver")
    }

    constructor(MySqlConnMsg: MySqlConnMsg) : this() {
        ConnectionString =
            "jdbc:mysql://${MySqlConnMsg.DataSource}:${MySqlConnMsg.Port}" +
                    "/?user=${MySqlConnMsg.User}&" +
                    "password=${MySqlConnMsg.PWD}&" +
                    "UseAffectedRows=TRUE;"
        /* UPDATE语句返回受影响的行数而不是符合查询条件的行数 */
    }

    constructor(MySqlConnMsg: MySqlConnMsg, Database: String) : this() {
        ConnectionString =/* USING目标数据库 */
            "jdbc:mysql://${MySqlConnMsg.DataSource}:${MySqlConnMsg.Port}" +
                    "/${Database}?user=${MySqlConnMsg.User}&" +
                    "password=${MySqlConnMsg.PWD}&" +
                    "UseAffectedRows=TRUE;"
    }

}

data class MySqlConnMsg(val DataSource: String, val Port: Int, val User: String, val PWD: String)