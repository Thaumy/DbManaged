package MySqlManager

import java.sql.*
import java.util.*


class MySqlManager() {
    private lateinit var ConnectionString: String
    private lateinit var ConnectionPool: MutableList<Connection>

    //数据库连接访问器
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

    //一次性连接使用器
    inline fun <T> DoInConnection(todo: (Connection) -> T): T {
        val conn: Connection = Connection;
        val result: T = todo(conn);
        conn.close()
        return result
    }

    //获取单张数据表
    fun GetTable(SQL: String): ResultSet {
        DoInConnection { conn ->
            return conn.createStatement().executeQuery(SQL)
        }
    }

    // 获取单张数据表（适用于参数化查询）
    fun GetTable(SQL: String, vararg parameters: MySqlParameter): ResultSet {
        DoInConnection { conn ->
            val state = conn.prepareStatement(SQL)
            for (el in parameters)
                state.setString(el.Index, el.Value)
            return state.executeQuery()
        }
    }

    // 取得首个键值（键匹配查询）
    fun GetKey(SQL: String): Any? {
        DoInConnection { conn ->
            /* 如果结果集为空，该方法返回null *///未作考虑
            val rs = conn.createStatement().executeQuery(SQL)
            rs.next()
            return rs.getObject(1)//JDBC字段索引从1开始
        }
    }

    // 取得首个键值（键匹配查询）
    fun GetKey(SQL: String, vararg parameters: MySqlParameter): Any? {
        DoInConnection { conn ->
            val state = conn.prepareStatement(SQL)
            for (el in parameters)
                state.setString(el.Index, el.Value)
            val rs = state.executeQuery()
            /* 如果结果集为空，该方法返回null *///未作考虑
            rs.next()
            return rs.getObject(1)
        }
    }

    // 取得指定键值（键匹配查询）
    fun GetKey(MySqlKey: MySqlKey, KeyName: String): Any? {
        DoInConnection { conn ->
            val SQL = "SELECT $KeyName FROM ${MySqlKey.Table} WHERE ${MySqlKey.Name}='${MySqlKey.Val}';";
            /* 如果结果集为空，该方法返回null */
            val state = conn.createStatement()
            val rs = state.executeQuery(SQL)
            rs.next()
            return rs.getObject(1)
        }
    }

    // 获得数据行
    fun GetRow(SQL: String): DataRow {
        /* 数组越界防止 */
        val rs = GetTable(SQL)
        rs.next()
        var rsmd = rs.metaData//rs元信息
        val row = DataRow()
        for (i in 0..rsmd.columnCount) {
            row.add(rsmd.getColumnLabel(i), rs.getObject(i))
        }
        return row
    }

    // 获得数据行（适用于参数化查询）
    fun GetRow(SQL: String, vararg parameters: MySqlParameter): DataRow {
        val rs = GetTable(SQL, *parameters)
        rs.next()
        val rsmd = rs.metaData//rs元信息
        val row = DataRow()
        for (i in 0..rsmd.columnCount) {
            row.add(rsmd.getColumnLabel(i), rs.getObject(i))
        }
        return row
    }


    // 取得查询结果中的第一列
    fun <T> GetColumn(SQL: String): DataColumn<T> {
        return GetColumn(GetTable(SQL))
    }

    // 取得查询结果中的指定列
    fun <T> GetColumn(SQL: String, Key: String): DataColumn<T> {
        return GetColumn(GetTable(SQL), Key)
    }

    // 取得查询结果中的第一列
    fun <T> GetColumn(SQL: String, vararg parameters: MySqlParameter): DataColumn<T> {
        return GetColumn(GetTable(SQL, *parameters))
    }

    // 取得查询结果中的指定列
    fun <T> GetColumn(SQL: String, Key: String, vararg parameters: MySqlParameter): DataColumn<T> {
        return GetColumn(GetTable(SQL, *parameters), Key)
    }


    /**
     * 更新单个键值
     * @param MySqlKey 目标表
     * @param Key 要更改的键
     * @param NewValue 新键值
     * @return 是否操作成功
     */
    fun UpdateKey(MySqlKey: MySqlKey, Key: String, NewValue: Any): Boolean {
        DoInConnection { conn ->
            val SQL = "UPDATE ${MySqlKey.Table} SET ${Key}=?NewValue WHERE ${MySqlKey.Name}=?Val"
            conn.autoCommit = false
            val state = conn.prepareStatement(SQL)

            state.setObject(1, NewValue)
            state.setObject(2, MySqlKey.Val)

            return when (state.executeUpdate()) {
                1 -> {
                    conn.commit()
                    true
                }
                else -> {
                    conn.rollback()
                    false
                }
            }
        }
    }

    /**
     * 更新单个键值
     * @param Table 目标表
     * @param Key 键名
     * @param OldValue 旧值
     * @param NewValue 新值
     * @return 是否操作成功
     */
    fun UpdateKey(Table: String, Key: String, OldValue: Any, NewValue: Any): Boolean {
        return DoInConnection { conn ->
            val SQL = "UPDATE ${Table} SET ${Key}=?NewValue WHERE ${Key}=?OldValue"
            conn.autoCommit = false
            val state = conn.prepareStatement(SQL)

            state.setObject(1, NewValue)
            state.setObject(2, OldValue)

            return when (state.executeUpdate()) {
                1 -> {
                    conn.commit()
                    true
                }
                else -> {
                    conn.rollback()
                    false
                }
            }
        }
    }

    //static
    companion object {
        // 从DataTable中提取第一列（此方法无空值判断）
        fun <T> GetColumn(DataTable: ResultSet): DataColumn<T> {
            val List = DataColumn<T>()

            while (DataTable.next()) {
                List.add(DataTable.getObject(1) as T)
            }
            return List
        }

        // 从DataTable中提取指定列（此方法无空值判断）
        fun <T> GetColumn(DataTable: ResultSet, Key: String): DataColumn<T> {
            val List = DataColumn<T>()

            while (DataTable.next()) {
                List.add(DataTable.getObject(Key) as T)
            }

            return List
        }

        // 从DataTable中提取指定行
        fun GetRow(DataTable: ResultSet, KeyName: String, KeyValue: Any): DataRow? {
            while (DataTable.next()) {
                if (DataTable.getString(KeyName) == KeyValue.toString()) {
                    /* 返回符合被检索主键的行 */
                    val rsmd = DataTable.metaData//rs元信息
                    val row = DataRow()
                    for (i in 0..rsmd.columnCount) {
                        row.add(rsmd.getColumnLabel(i), DataTable.getObject(i))
                    }
                    return row
                }
            }
            return null/* 未检索到 */
        }
    }
}

data class MySqlConnMsg(val DataSource: String, val Port: Int, val User: String, val PWD: String)
data class MySqlKey(val Table: String, val Name: String, val Val: Any)
data class MySqlParameter(val Index: Int, val Value: String)

//数据行
class DataRow {
    private val innerMap = mutableMapOf<String, Any>()
    fun add(Key: String, Value: Any) {
        innerMap[Key] = Value
    }
}

//数据列
class DataColumn<T> {
    private val innerList = mutableListOf<T>()
    fun add(obj: T) {
        innerList.add(obj)
    }
}

//数据表
class DataTable() {

}