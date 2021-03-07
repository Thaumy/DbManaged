package MySqlManager

import java.sql.*


class MySqlManager() {
    private lateinit var ConnectionString: String//链接字符串
    private var ConnectionPool: MutableList<Connection>

    //数据库连接访问器
    val Connection: Connection
        get() {
            /* 在连接数超出时检查无用连接并进行清理 */
            if (ConnectionPool.count() > 32) {
                for (i in ConnectionPool.count() - 1 downTo 0) {
                    /* 如果连接中断或是关闭（这都是不工作的状态） *///JDBC貌似没有给出数据库连接的状态枚举信息
                    if (ConnectionPool[i].isClosed) {
                        //ConnectionPool[i].close();/* 注销并移除连接池 *///JDBC没有注销和关闭的概念区分，仅有关闭
                        ConnectionPool.removeAt(i);
                    }
                }
            }

            ConnectionPool.add(DriverManager.getConnection(ConnectionString))
            //ConnectionPool.last().Open();//JDBC中貌似没有打开数据库连接的操作？
            return ConnectionPool.last();
        }

    init {
        Class.forName("com.mysql.cj.jdbc.Driver")
        ConnectionPool = mutableListOf()
    }

    constructor(MySqlConnMsg: MySqlConnMsg, Database: String) : this() {
        ConnectionString =/* USING目标数据库 */
            "jdbc:mysql://${MySqlConnMsg.DataSource}:${MySqlConnMsg.Port}" +
                    "/${Database}?user=${MySqlConnMsg.User}&" +
                    "password=${MySqlConnMsg.PWD}&" +
                    "UseAffectedRows=TRUE;&serverTimezone=GMT"
    }

    //一次性连接使用器
    inline fun <T> DoInConnection(todo: (Connection) -> T): T {
        val conn = Connection;
        val result: T = todo(conn);
        conn.close()
        return result
    }

    //获取单张数据表
    fun GetTable(SQL: String): DataTable {
        DoInConnection { conn ->
            val rs = conn.createStatement().executeQuery(SQL)
            val rsmd = rs.metaData//rs元信息
            val dataTable = DataTable()//临时表
            while (rs.next()) {
                val row = DataRow()//临时行
                for (i in 1..rsmd.columnCount) {
                    row.add(rsmd.getColumnLabel(i), rs.getObject(i))
                }
                dataTable.addRow(row)
            }
            return dataTable
        }
    }

    // 获取单张数据表（适用于参数化查询）
    fun GetTable(SQL: String, vararg parameters: MySqlParameter): DataTable {
        DoInConnection { conn ->
            val state = conn.prepareStatement(SQL)
            for (el in parameters)
                state.setString(el.Index, el.Value)

            val rs = state.executeQuery()
            val rsmd = rs.metaData//rs元信息
            val dataTable = DataTable()//临时表
            while (rs.next()) {
                val row = DataRow()//临时行
                for (i in 1..rsmd.columnCount) {
                    row.add(rsmd.getColumnLabel(i), rs.getObject(i))
                }
                dataTable.addRow(row)
            }
            return dataTable
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
        return GetTable(SQL).getRow(0)
    }

    // 获得数据行（适用于参数化查询）
    fun GetRow(SQL: String, vararg parameters: MySqlParameter): DataRow {
        return GetTable(SQL, *parameters).getRow(0)
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

    //执行任意SQL
    fun ExcuteAny(SQL: String) {
        DoInConnection { conn ->
            conn.createStatement().execute(SQL)
        }
    }

    //执行任意SQL
    fun ExcuteAny(SQL: String, vararg parameters: MySqlParameter) {
        DoInConnection { conn ->
            val state = conn.prepareStatement(SQL)
            for (el in parameters) {
                state.setObject(el.Index, el.Value)
            }
            conn.commit()
        }
    }

    /**
     * 更新操作
     * @param Table 目标表
     * @param SET SET clause
     * @param WHERE WHERE clause
     * @return 是否操作成功
     */
    fun ExcuteUpdate(Table: String, SET: Pair, WHERE: Pair): Boolean {
        DoInConnection { conn ->
            val SQL = "UPDATE ${Table} SET ${SET.K}=?NewValue WHERE ${WHERE.K}=?Val"
            conn.autoCommit = false
            val state = conn.prepareStatement(SQL)

            state.setObject(1, SET.V)
            state.setObject(2, WHERE.V)

            return when (state.executeUpdate()) {
                1 -> {
                    conn.commit()
                    conn.autoCommit = true
                    true
                }
                else -> {
                    conn.rollback()
                    conn.autoCommit = true
                    false
                }
            }
        }
    }

    /**
     * 更新操作
     * @param Table 目标表
     * @param SET SET clause
     * @param OldValue 旧值
     * @return 是否操作成功
     */
    fun ExcuteUpdate(Table: String, SET: Pair, OldValue: Any): Boolean {
        DoInConnection { conn ->
            val SQL = "UPDATE ${Table} SET ${SET.K}=?NewValue WHERE ${SET.K}=?OldValue"
            conn.autoCommit = false
            val state = conn.prepareStatement(SQL)

            state.setObject(1, SET.V)
            state.setObject(2, OldValue)
            return when (state.executeUpdate()) {
                1 -> {
                    conn.commit()
                    conn.autoCommit = true
                    true
                }
                else -> {
                    conn.rollback()
                    conn.autoCommit = true
                    false
                }
            }
        }
    }

    //插入操作
    fun ExecuteInsert(Table: String, vararg Pairs: Pair): Boolean {
        DoInConnection { conn ->
            conn.autoCommit = false

            var part1 = ""/* VALUES语句前半部分 */
            var part2 = ""/* VALUES语句后半部分 */
            for ((K, _) in Pairs) {
                part1 += "`${K}`,"
                part2 += "?${K} ,"
            }
            part1 = part1.substring(0, part1.length - 1)/* 末尾逗号去除 */
            part2 = part2.substring(0, part1.length - 1)


            val SQL = "INSERT INTO $Table} (${part1})VALUES(${part2})"
            val state = conn.prepareStatement(SQL)
            var index = 0/* 索引值 */
            for ((_, V) in Pairs) {
                state.setObject(index, V)/* 参数添加 */
                index++
            }

            return when (state.executeUpdate()) {
                1 -> {
                    conn.commit()
                    conn.autoCommit = true
                    true
                }
                else -> {
                    conn.rollback()
                    conn.autoCommit = true
                    false
                }
            }
        }
    }

    //删除操作
    fun ExecuteDelete(Table: String, WHERE: Pair): Boolean {
        DoInConnection { conn ->
            conn.autoCommit = false

            val SQL = "DELETE FROM ${Table} WHERE `${WHERE.K}`=?Value"
            val state = conn.prepareStatement(SQL)

            state.setObject(1, WHERE.V)

            return when (state.executeUpdate()) {
                1 -> {
                    conn.commit()
                    conn.autoCommit = true
                    true
                }
                else -> {
                    conn.rollback()
                    conn.autoCommit = true
                    false
                }
            }
        }
    }

    //static
    companion object {
        // 从DataTable中提取第一列（此方法无空值判断）
        @Suppress("UNCHECKED_CAST")
        fun <T> GetColumn(DataTable: DataTable): DataColumn<T> {
            val List = DataColumn<T>()

            for (Row in DataTable) {
                List.add(Row.get(0) as T)
            }
            return List
        }

        // 从DataTable中提取指定列（此方法无空值判断）
        @Suppress("UNCHECKED_CAST")
        fun <T> GetColumn(DataTable: DataTable, Key: String): DataColumn<T> {
            val List = DataColumn<T>()

            for (Row in DataTable) {
                List.add(Row.get(Key) as T)
            }
            return List
        }

        // 从DataTable中提取指定行
        fun GetRow(DataTable: DataTable, Key: String, Value: Any): DataRow? {
            for (Row in DataTable)
                if (Row.get(Key) == Value)
                    return Row
            return null/* 未检索到 */
        }
    }
}

data class Pair(val K: String, val V: Any)
data class MySqlKey(val Table: String, val Name: String, val Val: String)
data class MySqlParameter(val Index: Int, val Value: String)
data class MySqlConnMsg(val DataSource: String, val Port: Int, val User: String, val PWD: String)

//数据行
class DataRow : Iterable<Any> {
    private val innerList = mutableListOf<Pair>()
    val colsCount
        //列计数
        get() = innerList.count()

    fun add(Key: String, Value: Any) {
        innerList.add(Pair(Key, Value))
    }

    fun get(Key: String): Any? {
        for (el in innerList)
            if (el.K == Key)
                return el.V
        return null
    }

    fun get(Index: Int): Any {
        return innerList[Index].V
    }

    /**
     * Returns an iterator over the elements of this object.
     */
    override fun iterator(): MutableIterator<Any> {
        return innerList.iterator()
    }
}

//数据列
class DataColumn<T> : Iterable<T> {
    private val innerList = mutableListOf<T>()
    val colsCount
        //行计数
        get() = innerList.count()

    fun add(obj: T) {
        innerList.add(obj)
    }

    fun get(Index: Int): Any? {
        return innerList[Index]
    }

    /**
     * Returns an iterator over the elements of this object.
     */
    override fun iterator(): MutableIterator<T> {
        return innerList.iterator()
    }
}

//数据表
class DataTable : Iterable<DataRow> {
    private val innerList = mutableListOf<DataRow>()
    val rowsCount
        //行计数
        get() = innerList.count()
    val colsCount
        //列计数
        get() = getRow(0).colsCount

    fun addRow(DataRow: DataRow) {
        innerList.add(DataRow)
    }

    fun getRow(Index: Int): DataRow {
        return innerList[Index]
    }

    /**
     * Returns an iterator over the elements of this object.
     */
    override fun iterator(): MutableIterator<DataRow> {
        return innerList.iterator()
    }
}