package MySqlManager

class MySqlManager() {
    private lateinit var ConnectionString: String

    constructor(MySqlConnMsg: MySqlConnMsg) : this() {
        ConnectionString =
            """;DataSource=${MySqlConnMsg.DataSource}
        ;Port=${MySqlConnMsg.Port}
        ;UserID=${MySqlConnMsg.User}
        ;Password=${MySqlConnMsg.PWD}
        ;UseAffectedRows=TRUE;"""/* UPDATE语句返回受影响的行数而不是符合查询条件的行数 */
    }

    constructor(MySqlConnMsg: MySqlConnMsg, Database: String) : this() {
        ConnectionString = /* USING目标数据库 */
            """;DataSource=${MySqlConnMsg.DataSource}
        ;DataBase=${Database}
        ;Port=${MySqlConnMsg.Port}
        ;UserID=${MySqlConnMsg.User}
        ;Password=${MySqlConnMsg.PWD}
        ;UseAffectedRows=TRUE;"""
    }

}

data class MySqlConnMsg(val DataSource: String, val Port: Int, val User: String, val PWD: String)