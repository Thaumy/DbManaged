using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using MySql.Data.MySqlClient;

namespace MySqlManager
{
    /// <summary>
    /// MySql数据库管理器
    /// </summary>
    public class MySqlManager
    {
        private string ConnectionString { get; init; }
        private List<MySqlConnection> ConnectionPool { get; init; }
        /// <summary>
        /// 数据库连接访问器
        /// </summary>
        /// <remarks>若连接为一次性使用，请使用DoInConnection。从此属性获取的连接对象需手动销毁。</remarks>
        public MySqlConnection Connection
        {
            get
            {
                MySqlConnection New()
                {
                    ConnectionPool.Add(new(ConnectionString));
                    ConnectionPool.Last().Open();
                    return ConnectionPool.Last();
                }
                if (ConnectionPool.Count <= 16)//连接数较少时，考虑新建
                {
                    return New();
                }
                else if (ConnectionPool.Count <= 32)//连接数较多时，考虑在循环复用的基础上新建
                {
                    foreach (var el in ConnectionPool)
                    {
                        if (el.State is ConnectionState.Broken or ConnectionState.Closed)
                        {
                            el.Open();
                            return el;
                        }
                    }
                    //找不到可分配连接时，新建
                    return New();
                }
                else
                {
                    CleanConnectionPool();//连接数过多时，清理后新建
                    return New();
                }
            }
        }

        /// <summary>
        /// 清理连接池
        /// </summary>
        /// <remarks>由于连接池具有内部复用机制，经常清理连接池可能会造成不良后果。</remarks>
        /// <remarks>此方法仅仅会尝试清理连接池，并不是所有连接都会被强制关闭</remarks>
        public void CleanConnectionPool()
        {
            for (int i = ConnectionPool.Count - 1; i >= 0; i--)
            { /* 如果连接中断或是关闭（这都是不工作的状态） */
                if (ConnectionPool[i].State is ConnectionState.Broken or ConnectionState.Closed)
                {
                    ConnectionPool[i].Dispose();/* 注销并移除连接池 */
                    ConnectionPool.RemoveAt(i);
                }
            };
        }

        /// <summary>
        /// 默认构造
        /// </summary>
        private MySqlManager() { }
        /// <summary>
        /// 连接信息构造
        /// </summary>
        /// <param name="MySqlConnMsg">MySQL数据库连接信息</param>
        public MySqlManager(MySqlConnMsg MySqlConnMsg)
        {
            ConnectionPool = new();
            ConnectionString =
            $@";DataSource={MySqlConnMsg.DataSource}
               ;Port={MySqlConnMsg.Port }
               ;UserID={MySqlConnMsg.User}
               ;Password={MySqlConnMsg.PWD}
               ;UseAffectedRows=TRUE;";/* UPDATE语句返回受影响的行数而不是符合查询条件的行数 */
        }
        /// <summary>
        /// 带有目标数据库的连接信息构造
        /// </summary>
        /// <param name="MySqlConnMsg">MySQL数据库连接信息</param>
        /// <param name="Database">目标数据库</param>
        public MySqlManager(MySqlConnMsg MySqlConnMsg, string Database)
        {
            ConnectionPool = new();
            ConnectionString = /* USING目标数据库 */
            $@";DataSource={MySqlConnMsg.DataSource}
               ;DataBase={Database}
               ;Port={MySqlConnMsg.Port }
               ;UserID={MySqlConnMsg.User}
               ;Password={MySqlConnMsg.PWD}
               ;UseAffectedRows=TRUE;";
        }

        /// <summary>
        /// 连接托管器
        /// </summary>
        /// <remarks>此托管器提供了一个打开的数据库连接，当委托完成时，连接会自动销毁。</remarks>
        /// <typeparam name="T">返回值类型</typeparam>
        /// <param name="todo">委托</param>
        /// <returns></returns>
        public T DoInConnection<T>(Func<MySqlConnection, T> todo)
        {
            MySqlConnection conn = Connection;
            T result = todo(conn);
            conn.Close();
            return result;
        }
        /// <summary>
        /// 命令托管器
        /// </summary>
        /// <remarks>此托管器提供了一个数据库命令，当委托完成时，命令会自动销毁。</remarks>
        /// <typeparam name="T">返回值类型</typeparam>
        /// <param name="Connection">承载命令的数据库连接</param>
        /// <param name="todo">委托</param>
        /// <returns></returns>
        public static T DoInCommand<T>(MySqlConnection Connection, Func<MySqlCommand, T> todo)
        {
            MySqlCommand Command = Connection.CreateCommand();
            T result = todo(Command);
            Command.Dispose();
            return result;
        }
        /// <summary>
        /// 事务托管器
        /// </summary>
        /// <remarks>此托管器提供了一个已经启动的事务，当委托完成时，事务会自动销毁。事务的提交等操作需在委托中完成。</remarks>
        /// <typeparam name="T">返回值类型</typeparam>
        /// <param name="Command">承载事务的数据库命令</param>
        /// <param name="todo">委托</param>
        /// <returns></returns>
        public static T DoInTransaction<T>(MySqlCommand Command, Func<MySqlTransaction, T> todo)
        {
            Command.Transaction = Command.Connection.BeginTransaction();
            T result = todo(Command.Transaction);
            Command.Transaction.Dispose();
            return result;
        }

        /// <summary>
        /// 获取单张数据表
        /// </summary>
        /// <param name="SQL">SQL语句，用于查询数据表</param>
        /// <returns>返回一个DataTable对象，无结果或错误则返回null</returns>
        public DataTable GetTable(string SQL)
        {
            return DoInConnection(conn =>
            {
                using DataTable table = new DataTable();
                new MySqlDataAdapter(SQL, conn).Fill(table);

                return table;
            });
        }
        /// <summary>
        /// 获取单张数据表（适用于参数化查询）
        /// </summary>
        /// <param name="SQL">携带查询参数的SQL语句</param>
        /// <param name="parameters">查询参数列表</param>
        /// <returns>返回一个DataTable对象，无结果或错误则返回null</returns>
        public DataTable GetTable(string SQL, params MySqlParameter[] parameters)
        {
            return DoInConnection(conn =>
            {
                return DoInCommand(conn, cmd =>
                {
                    using DataTable table = new DataTable();
                    cmd.CommandText = SQL;
                    cmd.Parameters.AddRange(parameters);/* 添加参数 */

                    new MySqlDataAdapter(cmd).Fill(table);
                    return table;
                });
            });
        }

        /// <summary>
        /// 取得首个键值（键匹配查询）
        /// </summary>
        /// <param name="SQL">SQL语句</param>
        /// <returns>返回结果集中的第一行第一列，若查询无果或异常则返回null</returns>
        public object GetKey(string SQL)
        {
            return DoInConnection(conn =>
            {
                return DoInCommand(conn, cmd =>
                {
                    cmd.CommandText = SQL;
                    /* 如果结果集为空，该方法返回null */
                    return cmd.ExecuteScalar(); ;
                });
            });
        }
        /// <summary>
        /// 取得首个键值（键匹配查询）
        /// </summary>
        /// <param name="SQL">携带查询参数的SQL语句</param>
        /// <param name="parameters">查询参数列表</param>
        /// <returns>返回结果集中的第一行第一列，若查询无果或异常则返回null</returns>
        public object GetKey(string SQL, params MySqlParameter[] parameters)
        {
            return DoInConnection(conn =>
            {
                return DoInCommand(conn, cmd =>
                {
                    cmd.CommandText = SQL;
                    cmd.Parameters.AddRange(parameters);

                    /* 如果结果集为空，该方法返回null */
                    return cmd.ExecuteScalar(); ;
                });
            });
        }
        /// <summary>
        /// 取得指定键值（键匹配查询）
        /// </summary>
        /// <param name="MySqlKey">操作定位器</param>
        /// <param name="KeyName">键名</param>
        /// <returns>返回结果集中的第一行第一列，若查询无果或异常则返回null</returns>
        public object GetKey((string Table, string Name, object Val) MySqlKey, string KeyName)
        {
            return DoInConnection(conn =>
            {
                return DoInCommand(conn, cmd =>
                {
                    cmd.CommandText = $"SELECT {KeyName} FROM {MySqlKey.Table} WHERE {MySqlKey.Name}='{MySqlKey.Val}';";
                    /* 如果结果集为空，该方法返回null */
                    return cmd.ExecuteScalar();
                });
            });
        }


        /// <summary>
        /// 获得数据行
        /// </summary>
        /// <param name="SQL">SQL语句</param>
        /// <returns>操作异常或目标行不存在时，返回null</returns>
        public DataRow GetRow(string SQL)
        {
            /* 数组越界防止 */
            DataRowCollection collection = GetTable(SQL).Rows;
            return collection.Count == 0 ? null : collection[0];
        }
        /// <summary>
        /// 获得数据行（适用于参数化查询）
        /// </summary>
        /// <param name="SQL">携带查询参数的SQL语句</param>
        /// <param name="parameters">查询参数列表</param>
        /// <returns>操作异常或目标行不存在时，返回null</returns>
        public DataRow GetRow(string SQL, params MySqlParameter[] parameters)
        {
            DataRowCollection collection = GetTable(SQL, parameters).Rows;
            return collection.Count == 0 ? null : collection[0];
        }
        /// <summary>
        /// 从DataTable中提取指定行
        /// </summary>
        /// <param name="DataTable">数据表实例</param>
        /// <param name="KeyName">键名</param>
        /// <param name="KeyValue">键值</param>
        /// <returns>返回获得的DataRow数据行实例，表为空或未检索到返回null</returns>
        public static DataRow GetRow(DataTable DataTable, string KeyName, object KeyValue)
        {
            foreach (DataRow DataRow in DataTable.Rows)
            {
                /* 全部转为string来判断是否相等，因为object箱结构不一样 */
                if (DataRow[KeyName].ToString() == KeyValue.ToString())
                {
                    return DataRow;/* 返回符合被检索主键的行 */
                }
            }
            return null;/* 未检索到 */
        }


        /// <summary>
        /// 取得查询结果中的第一列
        /// </summary>
        /// <typeparam name="T">元素类型</typeparam>
        /// <param name="SQL">用于查询的SQL语句</param>
        /// <returns></returns>
        public List<T> GetColumn<T>(string SQL)
        {
            return GetColumn<T>(GetTable(SQL));
        }
        /// <summary>
        /// 取得查询结果中的指定列
        /// </summary>
        /// <typeparam name="T">元素类型</typeparam>
        /// <param name="SQL">用于查询的SQL语句</param>
        /// <param name="Key">目标列键名</param>
        /// <returns></returns>
        public List<T> GetColumn<T>(string SQL, string Key)
        {
            return GetColumn<T>(GetTable(SQL), Key);
        }
        /// <summary>
        /// 取得查询结果中的第一列
        /// </summary>
        /// <typeparam name="T">元素类型</typeparam>
        /// <param name="SQL">携带查询参数的SQL语句</param>
        /// <param name="parameters">查询参数列表</param>
        /// <returns></returns>
        public List<T> GetColumn<T>(string SQL, params MySqlParameter[] parameters)
        {
            return GetColumn<T>(GetTable(SQL, parameters));
        }
        /// <summary>
        /// 取得查询结果中的指定列
        /// </summary>
        /// <typeparam name="T">元素类型</typeparam>
        /// <param name="SQL">携带查询参数的SQL语句</param>
        /// <param name="parameters">查询参数列表</param>
        /// <param name="Key">目标列键名</param>
        /// <returns></returns>
        public List<T> GetColumn<T>(string SQL, string Key, params MySqlParameter[] parameters)
        {
            return GetColumn<T>(GetTable(SQL, parameters), Key);
        }
        /// <summary>
        /// 从DataTable中提取第一列（此方法无空值判断）
        /// </summary>
        /// <typeparam name="T">元素类型</typeparam>
        /// <param name="DataTable">数据表实例</param>
        /// <returns></returns>
        public static List<T> GetColumn<T>(DataTable DataTable)
        {
            List<T> List = new List<T>();

            foreach (DataRow DataRow in DataTable.Rows)
            {
                List.Add((T)Convert.ChangeType(DataRow[0], typeof(T)));
            }
            return List;
        }
        /// <summary>
        /// 从DataTable中提取指定列（此方法无空值判断）
        /// </summary>
        /// <typeparam name="T">元素类型</typeparam>
        /// <param name="DataTable">数据表实例</param>
        /// <param name="Key">目标列键名</param>
        /// <returns>返回非泛型List{object}实例</returns>
        public static List<T> GetColumn<T>(DataTable DataTable, string Key)
        {
            List<T> List = new List<T>();

            foreach (DataRow DataRow in DataTable.Rows)
            {
                List.Add((T)Convert.ChangeType(DataRow[Key], typeof(T)));
            }
            return List;
        }

        /// <summary>
        /// 执行任意SQL语句
        /// </summary>
        /// <remarks>此方法中的所有SQL语句将在同一个事务内进行，SQL语句中任何一部分的执行失败都将导致整个事务被回滚。</remarks>
        /// <param name="SQL">SQL语句</param>
        /// <returns>返回受影响的行数</returns>
        public int ExecuteAny(string SQL)
        {
            return DoInConnection(conn =>
            {
                return DoInCommand(conn, cmd =>
                {
                    cmd.CommandText = SQL;

                    return DoInTransaction(cmd, tx =>
                    {
                        int AffectedRows = cmd.ExecuteNonQuery();
                        tx.Commit();/*提交事务*/

                        return AffectedRows;
                    });
                });
            });
        }
        /// <summary>
        /// 执行任意SQL语句
        /// </summary>
        /// <remarks>此方法中的所有SQL语句将在同一个事务内进行，SQL语句中任何一部分的执行失败都将导致整个事务被回滚。</remarks>
        /// <param name="SQL">SQL语句</param>
        /// <param name="parameters">查询参数列表</param>
        /// <returns>返回受影响的行数</returns>
        public int ExecuteAny(string SQL, params MySqlParameter[] parameters)
        {
            return DoInConnection(conn =>
            {
                return DoInCommand(conn, cmd =>
                {
                    cmd.CommandText = SQL;
                    cmd.Parameters.AddRange(parameters);/* 添加参数 */

                    return DoInTransaction(cmd, tx =>
                    {
                        int AffectedRows = cmd.ExecuteNonQuery();
                        tx.Commit();/*提交事务*/

                        return AffectedRows;
                    });
                });
            });
        }

        /// <summary>
        /// 更新操作
        /// </summary>
        /// <remarks>仅允许更新一条记录，在其他情况下事务将被回滚。只保证Value上的参数化查询。</remarks>
        /// <param name="Table">目标表</param>
        /// <param name="SET">被更新键值对</param>
        /// <param name="WHERE">定位键值对</param>
        /// <returns></returns>
        public bool ExecuteUpdate(string Table, (string K, object V) SET, (string K, object V) WHERE)
        {
            return DoInConnection(conn =>
            {
                return DoInCommand(conn, cmd =>
                {
                    cmd.CommandText = $"UPDATE {Table} SET {SET.K}=?SET_V WHERE {WHERE.K}=?WHERE_V";
                    cmd.Parameters.AddWithValue("SET_V", SET.V);
                    cmd.Parameters.AddWithValue("WHERE_V", WHERE.V);

                    return DoInTransaction(cmd, tx =>
                    {
                        if (cmd.ExecuteNonQuery() == 1)
                        {
                            tx.Commit();
                            return true;
                        }
                        else
                        {
                            tx.Rollback();
                            return false;
                        }
                    });
                });
            });
        }
        /// <summary>
        /// 更新操作
        /// </summary>
        /// <remarks>仅允许更新一条记录，在其他情况下事务将被回滚。只保证Value上的参数化查询。</remarks>
        /// <param name="Table">目标表</param>
        /// <param name="SET">被更新键值对</param>
        /// <param name="OldValue">被更新键的旧值</param>
        /// <returns>是否操作成功</returns>
        public bool ExecuteUpdate(string Table, (string K, object V) SET, object OldValue)
        {
            return ExecuteUpdate(Table, SET, new(SET.K, OldValue));
        }
        /// <summary>
        /// 插入操作
        /// </summary>
        /// <remarks>仅允许更新一条记录，在其他情况下事务将被回滚。只保证Value上的参数化查询。</remarks>
        /// <param name="Table">目标表</param>
        /// <param name="Pairs">键值对</param>
        /// <returns>返回插入的成功与否</returns>
        public bool ExecuteInsert(string Table, params (string K, object V)[] Pairs)
        {
            return DoInConnection(conn =>
            {
                return DoInCommand(conn, cmd =>
                {
                    string part1 = "";/* VALUES语句前半部分 */
                    string part2 = "";/* VALUES语句后半部分 */
                    foreach (var (K, V) in Pairs)
                    {
                        part1 += $"`{K}`,";
                        part2 += $"?{K} ,";

                        cmd.Parameters.AddWithValue(K, V);/* 参数添加 */
                    }
                    part1 = part1[0..^1];/* 末尾逗号去除 */
                    part2 = part2[0..^1];
                    cmd.CommandText = $"INSERT INTO {Table} ({part1})VALUES({part2})";

                    return DoInTransaction(cmd, tx =>
                    {
                        if (cmd.ExecuteNonQuery() == 1)
                        {
                            tx.Commit();
                            return true;
                        }
                        else
                        {
                            tx.Rollback();
                            return false;
                        }
                    });
                });
            });
        }
        /// <summary>
        /// 删除操作
        /// </summary>
        /// <remarks>仅允许删除一条记录，在其他情况下事务将被回滚。只保证Value上的参数化查询。</remarks>
        /// <param name="Table">目标表</param>
        /// <param name="Pair">键值对，满足此条件的行将被删除</param>
        /// <returns></returns>
        public bool ExecuteDelete(string Table, (string K, object V) Pair)
        {
            return DoInConnection(conn =>
            {
                return DoInCommand(conn, cmd =>
                {
                    cmd.CommandText = $"DELETE FROM {Table} WHERE `{Pair.K}`=?Value";
                    cmd.Parameters.AddWithValue("Value", Pair.V);/* 参数添加 */

                    return DoInTransaction(cmd, tx =>
                    {
                        if (cmd.ExecuteNonQuery() == 1)
                        {
                            tx.Commit();
                            return true;
                        }
                        else
                        {
                            tx.Rollback();
                            return false;
                        }
                    });
                });
            });
        }
    }

    /// <summary>
    /// 扩展方法
    /// </summary>
    public static class MySqlManager_Extension
    {
        /// <summary>
        /// 执行任意SQL语句
        /// </summary>
        /// <remarks>此方法中的所有SQL语句将在同一个事务内进行，SQL语句中任何一部分的执行失败都将导致整个事务被回滚。</remarks>
        /// <param name="cmd">承载命令</param>
        /// <param name="SQL">SQL语句</param>
        /// <returns>返回受影响的行数</returns>
        public static int ExecuteAny(this MySqlCommand cmd, string SQL)
        {
            cmd.CommandText = SQL;
            return cmd.ExecuteNonQuery();
        }
        /// <summary>
        /// 执行任意SQL语句
        /// </summary>
        /// <remarks>此方法中的所有SQL语句将在同一个事务内进行，SQL语句中任何一部分的执行失败都将导致整个事务被回滚。</remarks>
        /// <param name="cmd">承载命令</param>
        /// <param name="SQL">SQL语句</param>
        /// <param name="parameters">查询参数列表</param>
        /// <returns>返回受影响的行数</returns>
        public static int ExecuteAny(this MySqlCommand cmd, string SQL, params MySqlParameter[] parameters)
        {
            cmd.CommandText = SQL;
            cmd.Parameters.AddRange(parameters);/* 添加参数 */

            return cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// 更新操作
        /// </summary>
        /// <remarks>仅允许更新一条记录，在其他情况下事务将被回滚。只保证Value上的参数化查询。</remarks>
        /// <param name="cmd">承载命令</param>
        /// <param name="Table">目标表</param>
        /// <param name="SET">被更新键值对</param>
        /// <param name="WHERE">定位键值对</param>
        /// <returns></returns>
        public static int ExecuteUpdate(this MySqlCommand cmd, string Table, (string K, object V) SET, (string K, object V) WHERE)
        {
            cmd.CommandText = $"UPDATE {Table} SET {SET.K}=?SET_V WHERE {WHERE.K}=?WHERE_V";
            cmd.Parameters.AddWithValue("SET_V", SET.V);
            cmd.Parameters.AddWithValue("WHERE_V", WHERE.V);

            return cmd.ExecuteNonQuery();
        }
        /// <summary>
        /// 更新操作
        /// </summary>
        /// <remarks>仅允许更新一条记录，在其他情况下事务将被回滚。只保证Value上的参数化查询。</remarks>
        /// <param name="cmd">事务</param>
        /// <param name="Table">目标表</param>
        /// <param name="SET">被更新键值对</param>
        /// <param name="OldValue">被更新键的旧值</param>
        /// <returns>是否操作成功</returns>
        public static int ExecuteUpdate(this MySqlCommand cmd, string Table, (string K, object V) SET, object OldValue)
        {
            return ExecuteUpdate(cmd, Table, SET, new(SET.K, OldValue));
        }
        /// <summary>
        /// 插入操作
        /// </summary>
        /// <remarks>仅允许更新一条记录，在其他情况下事务将被回滚。只保证Value上的参数化查询。</remarks>
        /// <param name="cmd">承载命令</param>
        /// <param name="Table">目标表</param>
        /// <param name="Pairs">键值对</param>
        /// <returns>返回插入的成功与否</returns>
        public static int ExecuteInsert(this MySqlCommand cmd, string Table, params (string K, object V)[] Pairs)
        {
            string part1 = "";/* VALUES语句前半部分 */
            string part2 = "";/* VALUES语句后半部分 */
            foreach (var (K, V) in Pairs)
            {
                part1 += $"`{K}`,";
                part2 += $"?{K} ,";

                cmd.Parameters.AddWithValue(K, V);/* 参数添加 */
            }
            part1 = part1[0..^1];/* 末尾逗号去除 */
            part2 = part2[0..^1];
            cmd.CommandText = $"INSERT INTO {Table} ({part1})VALUES({part2})";

            return cmd.ExecuteNonQuery();
        }
        /// <summary>
        /// 删除操作
        /// </summary>
        /// <remarks>仅允许删除一条记录，在其他情况下事务将被回滚。只保证Value上的参数化查询。</remarks>
        /// <param name="cmd">承载命令</param>
        /// <param name="Table">目标表</param>
        /// <param name="Pair">键值对，满足此条件的行将被删除</param>
        /// <returns></returns>
        public static int ExecuteDelete(this MySqlCommand cmd, string Table, (string K, object V) Pair)
        {
            cmd.CommandText = $"DELETE FROM {Table} WHERE `{Pair.K}`=?Value";
            cmd.Parameters.AddWithValue("Value", Pair.V);/* 参数添加 */

            return cmd.ExecuteNonQuery();
        }
    }

    /// <summary>
    /// MySql数据库连接信息
    /// 数据源
    /// 数据库
    /// 端口
    /// 用户名
    /// 密码
    /// </summary>
    public record MySqlConnMsg(string DataSource, int Port, string User, string PWD);
}
