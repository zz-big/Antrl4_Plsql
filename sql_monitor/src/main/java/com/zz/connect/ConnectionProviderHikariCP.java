package com.zz.connect;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Description:
 *
 * @author zz
 * @date 2021/10/12
 */
public class ConnectionProviderHikariCP {
    private Logger logger = LoggerFactory.getLogger(ConnectionProviderHikariCP.class);

    private static final ConnectionProviderHikariCP INSTANCE;
    private final HikariDataSource hikariDataSource;
    private AtomicBoolean initialized;

    //连接只读数据库时配置为true， 保证安全 -->
    private static boolean readOnly = false;
    //等待连接池分配连接的最大时长（毫秒），超过这个时长还没可用的连接则发生SQLException， 缺省:30秒 -->
    private static int connectionTimeout = 30000;
    // 一个连接idle状态的最大时长（毫秒），超时则被释放（retired），缺省:10分钟 -->
    private static int idleTimeout = 60000;
    //一个连接的生命时长（毫秒），超时而且没被使用则被释放（retired），缺省:10分钟，建议设置比数据库超时时长少30秒，参考MySQL wait_timeout参数（show variables like '%timeout%';） -->
    private static int maxLifetime = 60000;
    private static int minimumIdle = 4;
    // 连接池中允许的最大连接数。缺省值：10；推荐的公式：((core_count * 2) + effective_spindle_count) -->
    private static int maximumPoolSize = 15;

    //class initializer:
    static {
        INSTANCE = new ConnectionProviderHikariCP();
    }

    private ConnectionProviderHikariCP() {
        hikariDataSource = new HikariDataSource();
        // initialized = new AtomicBoolean();
    }

    public static ConnectionProviderHikariCP getInstance() {
        // return INSTANCE;
        return new ConnectionProviderHikariCP();
    }

    public void init(String driverClassName, String jdbcUrl, String username, String password) {
        //多数据源创建多个连接池，关闭单例模式
        // if (initialized.compareAndSet(false, true)) {
        //driverClassName
        hikariDataSource.setDriverClassName(driverClassName);
        //database address
        hikariDataSource.setJdbcUrl(jdbcUrl);
        //useName 用户名
        hikariDataSource.setUsername(username);
        //password
        hikariDataSource.setPassword(password);
        //连接只读数据库时配置为true， 保证安全 -->
        hikariDataSource.setReadOnly(readOnly);
        //等待连接池分配连接的最大时长（毫秒），超过这个时长还没可用的连接则发生SQLException， 缺省:30秒 -->
        hikariDataSource.setConnectionTimeout(connectionTimeout);
        // 一个连接idle状态的最大时长（毫秒），超时则被释放（retired），缺省:10分钟 -->
        hikariDataSource.setIdleTimeout(idleTimeout);
        //一个连接的生命时长（毫秒），超时而且没被使用则被释放（retired），缺省:30分钟，建议设置比数据库超时时长少30秒，参考MySQL wait_timeout参数（show variables like '%timeout%';） -->
        hikariDataSource.setMaximumPoolSize(maxLifetime);
        hikariDataSource.setMinimumIdle(minimumIdle);
        // 连接池中允许的最大连接数。缺省值：10；推荐的公式：((core_count * 2) + effective_spindle_count) -->
        hikariDataSource.setMaximumPoolSize(maximumPoolSize);
        // } else {
        //     throw new IllegalStateException("Connection provider already initialized.");
        // }
    }

    public Connection getConnection() throws SQLException {
        // if (initialized.get()) {
        return hikariDataSource.getConnection();
        // }

        // throw new UnsupportedOperationException("Debe inicializar mediante el método Init() primero!!!!!.");
    }


    /**
     * 查询没有任何条件
     *
     * @param sql
     * @return
     */
    public List<Map<String, Object>> excuteQuery(String sql) {
        return excuteQuery(sql, null);
    }

    /**
     * 查询时传一个数据参数
     *
     * @param sql
     * @param obj
     * @return
     */
    public List<Map<String, Object>> excuteQuery(String sql, Object obj) {
        Object[] objs = new Object[1];
        objs[0] = obj;
        return excuteQuery(sql, objs);
    }

    /**
     * 查询时传递多个参数
     *
     * @param sql
     * @param objs
     * @return
     */
    public List<Map<String, Object>> excuteQuery(String sql, Object[] objs) {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        try {
            connection = getConnection();
            ps = connection.prepareStatement(sql);
            if (objs != null) {
                for (int i = 0; i < objs.length; i++) {
                    ps.setObject(i + 1, objs[i]);
                }
            }
            rs = ps.executeQuery();

            if (rs != null) {
                ResultSetMetaData md = rs.getMetaData();
                int columnCount = md.getColumnCount();
                while (rs.next()) {
                    Map<String, Object> rowData = new HashMap<String, Object>();
                    for (int i = 1; i <= columnCount; i++) {
                        rowData.put(md.getColumnName(i), rs.getObject(i));
                    }
                    list.add(rowData);
                }
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            logger.error(e.getMessage(), e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return list;
    }

    /**
     * DML操作，增删改操作
     *
     * @param sql
     * @return
     */
    public int excuteUpdate(String sql) {
        return excuteUpdate(sql, null);
    }

    /**
     * DML操作，增删改操作,传递一个参数
     *
     * @param sql
     * @return
     */
    public int excuteUpdate(String sql, Object obj) {
        Object[] objs = new Object[1];
        objs[0] = obj;
        return excuteUpdate(sql, objs);
    }

    /**
     * DML操作，增删改操作,传递多个参数
     *
     * @param sql
     * @return
     */
    public int excuteUpdate(String sql, Object[] params) {
        int rtn = 0;
        Connection connection = null;
        PreparedStatement ps = null;

        try {
            connection = getConnection();
            ps = connection.prepareStatement(sql);
            connection.setAutoCommit(false);
            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            }

            rtn = ps.executeUpdate();

            connection.commit();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return rtn;
    }

    /**
     * @param sql
     * @param paramsList
     * @return 每条SQL语句影响的行数
     */
    public int[] executeBatch(String sql, List<Object[]> paramsList) {
        int[] rtn = null;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();

            // 第一步：使用Connection对象，取消自动提交
            conn.setAutoCommit(false);

            pstmt = conn.prepareStatement(sql);

            // 第二步：使用PreparedStatement.addBatch()方法加入批量的SQL参数
            if (paramsList != null && paramsList.size() > 0) {
                for (Object[] params : paramsList) {
                    for (int i = 0; i < params.length; i++) {
                        pstmt.setObject(i + 1, params[i]);
                    }
                    pstmt.addBatch();
                }
            }

            // 第三步：使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
            rtn = pstmt.executeBatch();

            // 最后一步：使用Connection对象，提交批量的SQL语句
            conn.commit();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return rtn;
    }

    public boolean exec(String sql) throws SQLException {
        Connection conn = null;
        boolean execute = false;
        Statement statement = null;
        try {
            conn = getConnection();
            statement = conn.createStatement();
            execute = statement.execute(sql);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new SQLException(e);
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return execute;
    }

}
