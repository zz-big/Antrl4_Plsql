package com.zz.conn;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Description:
 *
 * @author zz
 * @date 2021/10/12
 */
public interface ConnectionProvider {
    void init(String driverClassName, String jdbcUrl, String username, String password);
    Connection getConnection() throws SQLException;
}
