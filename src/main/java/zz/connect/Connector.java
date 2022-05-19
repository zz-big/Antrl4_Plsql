package zz.connect;

import org.springframework.stereotype.Service;
import zz.util.MD5Util;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Description:
 *
 * @author zz
 * @date 2021/10/12
 */
@Service
public class Connector {
    private ConcurrentHashMap<String, ConnectionProviderHikariCP> poolMap = new ConcurrentHashMap<String, ConnectionProviderHikariCP>();

    public ConnectionProviderHikariCP getConnInPool(String driver, String url, String username, String password) {
        ConnectionProviderHikariCP jdbcConn = null;

        String key = MD5Util.getMD5Code(driver + url + username + password);

        if (!poolMap.containsKey(key)) {
            ConnectionProviderHikariCP connectionProvider = ConnectionProviderHikariCP.getInstance();
            connectionProvider.init(driver, url, username, password);
            poolMap.put(key, connectionProvider);
        }
        jdbcConn = poolMap.get(key);
        return jdbcConn;
    }
}
