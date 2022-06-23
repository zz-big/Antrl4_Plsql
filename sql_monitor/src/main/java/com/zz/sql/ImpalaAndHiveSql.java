package com.zz.sql;

import com.alibaba.druid.DbType;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zz.connect.ConnectionProviderHikariCP;
import com.zz.connect.CusConnector;
import com.zz.util.HttpUtils;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zz.util.SqlUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Description:
 *
 * @author zz
 * @date 2022/5/17
 */
public class ImpalaAndHiveSql {
    static Logger logger = LoggerFactory.getLogger(ImpalaAndHiveSql.class);
    //定时器
    private static ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) {

        //mysql
        CusConnector connector = new CusConnector();
        String driver = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://pd-cdh-192-168-0-10-node:3306/sql_monitor?useSSL=false";
        String usernameMysql = "root";
        String passwordMysql = "pdroot21";

        //cdh
        String username = "admin";
        String password = "admin";

        SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat dataFormatSql = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        dataFormat.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        dataFormatSql.setTimeZone(TimeZone.getTimeZone("GMT+8"));

        ConnectionProviderHikariCP connInPool = connector.getConnInPool(driver, url, usernameMysql, passwordMysql);
        ImpalaAndHiveSql impalaAndHiveSql = new ImpalaAndHiveSql();

        //程序挂掉再启动补数据
        Object impalaTimeMax = connInPool.excuteQuery("select  max(update_time) as max_time from sql_monitor.impala_sql_monitor").get(0).get("max_time");
        Object hiveTimeMax = connInPool.excuteQuery("select  max(update_time) as max_time from sql_monitor.hive_sql_monitor").get(0).get("max_time");
        long endTime = System.currentTimeMillis();
        long finalEndTime = endTime;
        try {

            long impalaTime = dataFormatSql.parse(String.valueOf(impalaTimeMax)).getTime();
            long hiveTime = dataFormatSql.parse(String.valueOf(hiveTimeMax)).getTime();
            long maxTime = Math.max(impalaTime, hiveTime);
            //最大时间和现在差值大于10min
            if ((endTime - maxTime) > 600000) {
                long startTime = endTime - 600000;
                while (maxTime < startTime) {
                    String batchTime = dataFormat.format(endTime);
                    System.out.println("Recover historical data!");
                    impalaAndHiveSql.exec(username, password, dataFormat, connInPool, startTime, endTime, batchTime);
                    endTime -= 600001;
                    startTime = endTime - 600000;
                }
                //还差maxTime ~ endTime 这一段不到10分钟的
                String batchTime = dataFormat.format(endTime);
                System.out.println("Recover historical data!");
                impalaAndHiveSql.exec(username, password, dataFormat, connInPool, maxTime, endTime, batchTime);
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }

        scheduledExecutor.scheduleWithFixedDelay(new TimerTask() {
            long startTime = finalEndTime - 600000;
//            long startTime = new Date().getTime() - 3600000;

            @Override
            public void run() {
                //毫秒

                String batchTime = dataFormat.format(finalEndTime);
                logger.info("starttime-->" + startTime);
                logger.info("endTime-->" + finalEndTime);
                logger.info("batchTime-->" + batchTime);

                impalaAndHiveSql.exec(username, password, dataFormat, connInPool, startTime, finalEndTime, batchTime);
                startTime = finalEndTime + 1;
            }
        }, 600, 600, TimeUnit.SECONDS);
    }

    public void exec(String username, String password, SimpleDateFormat dataFormat, ConnectionProviderHikariCP connInPool, long startTime, long endTime, String batchTime) {

        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
                new AuthScope("pd-cdh-192-168-0-3-node", AuthScope.ANY_PORT),
                new UsernamePasswordCredentials(username, password));
        CloseableHttpClient httpclient = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .build();


        try {
            ArrayList<String> impalaSqls = getImpalaSql(httpclient, endTime, startTime, connInPool, dataFormat, batchTime);
            ArrayList<String> hiveSqls = getHiveSql(httpclient, endTime, startTime, connInPool, dataFormat, batchTime);
            parserSql(connInPool, batchTime);

        } finally {
            try {
                httpclient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public ArrayList<String> getImpalaSql(CloseableHttpClient httpclient, long endTime, long startTime, ConnectionProviderHikariCP connInPool, SimpleDateFormat dataFormat, String batchTime) {
//            String encode = new URLCodec().encode("statement RLIKE \".*select.*\"");
//            HttpGet httpget = new HttpGet("http://pd-cdh-192-168-0-3-node:7180/api/v9/clusters/pd_cluster/services/impala/impalaQueries?startTime="
//            +startTime  + "&endTime=" +endTime  + "&offset=0&limit=10&filters="+encode );
        //impala utc时区
        SimpleDateFormat dataFormat1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dataFormat1.setTimeZone(TimeZone.getTimeZone("UTC"));
        String from = dataFormat1.format(new Date(startTime));
        String to = dataFormat1.format(new Date(endTime));
        logger.info("impala fromTime-->" + from);
        logger.info("impala toTime-->" + to);
        HttpGet httpget = new HttpGet("http://pd-cdh-192-168-0-3-node:7180/api/v9/clusters/pd_cluster/services/impala/impalaQueries?from="
                + from + "&to=" + to + "&offset=0&limit=1000");

        String sql = "replace into impala_sql_monitor(query_id,`database`,`sql`,query_state,connected_user,oom,ddl_type,query_type,start_time,end_time,duration_millis,update_time) values" +
                "(?,?,?,?,?,?,?,?,?,?,?,?)";

        CloseableHttpResponse response = null;
        ArrayList<String> sqlsList = new ArrayList<String>();
        Connection connection = null;

        try {
            response = httpclient.execute(httpget);
            JSONObject jsonObject = JSONObject.parseObject(EntityUtils.toString(response.getEntity()));
            JSONArray queries = jsonObject.getJSONArray("queries");

            connection = connInPool.getConnection();
            PreparedStatement ps = connection.prepareStatement(sql);

            for (int i = 0; i < queries.size(); i++) {
                JSONObject queriesJSONObject = queries.getJSONObject(i);
                String database = queriesJSONObject.getString("database");
                String statement = queriesJSONObject.getString("statement");
                String queryState = queriesJSONObject.getString("queryState");
                String endTimeQuery = "";
                if (!queryState.equals("RUNNING")) {
                    endTimeQuery = queriesJSONObject.getString("endTime");
                    ps.setObject(10, dataFormat.format(dataFormat1.parse(endTimeQuery)));
                } else {
                    ps.setObject(10, null);
                }
                JSONObject attributes = queriesJSONObject.getJSONObject("attributes");
                String connectedUser = queriesJSONObject.getString("user");
                String oom = attributes.getString("oom");
                String ddlType = attributes.getString("ddl_type");
                String startTimeQuery = queriesJSONObject.getString("startTime");
                String durationMillis = queriesJSONObject.getString("durationMillis");
                String queryId = queriesJSONObject.getString("queryId");
                String queryType = queriesJSONObject.getString("queryType");

                ps.setObject(1, queryId);
                ps.setObject(2, database);
                ps.setObject(3, statement);
                ps.setObject(4, queryState);
                ps.setObject(5, connectedUser);
                ps.setObject(6, Boolean.parseBoolean(oom));
                ps.setObject(7, ddlType);
                ps.setObject(8, queryType);
                ps.setObject(9, dataFormat.format(dataFormat1.parse(startTimeQuery)));

                ps.setObject(11, durationMillis);
                ps.setObject(12, batchTime);

                sqlsList.add(statement);
                //"攒"sql
                ps.addBatch();
                if (i % 50 == 0) {
                    //2.执行batch
                    ps.executeBatch();
                    //3.清空batch
                    ps.clearBatch();
                }
            }
            ps.executeBatch();
            ps.clearBatch();
            logger.info("impala execute successfully!");
        } catch (IOException | SQLException | ParseException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                response.close();
                connection.close();
            } catch (IOException | SQLException e) {
                e.printStackTrace();
            }
        }
        return sqlsList;
    }


    public ArrayList<String> getHiveSql(CloseableHttpClient httpclient, long endTime, long startTime, ConnectionProviderHikariCP connInPool, SimpleDateFormat dataFormat, String batchTime) {
//        String url = "http://192-168-0-3:7180/cmf/yarn/completedApplications?" +
//                "startTime=" + startTime + "&endTime=" + endTime +
//                "&filters=hive_query_id%20RLIKE%20%22.*%22&offset=0&limit=1000" +
//                "&serviceName=yarn" +
//                "&histogramAttributes=adl_bytes_read%2Cadl_bytes_written%2Ccpu_milliseconds%2Cs3a_bytes_read%2Cs3a_bytes_written%2Cused_memory_max%2Cmb_millis%2Chdfs_bytes_written%2Cfile_bytes_written%2Callocated_vcore_seconds%2Callocated_memory_seconds%2Capplication_duration%2Cunused_vcore_seconds%2Cunused_memory_seconds%2Cpool%2Cuser%2Chdfs_bytes_read%2Cfile_bytes_read";
//        HttpGet httpget = new HttpGet(url);
        SimpleDateFormat dataFormat1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dataFormat1.setTimeZone(TimeZone.getTimeZone("UTC"));
        String from = dataFormat1.format(new Date(startTime));
        String to = dataFormat1.format(new Date(endTime));
        logger.info("hive fromTime-->" + from);
        logger.info("hive toTime-->" + to);


        HttpGet httpget = new HttpGet("http://pd-cdh-192-168-0-3-node:7180/api/v9/clusters/pd_cluster/services/yarn/yarnApplications?" +
                "from=" + from + "&to=" + to +
                "&offset=0&limit=1000");
        String sql = "replace into hive_sql_monitor(job_id,query_id,`user`,start_time,end_time,`state`,pool,`sql`,update_time) values" +
                "(?,?,?,?,?,?,?,?,?)";

        HashMap<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Accept", "application/json");

        CloseableHttpResponse response = null;
        ArrayList<String> sqlsList = new ArrayList<String>();
        Connection connection = null;

        try {
            response = httpclient.execute(httpget);
//            String result = HttpUtils.getAccessByAuth(url, headers, username, password);
            JSONObject jsonObject = JSONObject.parseObject(EntityUtils.toString(response.getEntity()));
            JSONArray applications = jsonObject.getJSONArray("applications");

            connection = connInPool.getConnection();
            PreparedStatement ps = connection.prepareStatement(sql);

            for (int i = 0; i < applications.size(); i++) {
                JSONObject queriesJSONObject = applications.getJSONObject(i);
                JSONObject attributes = queriesJSONObject.getJSONObject("attributes");

                String id = queriesJSONObject.getString("applicationId");
                String user = queriesJSONObject.getString("user");
                String state = queriesJSONObject.getString("state");
                String startTimeQuery = queriesJSONObject.getString("startTime");
                String endTimeQuery = null;
                String hiveQueryId = null;
                String hiveQueryString = null;

                if (!state.equals("RUNNING")) {
                    endTimeQuery = queriesJSONObject.getString("endTime");
                    ps.setObject(5, dataFormat.format(dataFormat1.parse(endTimeQuery)));
                    hiveQueryId = attributes.getString("hive_query_id");
                    hiveQueryString = attributes.getString("hive_query_string");
                } else {
                    ps.setObject(5, null);
                }

                String pool = queriesJSONObject.getString("pool");


                ps.setObject(1, id);
                ps.setObject(2, hiveQueryId);
                ps.setObject(3, user);
                ps.setObject(4, dataFormat.format(dataFormat1.parse(startTimeQuery)));

                ps.setObject(6, state);
                ps.setObject(7, pool);
                ps.setObject(8, hiveQueryString);
                ps.setObject(9, batchTime);

                sqlsList.add(hiveQueryString);

                //"攒"sql
                ps.addBatch();
                if (i % 50 == 0) {
                    //2.执行batch
                    ps.executeBatch();
                    //3.清空batch
                    ps.clearBatch();
                }
            }
            ps.executeBatch();
            ps.clearBatch();
            logger.info("hive execute successfully!");
        } catch (SQLException sqlException) {
            logger.error(sqlException.getMessage());
            sqlException.printStackTrace();
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return sqlsList;
    }

    public void parserSql(ConnectionProviderHikariCP connInPool, String batchTime) {

        String insertSql = "insert into tables_statistics(table_name,times,source,update_time) values(?,?,?,?)";
        String insertErrSql = "insert into err_parser_sql(`sql`,source,create_time) values(?,?,?)";
        List<Map<String, Object>> hiveSqls = connInPool.excuteQuery("select `sql`  from hive_sql_monitor  where update_time =\'" + batchTime + "\' and `state` != 'RUNNING' group  by query_id, `sql`");
        List<Map<String, Object>> impalaSqls = connInPool.excuteQuery("select `sql`  from impala_sql_monitor where update_time =\'" + batchTime + "\' and query_state='FINISHED' and query_type='QUERY' group  by query_id ");

        ArrayList<String> tableListHive = new ArrayList<String>();
        ArrayList<String> tableListImpala = new ArrayList<String>();
        ArrayList<String> errSqlListHive = new ArrayList<String>();
        ArrayList<String> errSqlListImpala = new ArrayList<String>();
//        ParseDriver pd = new ParseDriver();

        for (Map<String, Object> map : impalaSqls) {
            String sql = map.values().toArray()[0].toString();
//            ArrayList<String> tableListOne = new ArrayList<String>();
            try {
//                ASTNode ast = pd.parse(sql);
//                String strTree = ast.toStringTree();
//                getTableList(strTree, tableListOne);
//                List<String> listWithoutDuplicates = tableListOne.stream().distinct().collect(Collectors.toList());
                List<String> tableNameList = SqlUtils.getTableNameList(sql, DbType.hive);
                tableListImpala.addAll(tableNameList);

            } catch (Exception e) {
                logger.error(e.getMessage());
                e.printStackTrace();
                errSqlListImpala.add(sql);
            }
        }

        for (Map<String, Object> map : hiveSqls) {
            String sql = map.values().toArray()[0].toString();
//            ArrayList<String> tableListOne = new ArrayList<String>();
            try {
//                ASTNode ast = pd.parse(sql);
//                String strTree = ast.toStringTree();
//                getTableList(strTree, tableListOne);
//                List<String> listWithoutDuplicates = tableListOne.stream().distinct().collect(Collectors.toList());

                List<String> tableNameList = SqlUtils.getTableNameList(sql, DbType.hive);
                tableListHive.addAll(tableNameList);

            } catch (Exception e) {
                logger.error(e.getMessage());
                e.printStackTrace();
                errSqlListHive.add(sql);
            }
        }

        Map<Object, Long> tableTimesMapHive = tableListHive.stream().collect(Collectors.groupingBy(p -> p, Collectors.counting()));
        Map<Object, Long> tableTimesMapImpala = tableListImpala.stream().collect(Collectors.groupingBy(p -> p, Collectors.counting()));

        List<Object[]> objectsInsertSql = new ArrayList<>();
        List<Object[]> objectsErrSql = new ArrayList<>();

        for (Map.Entry<Object, Long> entry : tableTimesMapHive.entrySet()) {
            Object[] value = {entry.getKey(), entry.getValue(), "hive", batchTime};
            objectsInsertSql.add(value);
        }

        for (Map.Entry<Object, Long> entry : tableTimesMapImpala.entrySet()) {
            Object[] value = {entry.getKey(), entry.getValue(), "impala", batchTime};
            objectsInsertSql.add(value);
        }

        for (String errSql : errSqlListImpala) {
            Object[] value = {errSql, "impala", batchTime};
            objectsErrSql.add(value);
        }
        for (String errSql : errSqlListHive) {
            Object[] value = {errSql, "hive", batchTime};
            objectsErrSql.add(value);
        }

        connInPool.executeBatch(insertSql, objectsInsertSql);
        connInPool.executeBatch(insertErrSql, objectsErrSql);
    }

    public static void getTableList(String strTree, ArrayList<String> list) {

        int i1 = strTree.indexOf("tok_tabname");
        String substring1 = "";
        String substring2 = "";
        if (i1 > 0) {
            substring1 = strTree.substring(i1 + 12);
            int i2 = substring1.indexOf(")");
            substring2 = substring1.substring(0, i2);
//            System.out.println(substring2);
            String[] split = substring2.split(" ");
            if (split.length == 2) {
                substring2 = split[1];
            } else {
                substring2 = split[0];
            }
            list.add(substring2);
            getTableList(substring1, list);
        }
    }

//    public ArrayList<String> getHiveSql(String username, String password, long endTime, long startTime, ConnectionProviderHikariCP connInPool, SimpleDateFormat dataFormat, String batchTime) {
////        String url = "http://192-168-0-3:7180/cmf/yarn/completedApplications?" +
////                "startTime=" + startTime + "&endTime=" + endTime +
////                "&filters=hive_query_id%20RLIKE%20%22.*%22&offset=0&limit=1000" +
////                "&serviceName=yarn" +
////                "&histogramAttributes=adl_bytes_read%2Cadl_bytes_written%2Ccpu_milliseconds%2Cs3a_bytes_read%2Cs3a_bytes_written%2Cused_memory_max%2Cmb_millis%2Chdfs_bytes_written%2Cfile_bytes_written%2Callocated_vcore_seconds%2Callocated_memory_seconds%2Capplication_duration%2Cunused_vcore_seconds%2Cunused_memory_seconds%2Cpool%2Cuser%2Chdfs_bytes_read%2Cfile_bytes_read";
//        String url = "http://192-168-0-3:7180/api/v9/clusters/pd_cluster/services/yarn/yarnApplications?" +
//                "startTime=" + startTime + "&endTime=" + endTime +
//                "&offset=0&limit=1000";
//        String sql = "replace into hive_sql_monitor(job_id,query_id,`user`,start_time,end_time,isFailed,completed,pool,`sql`,update_time) values" +
//                "(?,?,?,?,?,?,?,?,?,?)";
//
//        HashMap<String, String> headers = new HashMap<>();
//        headers.put("Content-Type", "application/json");
//        headers.put("Accept", "application/json");
//
//        ArrayList<String> sqlsList = new ArrayList<String>();
//        Connection connection = null;
//
//        try {
//            String result = HttpUtils.getAccessByAuth(url, headers, username, password);
//            JSONObject jsonObject = JSONObject.parseObject(result);
//            JSONArray items = jsonObject.getJSONArray("items");
//
//            connection = connInPool.getConnection();
//            PreparedStatement ps = connection.prepareStatement(sql);
//
//            for (int i = 0; i < items.size(); i++) {
//                JSONObject queriesJSONObject = items.getJSONObject(i);
//                String isFailed = queriesJSONObject.getString("isFailed");
//                String startTimeQuery = queriesJSONObject.getJSONObject("startTime").getString("millis");
//                String endTimeQuery = queriesJSONObject.getJSONObject("endTime").getString("millis");
//                String id = queriesJSONObject.getString("id");
//
//                JSONObject syntheticAttributes = queriesJSONObject.getJSONObject("syntheticAttributes");
//                String hiveQueryId = syntheticAttributes.getString("hive_query_id");
//                String hiveQueryString = syntheticAttributes.getString("hive_query_string");
//
//                String pool = queriesJSONObject.getString("pool");
//                String completed = queriesJSONObject.getString("completed");
//                String user = queriesJSONObject.getString("user");
//
//                ps.setObject(1, id);
//                ps.setObject(2, hiveQueryId);
//                ps.setObject(3, user);
//                ps.setObject(4, dataFormat.format(new Date(Long.valueOf(startTimeQuery))));
//                ps.setObject(5, dataFormat.format(new Date(Long.valueOf((endTimeQuery)))));
//                ps.setObject(6, Boolean.valueOf(isFailed));
//                ps.setObject(7, Boolean.valueOf(completed));
//                ps.setObject(8, pool);
//                ps.setObject(9, hiveQueryString);
//                ps.setObject(10, batchTime);
//
//                sqlsList.add(hiveQueryString);
//
//                //"攒"sql
//                ps.addBatch();
//                if (i % 50 == 0) {
//                    //2.执行batch
//                    ps.executeBatch();
//                    //3.清空batch
//                    ps.clearBatch();
//                }
//            }
//            ps.executeBatch();
//
//        } catch (SQLException sqlException) {
//            logger.error(sqlException.getMessage());
//            sqlException.printStackTrace();
//        } catch (Exception e) {
//            logger.error(e.getMessage());
//            e.printStackTrace();
//        } finally {
//            try {
//                connection.close();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//        return sqlsList;
//    }


}