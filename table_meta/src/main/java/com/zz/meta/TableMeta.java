package com.zz.meta;

import cn.hutool.core.util.StrUtil;
import com.alibaba.druid.DbType;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zz.conn.ConnectionProviderHikariCP;
import com.zz.constant.Constant;
import com.zz.okhttp.FastHttpClient;
import com.zz.util.MD5Util;
import com.zz.util.MapDiffUtil;
import com.zz.util.SqlUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Description:
 *
 * @author zz
 * @date 2022/6/14
 */
public class TableMeta {

    //    private static String propertiesPath = TableMeta.class.getClassLoader().getResource("table_meta.properties").getPath();
    private static String propertiesPath = "table_meta.properties";

    private ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);

    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private ConcurrentHashMap<String, ConnectionProviderHikariCP> poolMap = new ConcurrentHashMap<>();
    private Properties properties = null;

    private static String hiveDriver = null;
    private static String hiveUrl = null;
    private static String hiveUsername = null;
    private static String hivePassword = null;
    private static String mysqlDriver = null;
    private static String mysqlUrl = null;
    private static String mysqlUsername = null;
    private static String mysqlPassword = null;
    private static String insertMetaTableSql = null;
    private static String dsUrl = null;
    private static String dsUser = null;
    private static String dsPassword = null;
    private static String getTableMetaSql = null;
    private static String insertTableMetaMonitorSql = null;
    private static String execTime = null;

    private void init() {
        properties = new Properties();
        try {
            properties.load(new FileInputStream(propertiesPath));
        } catch (IOException e) {
            e.printStackTrace();
        }

        hiveDriver = properties.getProperty("hiveDriverName");
        hiveUrl = properties.getProperty("hiveUrl");
        hiveUsername = properties.getProperty("hiveUser");
        hivePassword = properties.getProperty("hivePassword");

        mysqlDriver = properties.getProperty("mysqlDriverName");
        mysqlUrl = properties.getProperty("mysqlUrl");
        mysqlUsername = properties.getProperty("mysqlUser");
        mysqlPassword = properties.getProperty("mysqlPassword");
        insertMetaTableSql = properties.getProperty("insertMetaTableSql");

        dsUrl = properties.getProperty("dsUrl");
        dsUser = properties.getProperty("dsUser");
        dsPassword = properties.getProperty("dsPassword");

        getTableMetaSql = properties.getProperty("getTableMetaSql");

        insertTableMetaMonitorSql = properties.getProperty("insertTableMetaMonitorSql");
        execTime = properties.getProperty("execTime");


    }


    public boolean insertIntoTableMeta(ConnectionProviderHikariCP mysqlConn, String key, String projectName, String db, String SourceTabName, String SourceTableComment, String SinkTabName, Map<String, Map<String, Object>> mysqlTableColumnsMap, Map<String, Map<String, Object>> hiveTableColumnsMap, String nowTime) throws SQLException {

        List<Map<String, Object>> mapsToInsert = MapDiffUtil.mergeMapToInsert(projectName, db, key, SourceTabName, SourceTableComment, SinkTabName, mysqlTableColumnsMap, hiveTableColumnsMap, nowTime);
        ArrayList<Object[]> insertSqlValuesList = new ArrayList<>();
        for (Map<String, Object> map : mapsToInsert) {
            ArrayList<Object> values = new ArrayList<>();
            values.add(map.get("source_key"));
            values.add(map.get("ds_project_name"));
            values.add(map.get("source_db"));
            values.add(map.get("source_tab_name"));
            values.add(map.get("source_tab_comment"));
            values.add(map.get("source_col_name"));
            values.add(map.get("source_col_type"));
            values.add(map.get("source_col_comment"));
            values.add(map.get("sink_tab_name"));
            values.add(map.get("sink_col_name"));
            values.add(map.get("sink_col_type"));
            values.add(map.get("sink_col_comment"));
            values.add(map.get("create_time"));

            insertSqlValuesList.add(values.toArray());
        }
        int[] ints = mysqlConn.executeBatch(insertMetaTableSql, insertSqlValuesList);
        return true;
    }

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


    public JSONArray getDolphinProject(String sessionId) throws Exception {
        String response = FastHttpClient.
                get().
                addHeader("cookie", sessionId).
                url(String.format("%s/dolphinscheduler/projects/list-paging?pageSize=1000&pageNo=1&searchVal=", dsUrl)).
                build().
                execute().string();

        //{"code":0,"msg":"success","data":{"totalList":[{"id":8,"userId":6,"userName":"xx","name":"report_data_sync","description":"日志数据同步","createTime":"2021-09-08T17:30:42.000+0800","updateTime":"2021-09-08T17:30:42.000+0800","perm":7,"defCount":7,"instRunningCount":0}],"total":6,"currentPage":1,"totalPage":1}}

        JSONArray jsonArray = JSON.parseObject(response).getJSONObject("data").getJSONArray("totalList");
        return jsonArray;
    }

    public JSONArray getProcess(String sessionId, String projectName, String today) throws Exception {
        String url = "%s/dolphinscheduler/projects/%s/instance/list-paging?searchVal=&pageSize=%s&pageNo=1&host=&stateType=&startDate=%s+00:00:00&endDate=%s+23:59:59&executorName=";

        String totalPageUrl = String.format(url, dsUrl, projectName, 1, today, today);
        String totalPageResponse = FastHttpClient.
                get().
                addHeader("cookie", sessionId).
                url(totalPageUrl).
                build().
                execute().string();

        //{"code":0,"msg":"success","data":{"totalList":[{"id":8,"userId":6,"userName":"xx","name":"report_data_sync","description":"日志数据同步","createTime":"2021-09-08T17:30:42.000+0800","updateTime":"2021-09-08T17:30:42.000+0800","perm":7,"defCount":7,"instRunningCount":0}],"total":6,"currentPage":1,"totalPage":1}}

        Integer totalPage = JSON.parseObject(totalPageResponse).getJSONObject("data").getInteger("totalPage");
        String allProcessUrl = String.format(url, dsUrl, projectName, totalPage, today, today);
        String allProcessResponse = FastHttpClient.
                get().
                addHeader("cookie", sessionId).
                url(allProcessUrl).
                build().
                execute().string();
        JSONArray jsonArray = JSON.parseObject(allProcessResponse).getJSONObject("data").getJSONArray("totalList");
        return jsonArray;
    }


    public JSONArray getDataXInfo(String sessionId, String projectName, String processId) throws Exception {
        String url = "%s/dolphinscheduler/projects/%s/instance/select-by-id?processInstanceId=%s";
        String processUrl = String.format(url, dsUrl, projectName, processId);

        String response = FastHttpClient.
                get().
                addHeader("cookie", sessionId).
                url(processUrl).
                build().
                execute().string();

        //{"code":0,"msg":"success","data":{"totalList":[{"id":8,"userId":6,"userName":"xx","name":"report_data_sync","description":"日志数据同步","createTime":"2021-09-08T17:30:42.000+0800","updateTime":"2021-09-08T17:30:42.000+0800","perm":7,"defCount":7,"instRunningCount":0}],"total":6,"currentPage":1,"totalPage":1}}

        JSONArray tasksArr = JSON.parseObject(response).getJSONObject("data").getJSONObject("processInstanceJson").getJSONArray("tasks");

        return tasksArr;
    }

    public String getDolphinSession(String username, String password) {
        String sessionId = null;
        String loginUrl = String.format("%s/dolphinscheduler/login", dsUrl);
        ObjectParam param = new ObjectParam();
        param.userName = username;
        param.userPassword = password;
        String resp = null;
        try {
            resp = FastHttpClient.post().
                    url(loginUrl).
                    addParams(param).
                    build().
                    execute().string();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        //{"code":0,"msg":"login success","data":{"sessionId":"268a8781-ff54-42d6-a340-b09b3c14a077"}}
        if (JSONObject.parseObject(resp).getInteger("code") == 0) {
            sessionId = "sessionId" + "=" + JSONObject.parseObject(resp).getJSONObject("data").get("sessionId").toString();
        }

        return sessionId;
    }


    public boolean exec(TableMeta tableMeta, ConnectionProviderHikariCP mysqlConn, ConnectionProviderHikariCP hiveConn, String today, String nowTime) throws Exception {


        String sessionId = tableMeta.getDolphinSession(dsUser, dsPassword);
        JSONArray dolphinProject = tableMeta.getDolphinProject(sessionId);
        for (Object project : dolphinProject) {
            String projectName = JSON.parseObject(project.toString()).getString("name");
            if (projectName.equals("HK_CP")) {
                continue;
            }
            JSONArray processArr = tableMeta.getProcess(sessionId, projectName, today);
            for (Object process : processArr) {
                String state = JSON.parseObject(process.toString()).getString("state");
                if (!state.equals("SUCCESS")) {
                    continue;
                }
                String id = JSON.parseObject(process.toString()).getString("id");

                JSONArray dataXInfoArr = tableMeta.getDataXInfo(sessionId, projectName, id);
                for (Object task : dataXInfoArr) {
                    JSONObject taskJson = JSON.parseObject(task.toString());
                    if (taskJson.getString("type").equalsIgnoreCase("datax")) {
                        JSONObject content = taskJson.getJSONObject("params").getJSONObject("json").getJSONObject("job").getJSONArray("content").getJSONObject(0);

                        JSONObject reader = content.getJSONObject("reader");
                        String name = reader.getString("name");
                        if (!name.equals("mysqlreader") && !name.equals("postgresqlreader") && !name.equals("sqlserverreader") && !name.equals("rdbmsreader")) {
                            continue;
                        }


                        String username = reader.getJSONObject("parameter").getString("username");
                        String password = reader.getJSONObject("parameter").getString("password");
                        String jdbcUrl = reader.getJSONObject("parameter").getJSONArray("connection").getJSONObject(0).getJSONArray("jdbcUrl").getString(0);
                        String querySql = reader.getJSONObject("parameter").getJSONArray("connection").getJSONObject(0).getJSONArray("querySql").getString(0);


                        String writerPath = content.getJSONObject("writer").getJSONObject("parameter").getString("path");
                        String[] writerPathTmp = writerPath.split("/");
                        String stgTableName = writerPathTmp[writerPathTmp.length - 1];


                        List<Map<String, Object>> hiveTableColumns = hiveConn.excuteQuery(String.format(Constant.SHOW_COLUMNS_HIVE, stgTableName));
                        Map<String, Map<String, Object>> hiveTableColumnsCov = new HashMap<>();
                        hiveTableColumns.sort(Comparator.comparing(o -> o.get("col_name").toString()));
                        hiveTableColumns.removeIf(o -> o.get("col_name").toString().equalsIgnoreCase("stg_update_time"));
                        hiveTableColumns.forEach(o -> {
                            hiveTableColumnsCov.put(o.get("col_name").toString(), o);
                        });

                        switch (name) {
                            case "mysqlreader":
                                String driverMysql = "com.mysql.cj.jdbc.Driver";
                                String dbMysql = "";
                                String tableNameMysql = "";
                                List<String> tableNameListMysql = SqlUtils.getTableNameList(querySql, DbType.mysql);
                                //datax mysql
                                ConnectionProviderHikariCP connMysql = tableMeta.getConnInPool(driverMysql, jdbcUrl, username, password);
                                for (String tableName : tableNameListMysql) {
                                    String[] tableAndDbArr = tableName.split("\\.");
                                    if (tableAndDbArr.length == 2) {
                                        tableNameMysql = tableAndDbArr[1];
                                        dbMysql = tableAndDbArr[0];
                                    } else {
                                        tableNameMysql = tableName;
                                        if (jdbcUrl.contains("?")) {
                                            dbMysql = jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1, jdbcUrl.indexOf("?"));
                                        } else {
                                            dbMysql = jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1);
                                        }
                                    }

                                    List<Map<String, Object>> mysqlTableColumns = connMysql.excuteQuery(String.format(Constant.SHOW_COLUMNS_MYSQL, tableName));
                                    String mysqlTableComment = StrUtil.trimToNull(connMysql.excuteQuery(String.format(Constant.TABLE_COMMENT_MYSQL, dbMysql, tableNameMysql)).get(0).get("TABLE_COMMENT").toString());
                                    mysqlTableColumns.sort(Comparator.comparing(o ->
                                            o.getOrDefault("COLUMN_NAME", o.get("Field")).toString()
                                    ));

                                    //hive table 与 mysql table对比
                                    Map<String, Map<String, Object>> mysqlTableColumnsCov = new HashMap<>();

                                    mysqlTableColumns.forEach(o -> {
                                                Map<String, Object> mysqlColumnMapTmp = new HashMap<>();
                                                mysqlColumnMapTmp.put("col_name", o.getOrDefault("COLUMN_NAME", o.get("Field")).toString());
                                                mysqlColumnMapTmp.put("data_type", o.getOrDefault("COLUMN_TYPE", o.get("Type")).toString());
                                                mysqlColumnMapTmp.put("comment", StrUtil.trimToNull(o.getOrDefault("COLUMN_COMMENT", o.get("Comment")).toString()));
                                                mysqlTableColumnsCov.put(o.getOrDefault("COLUMN_NAME", o.get("Field")).toString().toLowerCase(), mysqlColumnMapTmp);
                                            }

                                    );
                                    String key = MD5Util.getMD5Code(querySql + jdbcUrl + username + password);
                                    boolean b = tableMeta.insertIntoTableMeta(mysqlConn, key, projectName, dbMysql, tableNameMysql, mysqlTableComment, stgTableName, mysqlTableColumnsCov, hiveTableColumnsCov, nowTime);

                                }
                                break;
                            case "postgresqlreader":
                                String driverPostgre = "org.postgresql.Driver";
                                String dbPostgre = "";
                                List<String> tableNameListPostgre = SqlUtils.getTableNameList(querySql, DbType.postgresql);

                                ConnectionProviderHikariCP connPostgre = tableMeta.getConnInPool(driverPostgre, jdbcUrl, username, password);
                                for (String tableName : tableNameListPostgre) {
                                    String[] tableAndDbArr = tableName.split("\\.");
                                    if (tableAndDbArr.length == 2) {
                                        tableName = tableAndDbArr[1];
                                        dbPostgre = tableAndDbArr[0];
                                    } else {

                                        if (jdbcUrl.contains("?")) {
                                            dbPostgre = jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1, jdbcUrl.indexOf("?"));
                                        } else {
                                            dbPostgre = jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1);
                                        }
                                    }
                                    List<Map<String, Object>> postgreTableColumns = connPostgre.excuteQuery(String.format(Constant.SHOW_COLUMNS_POSTGRE, tableName));
                                    String postgreTableComment = StrUtil.trimToNull(String.valueOf(connPostgre.excuteQuery(String.format(Constant.TABLE_COMMENT_POSTGRE, tableName)).get(0).get("table_comment")));

                                    postgreTableColumns.sort(Comparator.comparing(o ->
                                            o.get("col_name").toString()
                                    ));

                                    Map<String, Map<String, Object>> postgreTableColumnsCov = new HashMap<>();

                                    postgreTableColumns.forEach(o -> {
                                                Map<String, Object> postgreColumnMapTmp = new HashMap<>();
                                                postgreColumnMapTmp.put("col_name", o.get("col_name").toString());
                                                postgreColumnMapTmp.put("data_type", o.get("data_type").toString());
                                                postgreColumnMapTmp.put("comment", StrUtil.trimToNull(o.get("col_comment").toString()));
                                                postgreTableColumnsCov.put(o.get("col_name").toString().toLowerCase(), postgreColumnMapTmp);
                                            }

                                    );
                                    String key = MD5Util.getMD5Code(querySql + jdbcUrl + username + password);
                                    boolean b = tableMeta.insertIntoTableMeta(mysqlConn, key, projectName, dbPostgre, tableName, postgreTableComment, stgTableName, postgreTableColumnsCov, hiveTableColumnsCov, nowTime);

                                }
                                break;


                            case "sqlserverreader":
                                String driverSqlserver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
                                String dbSqlserver = "";
                                List<String> tableNameListSqlserver = SqlUtils.getTableNameList(querySql, DbType.sqlserver);

                                ConnectionProviderHikariCP connSqlserver = tableMeta.getConnInPool(driverSqlserver, jdbcUrl, username, password);
                                for (String tableName : tableNameListSqlserver) {
                                    String[] tableAndDbArr = tableName.split("\\.");
                                    if (tableAndDbArr.length == 2) {
                                        tableName = tableAndDbArr[1];
                                        dbSqlserver = tableAndDbArr[0];
                                    } else {
                                        //jdbc:sqlserver://rm-wz99dc1501j1u8k30uo.sqlserver.rds.aliyuncs.com:1433;DatabaseName=kingdee_20201124;
                                        if (jdbcUrl.contains("DatabaseName=")) {
                                            dbSqlserver = jdbcUrl.split("DatabaseName=")[1].split(";")[0];
                                        }
                                    }
                                    List<Map<String, Object>> sqlserverTableColumns = connSqlserver.excuteQuery(String.format(Constant.SHOW_COLUMNS_SQL_SERVER, tableName));
                                    String sqlserverTableComment = StrUtil.trimToNull(connSqlserver.excuteQuery(String.format(Constant.TABLE_COMMENT_SQL_SERVER, tableName)).get(0).get("table_comment").toString());
                                    sqlserverTableColumns.sort(Comparator.comparing(o ->
                                            o.get("col_name").toString()
                                    ));

                                    Map<String, Map<String, Object>> sqlserverTableColumnsCov = new HashMap<>();

                                    sqlserverTableColumns.forEach(o -> {
                                                Map<String, Object> sqlserverColumnMapTmp = new HashMap<>();
                                                sqlserverColumnMapTmp.put("col_name", o.get("col_name").toString());
                                                sqlserverColumnMapTmp.put("data_type", o.get("data_type").toString());
                                                sqlserverColumnMapTmp.put("comment", StrUtil.trimToNull(o.get("col_comment").toString()));
                                                sqlserverTableColumnsCov.put(o.get("col_name").toString().toLowerCase(), sqlserverColumnMapTmp);
                                            }

                                    );
                                    String key = MD5Util.getMD5Code(querySql + jdbcUrl + username + password);
                                    boolean b = tableMeta.insertIntoTableMeta(mysqlConn, key, projectName, dbSqlserver, tableName, sqlserverTableComment, stgTableName, sqlserverTableColumnsCov, hiveTableColumnsCov, nowTime);

                                }
                                break;

                            case "rdbmsreader":
                                if (!jdbcUrl.startsWith("jdbc:clickhouse:")) {
                                    break;
                                }
                                String driverCK = "ru.yandex.clickhouse.ClickHouseDriver";
                                String dbCK = "";
                                String tableNameCK = "";
                                List<String> tableNameListCK = SqlUtils.getTableNameList(querySql, DbType.mysql);
                                ConnectionProviderHikariCP connCK = tableMeta.getConnInPool(driverCK, jdbcUrl, username, password);
                                for (String tableName : tableNameListCK) {
                                    String[] tableAndDbArr = tableName.split("\\.");
                                    if (tableAndDbArr.length == 2) {
                                        tableNameCK = tableAndDbArr[1];
                                        dbCK = tableAndDbArr[0];
                                    } else {
                                        tableNameCK = tableName;
                                        if (jdbcUrl.contains("?")) {
                                            dbCK = jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1, jdbcUrl.indexOf("?"));
                                        } else {
                                            dbCK = jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1);
                                        }
                                    }

                                    List<Map<String, Object>> ckTableColumns = connCK.excuteQuery(String.format(Constant.SHOW_COLUMNS_CK, dbCK, tableNameCK));
                                    String cKTableComment = StrUtil.trimToNull(connCK.excuteQuery(String.format(Constant.TABLE_COMMENT_CK, dbCK, tableNameCK)).get(0).get("table_comment").toString());
                                    ckTableColumns.sort(Comparator.comparing(o ->
                                            o.get("col_name").toString()
                                    ));


                                    Map<String, Map<String, Object>> ckTableColumnsCov = new HashMap<>();

                                    ckTableColumns.forEach(o -> {
                                                Map<String, Object> ckColumnMapTmp = new HashMap<>();
                                                ckColumnMapTmp.put("col_name", o.get("col_name").toString());
                                                ckColumnMapTmp.put("data_type", o.get("data_type").toString());
                                                ckColumnMapTmp.put("col_comment", StrUtil.trimToNull(o.get("col_comment").toString()));
                                                ckTableColumnsCov.put(o.get("col_name").toString().toLowerCase(), ckColumnMapTmp);
                                            }

                                    );
                                    String key = MD5Util.getMD5Code(querySql + jdbcUrl + username + password);
                                    boolean b = tableMeta.insertIntoTableMeta(mysqlConn, key, projectName, dbCK, tableNameCK, cKTableComment, stgTableName, ckTableColumnsCov, hiveTableColumnsCov, nowTime);

                                }
                                break;
                            default:
                                break;

                        }


                    }
                }
            }
        }

        return true;
    }

    private long getTimeMillis(String time) {
        try {
            DateFormat dateFormat = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
            DateFormat dayFormat = new SimpleDateFormat("yy-MM-dd");
            Date curDate = dateFormat.parse(dayFormat.format(new Date()) + " " + time);
            return curDate.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return 0;
    }

    public void stat(TableMeta tableMeta, ConnectionProviderHikariCP mysqlConn, String today, String nowTime) throws Exception {

        long yesterdayTime = simpleDateFormat.parse(today).getTime() - 24 * 60 * 60 * 1000;
        String yesterday = simpleDateFormat.format(yesterdayTime);

        List<Map<String, Object>> todayRes = mysqlConn.excuteQuery(getTableMetaSql, today);
        List<Map<String, Object>> yesterdayRes = mysqlConn.excuteQuery(getTableMetaSql, yesterday);
        //去重
        HashMap<String, HashMap<String, Object>> todayResCardinalityMap = MapDiffUtil.getBaseMap(todayRes);
        HashMap<String, HashMap<String, Object>> yesterdayResCardinalityMap = MapDiffUtil.getBaseMap(yesterdayRes);

        Map<String, List<Map.Entry<String, HashMap<String, Object>>>> todayGroupByTableMap = todayResCardinalityMap.entrySet().stream().collect(Collectors.groupingBy(c -> c.getKey().split("@@")[0]));
        Map<String, List<Map.Entry<String, HashMap<String, Object>>>> yesterdayGroupByTableMap = yesterdayResCardinalityMap.entrySet().stream().collect(Collectors.groupingBy(c -> c.getKey().split("@@")[0]));

        Iterator it = todayGroupByTableMap.keySet().iterator();

        while (it.hasNext()) {
            Object obj = it.next();
            String key = String.valueOf(obj);

            //一张表的所有字段
            List<Map.Entry<String, HashMap<String, Object>>> todayObj = todayGroupByTableMap.get(key);
            List<Map.Entry<String, HashMap<String, Object>>> yesterdayObj = yesterdayGroupByTableMap.get(key);

            Object dsProjectNameToday = todayObj.get(0).getValue().get("ds_project_name");
            Object sourceDbToday = todayObj.get(0).getValue().get("source_db");
            Object sourceTabNameToday = todayObj.get(0).getValue().get("source_tab_name");


            ArrayList<Object[]> insertSqlValues = new ArrayList<>();


            if (yesterdayObj == null) {

                ArrayList<Object> values = new ArrayList<>();
                values.add(dsProjectNameToday);
                values.add(sourceDbToday);
                values.add(sourceTabNameToday);
                values.add(JSONObject.toJSONString(todayObj));
                values.add(null);
                values.add("now_table");
                values.add(null);
                values.add(nowTime);
                insertSqlValues.add(values.toArray());
            } else {
                Map<String, HashMap<String, Object>> yesterdayColsMap = yesterdayObj.stream().collect(Collectors.toMap(s -> s.getKey(), s -> s.getValue()));

                boolean tableCommentHasCompared = false;
                //一张表对比统计
                for (Map.Entry<String, HashMap<String, Object>> todayMap : todayObj) {
                    //get key
                    String todayMapKey = todayMap.getKey();
                    // get todayColMap
                    HashMap<String, Object> todayColMap = todayMap.getValue();

                    //get yesterdayColsMap
                    HashMap<String, Object> yesterdayColMap = yesterdayColsMap.get(todayMapKey);

                    if (todayColMap.equals(yesterdayColMap)) {
                        continue;
                    }

                    //统计信息插入
                    HashMap<String, HashMap<String, Object>> insertMap = new HashMap<>();
                    ArrayList<Object> values = new ArrayList<>();
                    values.add(dsProjectNameToday);
                    values.add(sourceDbToday);
                    values.add(sourceTabNameToday);
                    values.add(JSONObject.toJSONString(todayObj));
                    values.add(JSONObject.toJSONString(yesterdayObj));


                    //get today data
                    Object sourceColNameToday = todayColMap.get("source_col_name");
                    Object sourceColTypeToday = todayColMap.get("source_col_type");
                    Object sourceColCommentToday = todayColMap.get("source_col_comment");
                    Object sourceTabCommentToday = todayColMap.get("source_tab_comment");

                    if (yesterdayColMap == null) {
                        HashMap<String, Object> insertMapTmp = new HashMap<>();
                        //这个字段今天有，昨天没有
                        values.add("new_row");
                        insertMapTmp.put("col_name", sourceColNameToday);
                        insertMapTmp.put("col_type", sourceColTypeToday);
                        insertMapTmp.put("col_comment", sourceColCommentToday);
                        insertMap.put("new_row", insertMapTmp);
                        values.add(JSONObject.toJSONString(insertMap));
                        values.add(nowTime);
                        insertSqlValues.add(values.toArray());
                    } else {
                        //这个字段今天有，昨天也有
                        //比较字段类型，注释，
                        Object sourceColTypeYesterday = yesterdayColMap.get("source_col_type");
                        Object sourceColCommentYesterday = yesterdayColMap.get("source_col_comment");
                        Object sourceTabCommentYesterday = yesterdayColMap.get("source_tab_comment");

                        //字段类型不同
                        if (!sourceColTypeToday.equals(sourceColTypeYesterday)) {
                            HashMap<String, Object> insertMapTmp = new HashMap<>();
                            values.add("type_update");
                            insertMapTmp.put("old", sourceColTypeYesterday);
                            insertMapTmp.put("new", sourceColTypeToday);
                            insertMap.put("type_update", insertMapTmp);
                            values.add(JSONObject.toJSONString(insertMap));
                            values.add(sourceColNameToday);
                            values.add(nowTime);
                            insertSqlValues.add(values.toArray());
                        }

                        //字段注释不同
                        if (!sourceColCommentToday.equals(sourceColCommentYesterday)) {
                            HashMap<String, Object> insertMapTmp = new HashMap<>();
                            values.add("comment_update");
                            insertMapTmp.put("old", sourceColCommentYesterday);
                            insertMapTmp.put("new", sourceColCommentToday);
                            insertMap.put("comment_update", insertMapTmp);
                            values.add(JSONObject.toJSONString(insertMap));
                            values.add(sourceColNameToday);
                            values.add(nowTime);
                            insertSqlValues.add(values.toArray());

                        }

                        //表注释不同
                        if (!tableCommentHasCompared && !sourceTabCommentToday.equals(sourceTabCommentYesterday)) {
                            HashMap<String, Object> insertMapTmp = new HashMap<>();
                            values.add("tab_comment_update");
                            insertMapTmp.put("old", sourceTabCommentYesterday);
                            insertMapTmp.put("new", sourceTabCommentToday);
                            insertMap.put("tab_comment_update", insertMapTmp);
                            values.add(JSONObject.toJSONString(insertMap));
                            values.add(sourceColNameToday);
                            values.add(nowTime);
                            insertSqlValues.add(values.toArray());
                            //只对比一次
                            tableCommentHasCompared = true;
                        }
                    }
                }
            }

            if (insertSqlValues.size() != 0) {
                int[] ints = mysqlConn.executeBatch(insertTableMetaMonitorSql, insertSqlValues);

            }

        }

        System.out.println("exec stat finished.");
    }


    public static void main(String[] args) {
        TableMeta tableMeta = new TableMeta();
        tableMeta.init();
        //本地mysql
        ConnectionProviderHikariCP mysqlConn = tableMeta.getConnInPool(mysqlDriver, mysqlUrl, mysqlUsername, mysqlPassword);

        ConnectionProviderHikariCP hiveConn = tableMeta.getConnInPool(hiveDriver, hiveUrl, hiveUsername, hivePassword);

        //监控变化
        long oneDay = 24 * 60 * 60 * 1000;
        long initDelay = tableMeta.getTimeMillis("10:05:00") - System.currentTimeMillis();
        initDelay = initDelay > 0 ? initDelay : oneDay + initDelay;
        tableMeta.scheduledExecutor.scheduleAtFixedRate(() -> {
            Date date = new Date();
            String nowTime = timeFormat.format(date);
            String today = simpleDateFormat.format(date);
            System.out.println("nowTime--->" + nowTime);

            try {
                boolean exec = tableMeta.exec(tableMeta, mysqlConn, hiveConn, today, nowTime);
                tableMeta.stat(tableMeta, mysqlConn, today, nowTime);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, initDelay, oneDay, TimeUnit.MILLISECONDS);
//        Date date = new Date();
//        String nowTime = timeFormat.format(date);
//        String today = simpleDateFormat.format(date);
//        try {
////            tableMeta.exec(tableMeta, mysqlConn, hiveConn, today, nowTime);
//            tableMeta.stat(tableMeta, mysqlConn, today, nowTime);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }


    private static class ObjectParam {
        public String userName;
        public String userPassword;
    }
}
