package com.zz.atlas;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Description:
 *
 * @author zz
 * @date 2022/5/11
 */
public class RequestAtlasData {
    static Logger logger = LoggerFactory.getLogger(RequestAtlasData.class);

    //定时器
    private static ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);


    public static void main(String[] args) {

        SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        scheduledExecutor.scheduleWithFixedDelay(new TimerTask() {

            @Override
            public void run() {
                //毫秒
                long endTime = new Date().getTime();
                String batchTime = dataFormat.format(endTime);
                logger.info("execTime-->" + batchTime);
                new RequestAtlasData().exec();
            }
        }, 0, 600, TimeUnit.SECONDS);




    }

    public void exec() {
        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://192-168-80-52:21000"}, new String[]{"admin", "admin"});

        //neo4j
        Driver driver = GraphDatabase.driver("bolt://192-168-80-54/:7687", AuthTokens.basic("neo4j", "neo4j123"));
        Session session = driver.session();

        try {
            ArrayList<String> neoTableGuidList = createNeoTable(atlasClientV2, session);
//            createNeoProcess(atlasClientV2, session, "hive_process");
//            createNeoProcess(atlasClientV2, session, "hive_process_execution");
            for (String id : neoTableGuidList) {
                createLineage(atlasClientV2, session, id);
            }
        } finally {
            session.close();
            driver.close();
        }


    }


    public static ArrayList<String> createNeoTable(AtlasClientV2 atlasClientV2, Session session) {
        SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        MultivaluedMapImpl paramMap = new MultivaluedMapImpl();
        paramMap.add("typeName", "hive_table");//这里添加要搜索的类型
        paramMap.add("limit", "5000");
        paramMap.add("offset", "0");

        ArrayList<String> tableGuidList = new ArrayList<String>();

        JSONObject jsonObject = null;
        try {
            jsonObject = atlasClientV2.callAPI(AtlasClientV2.API_V2.BASIC_SEARCH, JSONObject.class, paramMap);

            JSONArray entities = jsonObject.getJSONArray("entities");
            for (int i = 0; i < entities.size(); i++) {
                String status = entities.getJSONObject(i).getString("status");
                if (status.equals("ACTIVE")) {
                    String guid = entities.getJSONObject(i).getString("guid");
                    JSONObject attributes = entities.getJSONObject(i).getJSONObject("attributes");
                    String owner = attributes.getString("owner");
                    String name = attributes.getString("name");
                    String qualifiedName = attributes.getString("qualifiedName");
                    String db = qualifiedName.split("\\.")[0];
                    BigInteger createTime = attributes.getBigInteger("createTime");
                    String format = dataFormat.format(createTime);

                    session.run("merge ( " + name + ":" + db + "  {name: {name}, db: {db}, owner: {owner}})",
                            // session.run("merge ( "+name+":"+db+"  {name: {name}, db: {db}, owner: {owner},  guid: {guid}})",
                            //         parameters("name", name, "db", db, "owner", owner,  "guid", guid));
                            parameters("name", name, "db", db, "owner", owner));
                    tableGuidList.add(guid);
                }
            }

        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
        return tableGuidList;
    }


    public static void createNeoProcess(AtlasClientV2 atlasClientV2, Session session, String process) {
        SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        MultivaluedMapImpl paramMap = new MultivaluedMapImpl();
        paramMap.add("typeName", process);//这里添加要搜索的类型
        paramMap.add("limit", "5000");//这里添加要搜索的类型
        paramMap.add("offset", "0");//这里添加要搜索的类型

        JSONObject jsonObject = null;
        try {
            jsonObject = atlasClientV2.callAPI(AtlasClientV2.API_V2.BASIC_SEARCH, JSONObject.class, paramMap);

            JSONArray entities = jsonObject.getJSONArray("entities");
            for (int i = 0; i < entities.size(); i++) {
                String status = entities.getJSONObject(i).getString("status");
                if (status.equals("ACTIVE")) {
                    String guid = entities.getJSONObject(i).getString("guid");
                    JSONObject attributes = entities.getJSONObject(i).getJSONObject("attributes");
                    String name = attributes.getString("name");
                    String qualifiedName = attributes.getString("qualifiedName");
                    String db = qualifiedName.split("\\.")[0].replace("-", "").replace(">", "")
                            .replace("QUERY:", "");

                    session.run("merge ( b:process {name: {name}, db: {db},  guid: {guid}})",
                            parameters("guid", guid, "db", db, "name", name));
                }
            }
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }

    }


    public static void createLineage(AtlasClientV2 atlasClientV2, Session session, String guid) {

        JSONObject jsonObject = null;
        MultivaluedMapImpl paramMap = new MultivaluedMapImpl();
        paramMap.add("depth", 10);
        paramMap.add("direction", "BOTH");
        try {
            jsonObject = atlasClientV2.callAPI(AtlasClientV2.API_V2.LINEAGE_INFO, JSONObject.class, paramMap, guid);

            JSONObject guidEntityMap = jsonObject.getJSONObject("guidEntityMap");
            ArrayList<HashMap<String, String>> mapArrayList = new ArrayList<HashMap<String, String>>();

            JSONArray relations = jsonObject.getJSONArray("relations");
            if (relations.size() == 0) {
                return;
            }

            for (int i = 0; i < relations.size(); i++) {
                String fromEntityId = relations.getJSONObject(i).getString("fromEntityId");
                String toEntityId = relations.getJSONObject(i).getString("toEntityId");
                String fromTypeName = guidEntityMap.getJSONObject(fromEntityId).getString("typeName");
                String fromStatus = guidEntityMap.getJSONObject(fromEntityId).getString("status");
                String toTypeName = guidEntityMap.getJSONObject(toEntityId).getString("typeName");
                String toStatus = guidEntityMap.getJSONObject(toEntityId).getString("status");
                if (fromStatus.equals("ACTIVE")) {
                    if (fromTypeName.equals("hive_table")) {
                        HashMap<String, String> tablesLinkHashMap = new HashMap<String, String>();
                        String nameFrom = guidEntityMap.getJSONObject(fromEntityId).getJSONObject("attributes").getString("name");
                        String qualifiedName = guidEntityMap.getJSONObject(fromEntityId).getJSONObject("attributes").getString("qualifiedName");
                        String dbFrom = qualifiedName.split("\\.")[0];
                        if (!toTypeName.equals("hive_table")) {
                            for (int j = 0; j < relations.size(); j++) {
                                String entityIdFrom = relations.getJSONObject(j).getString("fromEntityId");
                                if (entityIdFrom.equals(toEntityId)) {
                                    String entityIdTo = relations.getJSONObject(j).getString("toEntityId");
                                    JSONObject entityTo = guidEntityMap.getJSONObject(entityIdTo);
                                    String status = entityTo.getString("status");
                                    String typeName = entityTo.getString("typeName");
                                    String nameTo = entityTo.getJSONObject("attributes").getString("name");
                                    String qualifiedNameTo = entityTo.getJSONObject("attributes").getString("qualifiedName");
                                    String dbTo = qualifiedNameTo.split("\\.")[0];
                                    if (status.equals("ACTIVE") && typeName.equals("hive_table")) {
                                        tablesLinkHashMap.put("from", fromEntityId);
                                        tablesLinkHashMap.put("to", entityIdTo);
                                        tablesLinkHashMap.put("dbFrom", dbFrom);
                                        tablesLinkHashMap.put("dbTo", dbTo);
                                        tablesLinkHashMap.put("nameFrom", nameFrom);
                                        tablesLinkHashMap.put("nameTo", nameTo);
                                        mapArrayList.add(tablesLinkHashMap);
                                    }
                                }
                            }
                        } else {
                            tablesLinkHashMap.put("from", fromEntityId);
                            tablesLinkHashMap.put("to", toEntityId);
                            mapArrayList.add(tablesLinkHashMap);
                        }

                    }
                }

            }

            for (Map<String, String> map : mapArrayList) {
                // String from = map.get("from");
                // String to = map.get("to");
                String dbFrom = map.get("dbFrom");
                String dbTo = map.get("dbTo");
                String nameFrom = map.get("nameFrom");
                String nameTo = map.get("nameTo");
                session.run("match ( " + nameFrom + ":" + dbFrom + " {name:\'" + nameFrom + "\'}), (" + nameTo + ":" + dbTo + " {name:\'" + nameTo + "\'}) MERGE (" + nameFrom + ")-[:output]->(" + nameTo + ")");
//                session.run("MERGE ("+nameFrom+":"+dbFrom+"{guid:\'" + from + "\'})-[:output]->("+nameTo+":"+dbTo+"{guid:\'" + to + "\'})");
            }


        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
    }
}
