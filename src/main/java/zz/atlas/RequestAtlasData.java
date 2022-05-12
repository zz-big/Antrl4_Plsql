package zz.atlas;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.impexp.AtlasServer;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.*;
import org.neo4j.driver.v1.*;

import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Description:
 *
 * @author zz
 * @date 2022/5/11
 */
public class RequestAtlasData {
    public static void main(String[] args) {


        AtlasClientV2 atlasClientV2 = new AtlasClientV2(new String[]{"http://192-168-80-52:21000"}, new String[]{"admin", "admin"});

        //测试
        Driver driver = GraphDatabase.driver("bolt://192-168-80-54/:7687", AuthTokens.basic("neo4j", "neo4j123"));
        Session session = driver.session();
        JSONObject jsonObject = null;

        MultivaluedMapImpl paramMap = new MultivaluedMapImpl();
        paramMap.add("typeName", "hive_table");//这里添加要搜索的类型
        paramMap.add("limit", "5000");//这里添加要搜索的类型
        paramMap.add("offset", "0");//这里添加要搜索的类型
        paramMap.add("query", "dws_data_map_shop_tags_dd*");//这里添加要搜索的类型
        try {
            jsonObject = atlasClientV2.callAPI(AtlasClientV2.API_V2.BASIC_SEARCH, JSONObject.class, paramMap);
            System.out.println("aaa");
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }

//        try {
//            ArrayList<String> neoTableGuidList = createNeoTable(atlasClientV2, session);
//            createNeoProcess(atlasClientV2, session, "hive_process");
//            createNeoProcess(atlasClientV2, session, "hive_process_execution");
//            for (String id : neoTableGuidList) {
//                createLineage(atlasClientV2, session, id);
//            }
//        } finally {
//            session.close();
//            driver.close();
//        }


    }


    public static ArrayList<String> createNeoTable(AtlasClientV2 atlasClientV2, Session session) {
        SimpleDateFormat dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        MultivaluedMapImpl paramMap = new MultivaluedMapImpl();
        paramMap.add("typeName", "hive_table");//这里添加要搜索的类型
        paramMap.add("limit", "5000");//这里添加要搜索的类型
        paramMap.add("offset", "0");//这里添加要搜索的类型

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

                    session.run("CREATE ( a:hive_table  {name: {name}, db: {db}, owner: {owner}, createTime: {createTime}, guid: {guid}})",
                            parameters("name", name, "db", db, "owner", owner, "createTime", format, "guid", guid));
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

                    session.run("CREATE ( b:process {name: {name}, db: {db},  guid: {guid}})",
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

            JSONArray relations = jsonObject.getJSONArray("relations");
            for (int i = 0; i < relations.size(); i++) {
                String fromEntityId = relations.getJSONObject(i).getString("fromEntityId");
                String toEntityId = relations.getJSONObject(i).getString("toEntityId");
                String relationshipId = relations.getJSONObject(i).getString("relationshipId");

                session.run("match (a {guid:\'" + fromEntityId + "\'}), (b {guid:\'" + toEntityId + "\'}) MERGE (a)-[:output]->(b)");
            }

        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
    }
}
