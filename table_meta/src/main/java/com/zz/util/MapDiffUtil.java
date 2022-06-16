package com.zz.util;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.beust.jcommander.internal.Lists;
import org.apache.twill.internal.json.JsonUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Description:
 *
 * @author zz
 * @date 2022/6/15
 */
public class MapDiffUtil {

    private static final Integer INTEGER_ONE = 1;


    // 判断并返回结果 true和false
    public static boolean isEqualCollection(Collection a, Collection b) {
//        if (a.size() != b.size()) { // size是最简单的相等条件
//            return false;
//        }
        Map mapa = getCardinalityMap(a);
//        System.out.println(mapa);
        Map mapb = getCardinalityMap(b);
//        System.out.println(mapb);
//        // 转换map后，能去掉重复的，这时候size就是⾮重复项，也是先决条件
//        if (mapa.size() != mapb.size()) {
//            System.out.println("存储的map数据不⼀致！");
//            return false;
//        } else {
//            System.out.println("转换map后，能去掉重复的，这时候size就是⾮重复项后，存储的map数据⼀致！");
//        }
        Iterator it = mapa.keySet().iterator();
        while (it.hasNext()) {
            Object obj = it.next();
            // 查询同⼀个obj，⾸先两边都要有，⽽且还要校验重复个数，就是map.value
            if (getFreq(obj, mapa) != getFreq(obj, mapb)) {
                return false;
            }
        }
        return true;
    }

    public static final int getFreq(Object obj, Map freqMap) {
        Integer count = (Integer) freqMap.get(obj);
        if (count != null) {
            return count.intValue();
        }
        return 0;
    }

    /**
     * 以obj为key，可以防⽌重复，如果重复就value++ 这样实际上记录了元素以及出现的次数
     */
    public static Map getCardinalityMap(Collection coll) {
        Map count = new HashMap();
        for (Iterator it = coll.iterator(); it.hasNext(); ) {
            Object obj = it.next();
            Integer c = (Integer) count.get(obj);
            if (c == null)
                count.put(obj, INTEGER_ONE);
            else {
                count.put(obj, new Integer(c.intValue() + 1));
            }
        }
        return count;
    }


    public static HashMap<String, HashMap<String, Object>> getBaseMap(List<Map<String, Object>> coll) {
        HashMap<String, HashMap<String, Object>> baseMap = new HashMap();
        for (Iterator it = coll.iterator(); it.hasNext(); ) {
            HashMap<String, Object> map = (HashMap<String, Object>) it.next();

            //拼接key
            String key = map.get("ds_project_name").toString() + map.get("source_db").toString() + map.get("source_tab_name").toString()+"@@"+map.get("source_col_name").toString();
            baseMap.put(key, map);
        }
        return baseMap;
    }

    /**
     * 比较基础数据list和目标list,返回目标list中没有的数据和不一致的数据
     *
     * @param basicList  基础数据list
     * @param targetList 目标list
     * @param index      主键
     * @return
     */
    public static Map<String, List<Map<String, Object>>> checkDiffList(List<Map<String, Object>> basicList, List<Map<String, Object>> targetList, String... index) {
        Map<String, Map<String, Object>> basicMap = checkListToMap(basicList, index);
        Map<String, Map<String, Object>> targetMap = checkListToMap(targetList, index);
        Map<String, List<Map<String, Object>>> result = new HashMap<>();
        List<Map<String, Object>> insertList = Lists.newArrayList();
        List<Map<String, Object>> updateList = Lists.newArrayList();
        basicMap.forEach((k, v) -> {
            Map<String, Object> tblData = targetMap.get(k);
            if (tblData == null) {
                insertList.add(v);
            } else {
                if (checkDiffMap(v, tblData)) {
                    updateList.add(v);
                }
            }
        });
        result.put("insertList", insertList);
        result.put("updateList", updateList);
        return result;
    }


    /**
     * 比较基础数据list和目标list,返回目标list中没有的数据和不一致的数据
     *
     * @param basicMap  基础数据list
     * @param targetMap 目标list
     * @return
     */

    public static List<Map<String, Object>> mergeMapToInsert(String projectName, String db, String key, String SourceTabName, String SourceTableComment, String SinkTabName, Map<String, Map<String, Object>> basicMap, Map<String, Map<String, Object>> targetMap, String time) {
        List<Map<String, Object>> mergeMapList = Lists.newArrayList();
        basicMap.forEach((k, v) -> {
                    Map<String, Object> tblData = targetMap.get(k);
                    Map<String, Object> mapTmp = new HashMap<>();
                    mapTmp.put("source_tab_name", SourceTabName);
                    mapTmp.put("source_tab_comment", SourceTableComment);
                    mapTmp.put("source_col_name", v.get("col_name"));
                    mapTmp.put("source_col_type", v.get("data_type"));
                    mapTmp.put("source_col_comment", v.get("comment"));
                    mapTmp.put("sink_tab_name", SinkTabName);
                    mapTmp.put("source_key", key);
                    mapTmp.put("source_db", db);
                    mapTmp.put("ds_project_name", projectName);
                    mapTmp.put("create_time", time);
                    mergeMapList.add(mapTmp);

                    if (tblData == null) {
                        mapTmp.put("sink_col_name", null);
                        mapTmp.put("sink_col_type", null);
                        mapTmp.put("sink_col_comment", null);
                    } else {
                        mapTmp.put("sink_col_name", tblData.get("col_name"));
                        mapTmp.put("sink_col_type", tblData.get("data_type"));
                        mapTmp.put("sink_col_comment", StrUtil.trimToNull(tblData.get("comment").toString()));
                    }

                }
        );
        return mergeMapList;
    }


    /**
     * 根据index数组返回比对数据Map
     *
     * @param list
     * @param index
     * @return
     */
    private static Map<String, Map<String, Object>> checkListToMap(List<Map<String, Object>> list, String... index) {
        index = new String[]{"0", "1", "2", "3", "4", "5", "6"};
        String[] finalIndex = index;
        Map<String, Map<String, Object>> collect = list.stream().collect(Collectors.toMap(e -> {
            StringBuilder key = new StringBuilder();
            for (String item : finalIndex) {
                key.append(Optional.ofNullable(e.get(item)).orElse("")).append("_");
            }
            return key.toString();
        }, e -> e, (k1, k2) -> k1));
        return collect;
    }


    /**
     * 比对两个map 如果数据不一致 则返回true
     *
     * @param source 基础数据list
     * @param target 目标list
     * @return 是否一致
     */
    public static Boolean checkDiffMap(Map<String, Object> source, Map<String, Object> target) {
        source = new TreeMap<>(source);
        target = new TreeMap<>(target);
        String sourceStr = JSON.toJSONString(source);
        String targetStr = JSON.toJSONString(target);
        return !(sourceStr.equalsIgnoreCase(targetStr));
    }

    /**
     * 比对两个map 如果数据不一致 则返回true
     *
     * @param source 基础数据list
     * @param target 目标list
     * @return 是否一致
     */
    public static Boolean checkDiffMapString(Map<String, Object> source, Map<String, Object> target) {
        source = new TreeMap<>(source);
        target = new TreeMap<>(target);
        String sourceStr = JSON.toJSONString(source);
        String targetStr = JSON.toJSONString(target);
        return !(sourceStr.equalsIgnoreCase(targetStr));
    }
}
