package com.xingcloud.nba.service;

import com.google.gson.Gson;
import com.google.gson.internal.Pair;
import com.google.gson.reflect.TypeToken;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.nba.model.CacheModel;
import com.xingcloud.nba.model.GroupModel;
import com.xingcloud.nba.utils.BAUtil;
import com.xingcloud.nba.utils.Constant;
import com.xingcloud.nba.utils.DateManager;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ScanHBaseUID {

    private BasicDataSource ds;
    private static byte[] family = Bytes.toBytes("val");
    private static byte[] qualifier = Bytes.toBytes("val");

    public ScanHBaseUID() {
        ds = new BasicDataSource();
        Collection<String> initSql = new ArrayList<String>(1);
        initSql.add("select 1;");
        ds.setConnectionInitSqls(initSql);
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUsername("xingyun");
        ds.setPassword("xa");
        ds.setUrl("jdbc:mysql://65.255.35.134");
    }

    public static void main(String[] args) throws Exception{
        ScanHBaseUID test = new ScanHBaseUID();
        List<String> proj = new ArrayList<String>();
        proj.add("delta-homes");
        Map<String, Map<String,CacheModel>> res = test.getHBaseUID("20141130", "pay.search2", proj);
        Map<String,CacheModel> nation_results = res.get("nation");
        Map<String,CacheModel> ev3_results = res.get("ev3");
        Map<String,CacheModel> ev4_results = res.get("ev4");
        Map<String,CacheModel> ev5_results = res.get("ev5");

        int na_sum_num = 0;
        int na_sum_time = 0;
        BigDecimal na_sum_value = new BigDecimal(0);
        for(Map.Entry<String,CacheModel> nr : nation_results.entrySet()) {
            CacheModel cm = nr.getValue();
            na_sum_num += cm.getUserNum();
            na_sum_time += cm.getUserTime();
            na_sum_value = na_sum_value.add(cm.getValue());
            System.out.print(nr.getKey() + "---");
            System.out.print(nr.getValue());
            System.out.println();
        }
        System.out.println("nation sum values--------------------" + na_sum_num + "#" + na_sum_time + "#" + na_sum_value);

        int ev3_sum_num = 0;
        int ev3_sum_time = 0;
        BigDecimal ev3_sum_value = new BigDecimal(0);
        for(Map.Entry<String,CacheModel> nr : ev3_results.entrySet()) {
            CacheModel cm = nr.getValue();
            ev3_sum_num += cm.getUserNum();
            ev3_sum_time += cm.getUserTime();
            ev3_sum_value = ev3_sum_value.add(cm.getValue());
            System.out.print(nr.getKey() + "---");
            System.out.print(nr.getValue());
            System.out.println();
        }
        System.out.println("ev3 sum values--------------------" + ev3_sum_num + "#" + ev3_sum_time + "#" + ev3_sum_value);

        int ev4_sum_num = 0;
        int ev4_sum_time = 0;
        BigDecimal ev4_sum_value = new BigDecimal(0);
        for(Map.Entry<String,CacheModel> nr : ev4_results.entrySet()) {
            CacheModel cm = nr.getValue();
            ev4_sum_num += cm.getUserNum();
            ev4_sum_time += cm.getUserTime();
            ev4_sum_value = ev4_sum_value.add(cm.getValue());
            System.out.print(nr.getKey() + "---");
            System.out.print(nr.getValue());
            System.out.println();
        }
        System.out.println("ev4 sum values--------------------" + ev4_sum_num + "#" + ev4_sum_time + "#" + ev4_sum_value);

        int ev5_sum_num = 0;
        int ev5_sum_time = 0;
        BigDecimal ev5_sum_value = new BigDecimal(0);
        for(Map.Entry<String,CacheModel> nr : ev5_results.entrySet()) {
            CacheModel cm = nr.getValue();
            ev5_sum_num += cm.getUserNum();
            ev5_sum_time += cm.getUserTime();
            ev5_sum_value = ev5_sum_value.add(cm.getValue());
            System.out.print(nr.getKey() + "---");
            System.out.print(nr.getValue());
            System.out.println();
        }
        System.out.println("ev5 sum values--------------------" + ev5_sum_num + "#" + ev5_sum_time + "#" + ev5_sum_value);

    }

    /**
     * day是昨天的日期yyyy-MM-dd,昨天的数据清0
     * 计算前天的数据
     */
    public Map<String, Map<String,Number[]>> getResult(String day, String event, List projects) throws Exception {
        Map<String, Map<String,Number[]>> kv = new HashMap<String, Map<String,Number[]>>();

        String yesterdayKey = day + " 00:00";
        String scanDay = DateManager.getDaysBefore(day, 1);
        String valueKey = scanDay + " 00:00";
        String date = scanDay.replace("-","");
        Map<String, Map<String,CacheModel>> res = getHBaseUID(date, event, projects);

        Map<String,CacheModel> nation_results = res.get("nation");
        Map<String,CacheModel> ev3_results = res.get("ev3");
        Map<String,CacheModel> ev4_results = res.get("ev4");
        Map<String,CacheModel> ev5_results = res.get("ev5");
        Map<String, Number[]> nation_groupResult  = new HashMap<String, Number[]>();
        Map<String, Number[]> ev3_groupResult  = new HashMap<String, Number[]>();
        Map<String, Number[]> ev4_groupResult  = new HashMap<String, Number[]>();
        Map<String, Number[]> ev5_groupResult  = new HashMap<String, Number[]>();

        int na_sum_num = 0;
        int na_sum_time = 0;
        BigDecimal na_sum_value = new BigDecimal(0);
        for(Map.Entry<String,CacheModel> nr : nation_results.entrySet()) {
            CacheModel cm = nr.getValue();
            na_sum_num += cm.getUserNum();
            na_sum_time += cm.getUserTime();
            na_sum_value = na_sum_value.add(cm.getValue());
            nation_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String nationKey = "GROUP,internet-1," + scanDay + "," + scanDay + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,USER_PROPERTIES,nation";
        kv.put(nationKey, nation_groupResult);

        for(Map.Entry<String,CacheModel> nr : ev3_results.entrySet()) {
            CacheModel cm = nr.getValue();
            ev3_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String ev3Key = "GROUP,internet-1," + scanDay + "," + scanDay + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,2";
        kv.put(ev3Key, ev3_groupResult);

        for(Map.Entry<String,CacheModel> nr : ev4_results.entrySet()) {
            CacheModel cm = nr.getValue();
            ev4_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String ev4Key = "GROUP,internet-1," + scanDay + "," + scanDay + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,3";
        kv.put(ev4Key, ev4_groupResult);

        for(Map.Entry<String,CacheModel> nr : ev5_results.entrySet()) {
            CacheModel cm = nr.getValue();
            ev5_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String ev5Key = "GROUP,internet-1," + scanDay + "," + scanDay + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,4";
        kv.put(ev5Key, ev5_groupResult);

        String commonKey = "COMMON,internet-1," + scanDay + "," + scanDay + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
        Map<String, Number[]> result  = generateCacheValue(valueKey, na_sum_time, na_sum_value, na_sum_num);
        kv.put(commonKey, result);

        //清掉昨天的缓存
        Date d = new Date();
        String d1 = DateManager.dayfmt.format(d);
        String d2 = DateManager.getDaysBefore(d1, 1);
        if(d2.equals(day)) {
            String yes_nationKey = "GROUP,internet-1," + day + "," + day + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,USER_PROPERTIES,nation";
            nation_groupResult = generateCacheValue("null", 0, new BigDecimal(0), 0);
            kv.put(yes_nationKey, nation_groupResult);

            String yes_ev3Key = "GROUP,internet-1," + day + "," + day + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,2";
            ev3_groupResult = generateCacheValue("null", 0, new BigDecimal(0), 0);
            kv.put(yes_ev3Key, ev3_groupResult);

            String yes_ev4Key = "GROUP,internet-1," + day + "," + day + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,3";
            ev3_groupResult = generateCacheValue("null", 0, new BigDecimal(0), 0);
            kv.put(yes_ev4Key, ev4_groupResult);

            String yes_ev5Key = "GROUP,internet-1," + day + "," + day + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,4";
            ev3_groupResult = generateCacheValue("null", 0, new BigDecimal(0), 0);
            kv.put(yes_ev5Key, ev5_groupResult);

            String yes_commonKey = "COMMON,internet-1," + day + "," + day + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
            result = generateCacheValue(yesterdayKey, 0, new BigDecimal(0), 0);
            kv.put(yes_commonKey, result);
        }

        return kv;
    }

    public Map<String,Number[]> generateCacheValue(String key, int count, BigDecimal value, int num){
        Map<String, Number[]> result  = new HashMap<String, Number[]>();
        result.put(key, new Number[]{count, value, num, 1.0});
        return result;
    }

    public Map<String, Map<String,CacheModel>> getHBaseUID(String day, String event, List projects) throws Exception{
        ExecutorService service = Executors.newFixedThreadPool(16);
        List<Future<Map<String, Map<String,CacheModel>>>> tasks = new ArrayList<Future<Map<String, Map<String,CacheModel>>>>();

        for(int i=0;i<16;i++){
            tasks.add(service.submit(new ScanUID("node" + i, day, event, projects)));
        }

        Map<String, Map<String,CacheModel>> allResult = new HashMap<String, Map<String,CacheModel>>();
        Map<String,CacheModel> all_nation_results = new HashMap<String, CacheModel>();
        Map<String,CacheModel> all_ev3_results = new HashMap<String, CacheModel>();
        Map<String,CacheModel> all_ev4_results = new HashMap<String, CacheModel>();
        Map<String,CacheModel> all_ev5_results = new HashMap<String, CacheModel>();

        for(Future<Map<String, Map<String,CacheModel>>> uids : tasks){
            try{
                Map<String, Map<String,CacheModel>> nodeResult = uids.get();

                Map<String,CacheModel> nation_results = nodeResult.get("nation");
                Map<String,CacheModel> ev3_results = nodeResult.get("ev3");
                Map<String,CacheModel> ev4_results = nodeResult.get("ev4");
                Map<String,CacheModel> ev5_results = nodeResult.get("ev5");

                for(Map.Entry<String,CacheModel> nr : nation_results.entrySet()){
                    CacheModel cm = all_nation_results.get(nr.getKey());
                    if(cm == null){
                        all_nation_results.put(nr.getKey(), nr.getValue());
                    }else{
                        cm.incrDiffUser(nr.getValue());
                    }
                }

                for(Map.Entry<String,CacheModel> nr : ev3_results.entrySet()){
                    CacheModel cm = all_ev3_results.get(nr.getKey());
                    if(cm == null){
                        all_ev3_results.put(nr.getKey(), nr.getValue());
                    }else{
                        cm.incrDiffUser(nr.getValue());
                    }
                }

                for(Map.Entry<String,CacheModel> nr : ev4_results.entrySet()){
                    CacheModel cm = all_ev4_results.get(nr.getKey());
                    if(cm == null){
                        all_ev4_results.put(nr.getKey(), nr.getValue());
                    }else{
                        cm.incrDiffUser(nr.getValue());
                    }
                }

                for(Map.Entry<String,CacheModel> nr : ev5_results.entrySet()){
                    CacheModel cm = all_ev5_results.get(nr.getKey());
                    if(cm == null){
                        all_ev5_results.put(nr.getKey(), nr.getValue());
                    }else{
                        cm.incrDiffUser(nr.getValue());
                    }
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        service.shutdownNow();
        allResult.put("nation", all_nation_results);
        allResult.put("ev3", all_ev3_results);
        allResult.put("ev4", all_ev4_results);
        allResult.put("ev5", all_ev5_results);

        return allResult;
    }


    public Map<Long, String> getProperties(String project, String property, Set<Long> uids, String node) throws SQLException {
        if (uids.size() == 0) {
            return new HashMap<Long, String>();
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        StringBuilder sql = new StringBuilder("select t.uid, t.val from `16_");
        sql.append(project);
        sql.append("`.").append(property).append(" as t where t.uid in (?");
        char comma = ',';
        for (int i = 1; i < uids.size(); i++) {
            sql.append(comma);
            sql.append("?");
        }
        sql.append(')');
        Map<Long, String> idmap = new HashMap<Long, String>(uids.size());
        try {
            conn = MySql_16seqid.getInstance().getConnByNode(project, node);
            pstmt = conn.prepareStatement(sql.toString());
            int i = 1;
            for(long uid : uids){
                pstmt.setLong(i,uid);
                i++;
            }
            pstmt.setFetchSize(500000);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                try{
                    idmap.put(rs.getLong(1), rs.getString(2));
                }catch(Exception e){
                    e.printStackTrace();
                }
            }

        }catch(Exception e){
            e.printStackTrace();
        }finally{
            conn.close();
            pstmt.close();
            rs.close();
        }
        return idmap;
    }

    public Map<Long, String> executeSqlTrue(String projectId, Set<Long> uids) throws SQLException {
        if (uids.size() == 0) {
            return new HashMap<Long, String>();
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        StringBuilder sql = new StringBuilder("select t.id, lower(t.orig_id) from `vf_");
        sql.append(projectId);
        sql.append("`.id_map as t where t.id in (?");
        char comma = ',';
        for (int i = 1; i < uids.size(); i++) {
            sql.append(comma);
            sql.append("?");
        }
        sql.append(')');
        Map<Long, String> idmap = new HashMap<Long, String>(uids.size());
        try {
            conn = ds.getConnection();
            pstmt = conn.prepareStatement(sql.toString());
            int i = 1;
            for(long uid : uids){
                pstmt.setLong(i,uid);
                i++;
            }
            pstmt.setFetchSize(500000);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                try{
                    idmap.put(rs.getLong(1), rs.getString(2));
                }catch(Exception e){
                    e.printStackTrace();
                }
            }

        }catch(Exception e){
            e.printStackTrace();
        }finally{
            conn.close();
            pstmt.close();
            rs.close();
        }
//        System.out.println(uids.size() + "--------------------------------------" + idmap.size());
        return idmap;
    }



class ScanUID implements Callable<Map<String, Map<String,CacheModel>>>{

    String node;
    byte[] startKey;
    byte[] endKey;
    String day;
    List<String> projects;
    boolean maxVersion = false;

    public ScanUID(String node,String day,String event, List projects){
        this.day = day;
        this.node = node;
        this.startKey = Bytes.toBytes(day + event);
        this.endKey = Bytes.toBytes(BAUtil.asciiIncrease(day + event));
        this.projects = projects;
    }

    @Override
    public Map<String, Map<String,CacheModel>> call() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", node);
        conf.set("hbase.zookeeper.property.clientPort", "3181");

        Scan scan = new Scan();
        scan.setStartRow(startKey);
        scan.setStopRow(endKey);
        scan.setMaxVersions();
        scan.addColumn(family, qualifier);

        scan.setCaching(10000);
        Map<String, GroupModel> alluids = new HashMap<String, GroupModel>();
        for(String table : projects){
            scan(conf, scan, table, alluids);
        }

//        storeToFile(alluids, node, day, false);

        /*for(Map.Entry<String,Pair<String,CacheModel>> nr : alluids.entrySet()) {
            System.out.print(nr.getKey() + "---");
            System.out.print(nr.getValue().first + "---");
            System.out.print(nr.getValue().second);
            System.out.println();
        }*/

        Map<String, Map<String,CacheModel>> results = new HashMap<String, Map<String,CacheModel>>();
        Map<String,CacheModel> nation_results = new HashMap<String, CacheModel>();
        Map<String,CacheModel> ev3_results = new HashMap<String, CacheModel>();
        Map<String,CacheModel> ev4_results = new HashMap<String, CacheModel>();
        Map<String,CacheModel> ev5_results = new HashMap<String, CacheModel>();

        for(GroupModel groupModel : alluids.values()){
            Pair<String,CacheModel> nation = groupModel.getNation();
            if(nation != null) {
                CacheModel nation_cm = nation_results.get(nation.first);
                if(nation_cm == null){
                    nation_results.put(nation.first, nation.second);
                }else{
                    nation_cm.incrDiffUser(nation.second);
                }
            }


            mergeCM(ev3_results, groupModel.getEv3(), false);
            mergeCM(ev4_results, groupModel.getEv4(), false);
            mergeCM(ev5_results, groupModel.getEv5(), false);

/*            Map<String,CacheModel> ev3 = groupModel.getEv3();
            if(ev3 != null) {
                CacheModel ev3_cm = ev3_results.get(ev3.first);
                if(ev3_cm == null){
                    ev3_results.put(ev3.first, ev3.second);
                }else{
                    ev3_cm.incrDiffUser(ev3.second);
                }
            }

            Map<String,CacheModel> ev4 = groupModel.getEv4();
            if(ev4 != null) {
                CacheModel ev4_cm = ev4_results.get(ev4.first);
                if(ev4_cm == null){
                    ev4_results.put(ev4.first, ev4.second);
                }else{
                    ev4_cm.incrDiffUser(ev4.second);
                }
            }

            Map<String,CacheModel> ev5 = groupModel.getEv5();
            if(ev5 != null) {
                CacheModel ev5_cm = ev5_results.get(ev5.first);
                if(ev5_cm == null){
                    ev5_results.put(ev5.first, ev5.second);
                }else{
                    ev5_cm.incrDiffUser(ev5.second);
                }
            }*/
        }
        results.put("nation", nation_results);
        results.put("ev3", ev3_results);
        results.put("ev4", ev4_results);
        results.put("ev5", ev5_results);

        return results;
    }

    private void scan(Configuration conf, Scan scan, String tableName, Map<String, GroupModel> alluids) throws Exception{
        HTable table = new HTable(conf,"deu_" + tableName);
        ResultScanner scanner = table.getScanner(scan);

        Map<Long,CacheModel> cacheModelMap = new HashMap<Long,CacheModel>();
        Map<Long, GroupModel> groupModelMap = new HashMap<Long, GroupModel>();
        Map<Long,Long> localTruncMap = new HashMap<Long, Long>();
        try{
            String dayevent = "";
            String event = "";
            for(Result r : scanner){
                byte[] rowkey = r.getRow();
                long uid = BAUtil.transformerUID(Bytes.tail(rowkey, 5));
                long truncUid = BAUtil.truncate(uid);
                localTruncMap.put(truncUid,uid);

                dayevent = Bytes.toString(Bytes.head(rowkey,rowkey.length-6));
                event = dayevent.substring(8);
//            System.out.println("event-----------------------------------------" + event);
                String[] events = event.split("\\.");
                int len = events.length;
//            System.out.println("len--------------------------" + len);
                CacheModel cm = new CacheModel();
                cm.setUserNum(1);
                for(KeyValue kv : r.raw()){
                    cm.incrSameUser(Bytes.toBigDecimal(kv.getValue()));
                }

                String e3 = "XA-NA";
                String e4 = "XA-NA";
                String e5 = "XA-NA";
                if(3 == len) {
                    e3 = events[2];
                } else if(4 == len) {
                    e3 = events[2];
                    e4 = events[3];
                } else if(len >= 5) {
                    e3 = events[2];
                    e4 = events[3];
                    e5 = events[4];
                }

                if(cacheModelMap.get(truncUid) != null) {//这里有重复的truncUid,实际上是不同的uid
                    CacheModel cn = cacheModelMap.get(truncUid);
                    cn.incrSameUserInDifPro(cm);
                } else {
                    cacheModelMap.put(truncUid,cm);
                }

                GroupModel gm = new GroupModel();
                if(groupModelMap.get(truncUid) == null) {
                    gm.setEv3(new HashMap<String, CacheModel>());
                    gm.setEv4(new HashMap<String, CacheModel>());
                    gm.setEv5(new HashMap<String, CacheModel>());
                    groupModelMap.put(truncUid, gm);
                }

                GroupModel gn = groupModelMap.get(truncUid);
                addOrIncr(gn.getEv3(), cm, e3);
                addOrIncr(gn.getEv4(), cm, e4);
                addOrIncr(gn.getEv5(), cm, e5);

            }

        }finally {
            scanner.close();
            table.close();
        }

        //truncUID ==> orig_uid
        Map<Long,String> origUids = executeSqlTrue(tableName,localTruncMap.keySet());
        //localUID ==> nation
        Map<Long,String> nations = getProperties(tableName, "nation", new HashSet<Long>(localTruncMap.values()), node);
        //merge
        for(Map.Entry<Long,String> orig : origUids.entrySet()){
            long localid = localTruncMap.get(orig.getKey());
            GroupModel gm = alluids.get(orig.getValue());
            if(gm == null) {
                gm = groupModelMap.get(orig.getKey());
                Pair<String,CacheModel> nation = new Pair(nations.get(localid), cacheModelMap.get(orig.getKey()));
                gm.setNation(nation);
                alluids.put(orig.getValue(), gm);
            } else {
                Pair<String,CacheModel> nation = gm.getNation();
                if(nation == null){
                    if(nations.get(localid) == null) {
                        nation = new Pair("NA", cacheModelMap.get(orig.getKey()));
                    } else {
                        nation = new Pair(nations.get(localid), cacheModelMap.get(orig.getKey()));
                    }
                    gm.setNation(nation);
                }else{
                    nation.second.incrSameUserInDifPro(cacheModelMap.get(orig.getKey()));
                }

                mergeCM(gm.getEv3(), groupModelMap.get(orig.getKey()).getEv3(), true);
                mergeCM(gm.getEv4(), groupModelMap.get(orig.getKey()).getEv4(), true);
                mergeCM(gm.getEv5(), groupModelMap.get(orig.getKey()).getEv5(), true);

            }

        }

    }

    private void addOrIncr(Map<String,CacheModel> eventCM, CacheModel cm, String event){
        CacheModel cacheModel = eventCM.get(event);
        if(cacheModel == null){
            cacheModel = new CacheModel(cm);
            eventCM.put(event, cacheModel);
        }else{
            cacheModel.incrSameUserInDifPro(cm);
        }
    }



    /**
     * store data to local file
     */
    public void storeToFile(Map<String, GroupModel> result, String node, String day, boolean toHdfs) {
        String storeFilePath = BAUtil.getLocalGroupFileName(day, node);
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(new File(storeFilePath), true);
            String content = new Gson().toJson(result);
            out.write(content.getBytes("utf-8"));

            if(toHdfs){
                FileSystem fs = FileSystem.get(new Configuration());
                fs.copyFromLocalFile(new Path(storeFilePath), new Path(Constant.HDFS_CATCH_PATH));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
    private void mergeCM(Map<String,CacheModel> dest, Map<String,CacheModel> source, boolean sameuser){
        for(Map.Entry<String,CacheModel> cm : source.entrySet()){
            CacheModel destCM = dest.get(cm.getKey());
            if(destCM == null){
                dest.put(cm.getKey(), cm.getValue());
            }else{
                if(sameuser){
                    destCM.incrSameUserInDifPro(cm.getValue());
                }else{
                    destCM.incrDiffUser(cm.getValue());
                }
            }
        }
    }

    public Map<String, GroupModel> readFromFile(String day) throws Exception {
        Map<String, GroupModel> alluids = new HashMap<String, GroupModel>();
        Map<String, Map<String,CacheModel>> results = new HashMap<String, Map<String,CacheModel>>();
        Map<String,CacheModel> nation_results = new HashMap<String, CacheModel>();
        Map<String,CacheModel> ev3_results = new HashMap<String, CacheModel>();
        Map<String,CacheModel> ev4_results = new HashMap<String, CacheModel>();
        Map<String,CacheModel> ev5_results = new HashMap<String, CacheModel>();

        String storeFilePath = "";
        BufferedReader reader = null;
        for(int i = 0; i < 6; i++) {//取一周的数据，实际取的6天
            String date = DateManager.getDaysBefore(day, i);
            date = date.replace("-", "");
            for(int j = 0; j < 16; j++) {//data of one day
                storeFilePath = BAUtil.getLocalGroupFileName(date, "node"+j);
                reader = new BufferedReader(new FileReader(storeFilePath));
                String content = reader.readLine();
                Type groupType = new TypeToken<Map<String, GroupModel>>(){}.getType();
                Map<String, GroupModel> gmMap = new Gson().fromJson(content, groupType);
                for(Map.Entry<String, GroupModel> gmp : gmMap.entrySet()){
                    GroupModel gm = alluids.get(gmp.getKey());
                    if(gm == null) {
                        alluids.put(gmp.getKey(), gmp.getValue());
                    } else {
                        GroupModel gn = gmp.getValue();
                        gm.getNation().second.incrSameUserInDifPro(gn.getNation().second);
                        mergeCM(gm.getEv3(), gn.getEv3(), true);
                        mergeCM(gm.getEv4(), gn.getEv4(), true);
                        mergeCM(gm.getEv5(), gn.getEv5(), true);
                    }
                }
            }
        }

        return alluids;
    }

}

