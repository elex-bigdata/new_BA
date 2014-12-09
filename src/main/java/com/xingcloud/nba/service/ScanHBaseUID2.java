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
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
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

public class ScanHBaseUID2 {

    private BasicDataSource ds;
    private static byte[] family = Bytes.toBytes("val");
    private static byte[] qualifier = Bytes.toBytes("val");

    public ScanHBaseUID2() {
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
        ScanHBaseUID2 test = new ScanHBaseUID2();
        List<String> pros = new ArrayList<String>();
        pros.add("delta-homes");
//        Map<String, List<String>> specialProjectList = getSpecialProjectList();
//        List<String> pros = specialProjectList.get(Constant.INTERNET1);
//        pros.add("newtab2");
        String day = args[0];
        test.getHBaseUID(day, "pay.search2", pros);



    }

    /**
     * day是昨天的日期yyyy-MM-dd,昨天的数据清0
     * 计算前天的数据
     */
    /*public Map<String, Map<String,Number[]>> getResult(String day, String event, List projects) throws Exception {
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
            ev4_groupResult = generateCacheValue("null", 0, new BigDecimal(0), 0);
            kv.put(yes_ev4Key, ev4_groupResult);

            String yes_ev5Key = "GROUP,internet-1," + day + "," + day + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,4";
            ev5_groupResult = generateCacheValue("null", 0, new BigDecimal(0), 0);
            kv.put(yes_ev5Key, ev5_groupResult);

            String yes_commonKey = "COMMON,internet-1," + day + "," + day + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
            result = generateCacheValue(yesterdayKey, 0, new BigDecimal(0), 0);
            kv.put(yes_commonKey, result);
        }

        return kv;
    }*/

    public Map<String, Map<String,Number[]>> getWeekResults (String day) throws Exception {
        Map<String, Map<String,Number[]>> kv = new HashMap<String, Map<String,Number[]>>();
        String start = DateManager.getDaysBefore(day, 6);
        String end = DateManager.dayfmt.format(DateManager.dayfmt.parse(day));

        Map<String, GroupModel> alluids = readFromFile2(day);
        Map<String, Number[]> nation_groupResult  = new HashMap<String, Number[]>();
        Map<String, Number[]> ev3_groupResult  = new HashMap<String, Number[]>();
        Map<String, Number[]> ev4_groupResult  = new HashMap<String, Number[]>();
        Map<String, Number[]> ev5_groupResult  = new HashMap<String, Number[]>();

        Map<String,CacheModel> nation_results = new HashMap<String,CacheModel>();
        Map<String,CacheModel> ev3_results = new HashMap<String,CacheModel>();
        Map<String,CacheModel> ev4_results = new HashMap<String,CacheModel>();
        Map<String,CacheModel> ev5_results = new HashMap<String,CacheModel>();

        for(GroupModel groupModel : alluids.values()){
            Pair<String,CacheModel> nation = groupModel.getNation();
            if(nation_results.get(nation.first) == null) {
                nation_results.put(nation.first, nation.second);
            } else {
                CacheModel cm = nation_results.get(nation.first);
                cm.incrDiffUser(nation.second);
            }

            Map<String,CacheModel> ev3 = groupModel.getEv3();
            mergeCM(ev3_results, groupModel.getEv3(), false);
            mergeCM(ev4_results, groupModel.getEv4(), false);
            mergeCM(ev5_results, groupModel.getEv5(), false);
        }

        for(Map.Entry<String,CacheModel> nr : nation_results.entrySet()) {
            CacheModel cm = nr.getValue();
            nation_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        //GROUP,internet-1,2014-12-01,2014-12-07,pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,2
        String nationKey = "GROUP,internet-1," + start + "," + end + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,USER_PROPERTIES,nation";
        kv.put(nationKey, nation_groupResult);

        for(Map.Entry<String,CacheModel> nr : ev3_results.entrySet()) {
            CacheModel cm = nr.getValue();
            ev3_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String ev3Key = "GROUP,internet-1," + start + "," + end + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,2";
        kv.put(ev3Key, ev3_groupResult);

        for(Map.Entry<String,CacheModel> nr : ev4_results.entrySet()) {
            CacheModel cm = nr.getValue();
            ev4_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String ev4Key = "GROUP,internet-1," + start + "," + end + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,3";
        kv.put(ev4Key, ev4_groupResult);

        for(Map.Entry<String,CacheModel> nr : ev5_results.entrySet()) {
            CacheModel cm = nr.getValue();
            ev5_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String ev5Key = "GROUP,internet-1," + start + "," + end + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,4";
        kv.put(ev5Key, ev5_groupResult);

        return kv;
    }

    public Map<String,Number[]> generateCacheValue(String key, int count, BigDecimal value, int num){
        Map<String, Number[]> result  = new HashMap<String, Number[]>();
        result.put(key, new Number[]{count, value, num, 1.0});
        return result;
    }

    public void getHBaseUID(String day, String event, List projects) throws Exception{
        ExecutorService service = Executors.newFixedThreadPool(16);
        List<Future<Map<String, Object>>> tasks = new ArrayList<Future<Map<String, Object>>>();

        for(int i=0;i<16;i++){
            tasks.add(service.submit(new ScanUID("node" + i, day, event, projects)));
        }

        for(Future<Map<String, Object>> task : tasks){
            task.get();
        }

        service.shutdownNow();

//        return allResult;
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



    class ScanUID implements Callable<Map<String, Object>>{

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
        public Map<String, Object> call() throws Exception {
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


            writeToLocalFile(alluids, day, node);


            return null;
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

    }

    /**
     * store data to local file
     */
    public void storeToFile(Map<String, GroupModel> result, String day) {
        String storeFilePath = BAUtil.getLocalGroupFileName(day);
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(new File(storeFilePath), true);
            String content = new Gson().toJson(result);
            out.write(content.getBytes("utf-8"));

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

    public void writeToLocalFile(Map<String, GroupModel> alluids, String day, String node) {
        BufferedWriter nation_bw = null;
        BufferedWriter ev3_bw = null;
        BufferedWriter ev4_bw = null;
        BufferedWriter ev5_bw = null;
        try {
            String nationFileName = BAUtil.getSearchFileName(day, Constant.NATION, node);
            String ev3FileName = BAUtil.getSearchFileName(day, Constant.EV3, node);
            String ev4FileName = BAUtil.getSearchFileName(day, Constant.EV4, node);
            String ev5FileName = BAUtil.getSearchFileName(day, Constant.EV5, node);
            File nationFile = new File(nationFileName);
            File ev3File = new File(ev3FileName);
            File ev4File = new File(ev4FileName);
            File ev5File = new File(ev5FileName);
            if(!nationFile.getParentFile().exists()) {
                if(!nationFile.getParentFile().mkdirs()) {
                    System.out.println("fail to create nationFile！");
                }
            }
            if(!ev3File.getParentFile().exists()) {
                if(!ev3File.getParentFile().mkdirs()) {
                    System.out.println("fail to create ev3File！");
                }
            }
            if(!ev4File.getParentFile().exists()) {
                if(!ev4File.getParentFile().mkdirs()) {
                    System.out.println("fail to create ev4File！");
                }
            }
            if(!ev5File.getParentFile().exists()) {
                if(!ev5File.getParentFile().mkdirs()) {
                    System.out.println("fail to create ev5File！");
                }
            }

            nation_bw = new BufferedWriter(new FileWriter(nationFile));
            ev3_bw = new BufferedWriter(new FileWriter(ev3File));
            ev4_bw = new BufferedWriter(new FileWriter(ev4File));
            ev5_bw = new BufferedWriter(new FileWriter(ev5File));

            Pair<String, CacheModel> nation = null;
            Map<String,CacheModel> ev3 = null;
            Map<String,CacheModel> ev4 = null;
            Map<String,CacheModel> ev5 = null;
            for(Map.Entry<String, GroupModel> gmp : alluids.entrySet()) {
                String origUid = gmp.getKey();
                GroupModel gm = gmp.getValue();
                nation = gm.getNation();
                StringBuffer sb = new StringBuffer();
                sb.append(origUid).append("\t").append(nation.first).append("\t").append(nation.second.getUserNum()).append("\t").append(nation.second.getUserTime()).append("\t").append(nation.second.getValue()).append("\n");
                nation_bw.write(sb.toString());

                sb.setLength(0);
                ev3 = gm.getEv3();
                for(Map.Entry<String, CacheModel> e3m : ev3.entrySet()) {
                    sb.append(origUid).append("\t").append(e3m.getKey()).append("\t").append(e3m.getValue().getUserNum()).append("\t").append(e3m.getValue().getUserTime()).append("\t").append(e3m.getValue().getValue()).append("\n");
                    ev3_bw.write(sb.toString());
                }

                sb.setLength(0);
                ev4 = gm.getEv4();
                for(Map.Entry<String, CacheModel> e4m : ev4.entrySet()) {
                    sb.append(origUid).append("\t").append(e4m.getKey()).append("\t").append(e4m.getValue().getUserNum()).append("\t").append(e4m.getValue().getUserTime()).append("\t").append(e4m.getValue().getValue()).append("\n");
                    ev4_bw.write(sb.toString());
                }

                sb.setLength(0);
                ev5 = gm.getEv5();
                for(Map.Entry<String, CacheModel> e5m : ev5.entrySet()) {
                    sb.append(origUid).append("\t").append(e5m.getKey()).append("\t").append(e5m.getValue().getUserNum()).append("\t").append(e5m.getValue().getUserTime()).append("\t").append(e5m.getValue().getValue()).append("\n");
                    ev5_bw.write(sb.toString());
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                nation_bw.close();
                ev3_bw.close();
                ev4_bw.close();
                ev5_bw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public void uploadToHdfs(String day) {
        try {
            String[] types = {Constant.NATION, Constant.EV3, Constant.EV4, Constant.EV5};
            Path src = null;
            Path dst = null;
            FileSystem fs = FileSystem.get(new Configuration());
            for(String type : types) {
                src =new Path("/data/log/ba/search/" + day + "/" + type + "/");
                dst = new Path(Constant.HDFS_SEARCH_PATH + day + "/" + type + "/");
                fs.copyFromLocalFile(src, dst);
            }

        } catch (IOException e) {
            e.printStackTrace();
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


    public Map<String, GroupModel> readFromFile2(String day) throws Exception {
        Map<String, GroupModel> alluids = new HashMap<String, GroupModel>();
        String storeFilePath = "";
        BufferedReader reader = null;
        for(int i = 1; i < 7; i++) {//取一周的数据，实际取的6天
            String date = DateManager.getDaysBefore(day, i);
            date = date.replace("-", "");

            storeFilePath = BAUtil.getLocalGroupFileName(date);
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

        return alluids;
    }

    public static Map<String, List<String>> getSpecialProjectList() throws Exception {
        Map<String, List<String>> projectList = new HashMap<String, List<String>>();
        File file = new File(Constant.SPECIAL_TASK_PATH);
        String json = "";
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = reader.readLine()) != null) {
                json += line;
            }
            JSONArray jsonArray = JSONArray.fromObject(json);
            for (Object object : jsonArray) {
                JSONObject jsonObj = (JSONObject) object;
                String project = jsonObj.getString("project");
                String[] projects = jsonObj.getString("members").split(",");
                List<String> memberList = new ArrayList<String>();
                for (String member : projects) {
                    String kv[] = member.split(":");
                    memberList.add(kv[0]);
                }
                projectList.put(project, memberList);
            }
        } catch (Exception e) {
            throw new Exception("parse the json("+Constant.SPECIAL_TASK_PATH+") " + json + " get exception "  + e.getMessage());
        }
        return projectList;
    }

}

