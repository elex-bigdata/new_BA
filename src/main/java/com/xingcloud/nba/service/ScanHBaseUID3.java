package com.xingcloud.nba.service;

import com.google.gson.internal.Pair;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.nba.hive.HiveJdbcClient;
import com.xingcloud.nba.model.CacheModel;
import com.xingcloud.nba.model.GroupModel;
import com.xingcloud.nba.model.SearchModel;
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
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ScanHBaseUID3 {

    private BasicDataSource ds;
    private static byte[] family = Bytes.toBytes("val");
    private static byte[] qualifier = Bytes.toBytes("val");
    private Connection conn = null;
    private String[] types = {Constant.NATION, Constant.EV3, Constant.EV4, Constant.EV5};

    public ScanHBaseUID3() {
        ds = new BasicDataSource();
        Collection<String> initSql = new ArrayList<String>(1);
        initSql.add("select 1;");
        ds.setConnectionInitSqls(initSql);
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUsername("xingyun");
        ds.setPassword("xa");
        ds.setUrl("jdbc:mysql://65.255.35.134");

        try {
            conn = HiveJdbcClient.getInstance().getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception{
        ScanHBaseUID3 test = new ScanHBaseUID3();
//        List<String> pros = new ArrayList<String>();
//        pros.add("delta-homes");
        Map<String, List<String>> specialProjectList = getSpecialProjectList();
        List<String> pros = specialProjectList.get(Constant.INTERNET1);
        pros.add("newtab2");
        String cmd = args[0];
        String day = args[1];
        if(cmd.equals("calc")) {
            test.getHBaseUID(day, "pay.search2", pros);
        } else if(cmd.equals("upload")) {
            test.uploadToHdfs(day);
        }
    }

    public Map<String, Map<String,Number[]>> getResults(String day, String event, List projects) throws Exception {

        Map<String, Map<String,Number[]>> kv = new HashMap<String, Map<String,Number[]>>();
        String yesterdayKey = day + " 00:00";
        String scanDay = DateManager.getDaysBefore(day, 1);
        String valueKey = scanDay + " 00:00";
        String date = scanDay.replace("-","");

        getHBaseUID(date, event, projects);
        uploadToHdfs(date);
        alterTable(date);
System.out.println("----------------------------start to get results---------------------------");
        String start = DateManager.getDaysBefore(day, 6);
        String end = DateManager.dayfmt.format(DateManager.dayfmt.parse(day));

        //--------------------------------------------single day------------------------------------------------------------

        CacheModel comSearch = calcCommon(date);
        String commonKey = "COMMON,internet-1," + scanDay + "," + scanDay + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
        Map<String, Number[]> result  = generateCacheValue(valueKey, comSearch.getUserTime(), comSearch.getValue(), comSearch.getUserNum());
        kv.put(commonKey, result);

        String oneGrpCommKey = "";
        Map<String, Number[]> oneGrpCommResult  = null;

        Map<String,CacheModel> nation_results = calcPropGroup(date, Constant.NATION, false);
        Map<String, Number[]> nation_groupResult  = new HashMap<String, Number[]>();
        for(Map.Entry<String,CacheModel> nr : nation_results.entrySet()) {
            CacheModel cm = nr.getValue();
            //COMMON,internet-1,2014-12-02,2014-12-02,pay.search2.*,{"nation":"np"},VF-ALL-0-0,PERIOD
            oneGrpCommKey = "COMMON,internet-1," + scanDay + "," + scanDay + ",pay.search2.*,{\"nation\":\"" + nr.getKey() + "\"},VF-ALL-0-0,PERIOD";
            oneGrpCommResult = generateCacheValue(valueKey, cm.getUserTime(), cm.getValue(), cm.getUserNum());
            kv.put(oneGrpCommKey, oneGrpCommResult);
            nation_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String nationKey = "GROUP,internet-1," + scanDay + "," + scanDay + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,USER_PROPERTIES,nation";
        kv.put(nationKey, nation_groupResult);

        Map<String,CacheModel> ev3_results = calcPropGroup(date, Constant.EV3, false);
        Map<String, Number[]> ev3_groupResult  = new HashMap<String, Number[]>();
        for(Map.Entry<String,CacheModel> nr : ev3_results.entrySet()) {
            CacheModel cm = nr.getValue();
            //COMMON,internet-1,2014-12-02,2014-12-02,pay.search2.ds.*,TOTAL_USER,VF-ALL-0-0,PERIOD
            oneGrpCommKey = "COMMON,internet-1," + scanDay + "," + scanDay + ",pay.search2." + nr.getKey() + ".*,TOTAL_USER,VF-ALL-0-0,PERIOD";
            oneGrpCommResult = generateCacheValue(valueKey, cm.getUserTime(), cm.getValue(), cm.getUserNum());
            kv.put(oneGrpCommKey, oneGrpCommResult);
            ev3_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String ev3Key = "GROUP,internet-1," + scanDay + "," + scanDay + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,2";
        kv.put(ev3Key, ev3_groupResult);

        Map<String,CacheModel> ev4_results = calcPropGroup(date, Constant.EV4, false);
        Map<String, Number[]> ev4_groupResult  = new HashMap<String, Number[]>();
        for(Map.Entry<String,CacheModel> nr : ev4_results.entrySet()) {
            CacheModel cm = nr.getValue();
            //COMMON,internet-1,2014-12-02,2014-12-02,pay.search2.*.wpm11123.*,TOTAL_USER,VF-ALL-0-0,PERIOD
            oneGrpCommKey = "COMMON,internet-1," + scanDay + "," + scanDay + ",pay.search2.*." + nr.getKey() + ".*,TOTAL_USER,VF-ALL-0-0,PERIOD";
            oneGrpCommResult = generateCacheValue(valueKey, cm.getUserTime(), cm.getValue(), cm.getUserNum());
            kv.put(oneGrpCommKey, oneGrpCommResult);
            ev4_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String ev4Key = "GROUP,internet-1," + scanDay + "," + scanDay + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,3";
        kv.put(ev4Key, ev4_groupResult);

        Map<String,CacheModel> ev5_results = calcPropGroup(date, Constant.EV5, false);
        Map<String, Number[]> ev5_groupResult  = new HashMap<String, Number[]>();
        for(Map.Entry<String,CacheModel> nr : ev5_results.entrySet()) {
            CacheModel cm = nr.getValue();
            //COMMON,internet-1,2014-12-02,2014-12-02,pay.search2.*.*.google.*,TOTAL_USER,VF-ALL-0-0,PERIOD
            oneGrpCommKey = "COMMON,internet-1," + scanDay + "," + scanDay + ",pay.search2.*.*." + nr.getKey() + ".*,TOTAL_USER,VF-ALL-0-0,PERIOD";
            oneGrpCommResult = generateCacheValue(valueKey, cm.getUserTime(), cm.getValue(), cm.getUserNum());
            kv.put(oneGrpCommKey, oneGrpCommResult);
            ev5_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String ev5Key = "GROUP,internet-1," + scanDay + "," + scanDay + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,4";
        kv.put(ev5Key, ev5_groupResult);

        //------------------------------------------week-----------------------------------------------------

        //GROUP,internet-1,2014-12-03,2014-12-09,pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,2
        Map<String,CacheModel> nation_week_results = calcPropGroup(date, Constant.NATION, true);
        Map<String, Number[]> nation_week_groupResult  = new HashMap<String, Number[]>();
        for(Map.Entry<String,CacheModel> nr : nation_week_results.entrySet()) {
            CacheModel cm = nr.getValue();
            nation_week_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String nation_week_Key = "GROUP,internet-1," + start + "," + end + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,USER_PROPERTIES,nation";
        kv.put(nation_week_Key, nation_week_groupResult);

        Map<String,CacheModel> ev3_week_results = calcPropGroup(date, Constant.EV3, true);
        Map<String, Number[]> ev3_week_groupResult  = new HashMap<String, Number[]>();
        for(Map.Entry<String,CacheModel> nr : ev3_week_results.entrySet()) {
            CacheModel cm = nr.getValue();
            ev3_week_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String ev3_week_Key = "GROUP,internet-1," + start + "," + end + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,2";
        kv.put(ev3_week_Key, ev3_week_groupResult);

        Map<String,CacheModel> ev4_week_results = calcPropGroup(date, Constant.EV4, true);
        Map<String, Number[]> ev4_week_groupResult  = new HashMap<String, Number[]>();
        for(Map.Entry<String,CacheModel> nr : ev4_week_results.entrySet()) {
            CacheModel cm = nr.getValue();
            ev4_week_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String ev4_week_Key = "GROUP,internet-1," + start + "," + end + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,3";
        kv.put(ev4_week_Key, ev4_week_groupResult);

        Map<String,CacheModel> ev5_week_results = calcPropGroup(date, Constant.EV5, true);
        Map<String, Number[]> ev5_week_groupResult  = new HashMap<String, Number[]>();
        for(Map.Entry<String,CacheModel> nr : ev5_week_results.entrySet()) {
            CacheModel cm = nr.getValue();
            ev5_week_groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String ev5_week_Key = "GROUP,internet-1," + start + "," + end + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,EVENT,4";
        kv.put(ev5_week_Key, ev5_week_groupResult);

        //------------------------------------------清掉昨天的缓存---------------------------------------------------

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
    }

    public CacheModel calcCommon(String day) throws Exception {
        List<CacheModel> result = new ArrayList<CacheModel>();
        //select count(distinct uid),sum(count),sum(value) from user_search where day='20141203' and type='nation'
        String sql = "select count(distinct uid),sum(count),sum(value) from user_search where day = '" + day + "' and type = 'nation'";
        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        CacheModel cm = null;
        while (res.next()) {
            cm = new CacheModel();
            cm.setUserNum(res.getInt(1));
            cm.setUserTime(res.getInt(2));
            cm.setValue(BigDecimal.valueOf(res.getLong(3)));
            result.add(cm);
        }
        return result.get(0);
    }

    public Map<String, CacheModel> calcPropGroup(String day, String prop, boolean isWeek) throws Exception {
        Map<String, CacheModel> result = new HashMap<String, CacheModel>();
        String sql = "";
        if(!isWeek) {
            //select prop, count(distinct uid),sum(count),sum(value) from user_search where day='20141203' and type='nation' group by prop;
            sql = "select prop, count(distinct uid),sum(count),sum(value) from user_search where day = '" + day + "' and type = '" + prop + "' group by prop";
        } else {
            String end = day;
            day = day.substring(0,4) + "-" + day.substring(4,6) + "-" + day.substring(6);
            String start = DateManager.getDaysBefore(day, 5);
            start = start.replace("-", "");
            sql = "select prop, count(distinct uid),sum(count),sum(value) from user_search where day >= '" + start + "' and day <= '" + end + "' and type = '" + prop + "' group by prop";
        }

        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        CacheModel cm = null;
        while (res.next()) {
            cm = new CacheModel();
            cm.setUserNum(res.getInt(2));
            cm.setUserTime(res.getInt(3));
            cm.setValue(BigDecimal.valueOf(res.getLong(4)));
            result.put(res.getString(1), cm);
        }
        return result;
    }

    public Map<String,Number[]> generateCacheValue(String key, int count, BigDecimal value, int num){
        Map<String, Number[]> result  = new HashMap<String, Number[]>();
        result.put(key, new Number[]{count, value, num, 1.0});
        return result;
    }

    public void getHBaseUID(String day, String event, List projects) throws Exception{
        ExecutorService service = Executors.newFixedThreadPool(16);
//        List<Future<Map<String, Object>>> tasks = new ArrayList<Future<Map<String, Object>>>();

        for(int i=0;i<16;i++){
            service.submit(new ScanUID("node" + i, day, event, projects));
        }



        service.shutdownNow();
        System.out.println("-------------------------write file over------------------ ");
    }

    public void alterTable(String day) throws SQLException {
        //alter table user_search add partition(day='20141207', type='nation') location '/hadoop/user/search/20141207/nation'
        Statement stmt = conn.createStatement();
        for(String type : types) {
            String sql = "alter table user_search add partition(day='" + day + "') location '/hadoop/user/search/" + day + "'";
            try{
                stmt.execute(sql);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
System.out.println("------------------------------add partition over-------------------------");
        stmt.close();
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
        return idmap;
    }



    class ScanUID implements Runnable{

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
        public void run() {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", node);
            conf.set("hbase.zookeeper.property.clientPort", "3181");
System.out.println("--------------------start call-------------------");
            Scan scan = new Scan();
            scan.setStartRow(startKey);
            scan.setStopRow(endKey);
            scan.setMaxVersions();
            scan.addColumn(family, qualifier);
            scan.setCaching(10000);

            for(String table : projects){
                try {
                    scan(conf, scan, table);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }

        private void scan(Configuration conf, Scan scan, String tableName) throws Exception{
            Map<String, SearchModel> alluids = new HashMap<String, SearchModel>();
            HTable table = new HTable(conf,"deu_" + tableName);
            ResultScanner scanner = table.getScanner(scan);

            Map<Long,SearchModel> searchModelMap = new HashMap<Long,SearchModel>();
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
                    String[] events = event.split("\\.");
                    int len = events.length;
                    SearchModel sm = new SearchModel();

                    CacheModel cm = new CacheModel();
                    cm.setUserNum(1);
                    for(KeyValue kv : r.raw()){
                        cm.incrSameUser(Bytes.toBigDecimal(kv.getValue()));
                    }

                    String e3 = "XA-NA";
                    String e4 = "XA-NA";
                    String e5 = "XA-NA";
                    String nation = "XA-NA";
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

                    sm.setEvt3(e3);
                    sm.setEvt4(e4);
                    sm.setEvt5(e5);
                    sm.setNation(nation);

                    if(searchModelMap.get(truncUid).getCacheModel() != null) {//这里有重复的truncUid,实际上是不同的uid
                        CacheModel cn = searchModelMap.get(truncUid).getCacheModel();
                        cn.incrSameUserInDifPro(cm);
                    } else {
                        sm.setCacheModel(cm);
                        searchModelMap.put(truncUid,sm);
                    }

                }

            }finally {
                scanner.close();
                table.close();
            }

            //truncUID ==> orig_uid
            Map<Long,String> origUids = executeSqlTrue(tableName,localTruncMap.keySet());
            //localUID ==> nation
            Map<Long,String> nations = getProperties(tableName, "nation", new HashSet<Long>(localTruncMap.values()), node);

            for(Map.Entry<Long,String> orig : origUids.entrySet()){
                long localid = localTruncMap.get(orig.getKey());
                SearchModel sm = searchModelMap.get(orig.getKey());
                if(nations.get(localid) != null) {
                    sm.setNation(nations.get(localid));
                }
                alluids.put(orig.getValue(), sm);
            }
            writeToLocalFile(alluids, day);
        }

    }

    public void writeToLocalFile(Map<String, SearchModel> alluids, String day) {
        System.out.println("-------------------------start to write to file-----------------------");
        BufferedWriter bw = null;
        try {
            String fileName = BAUtil.getSearchFileName(day);
            File file = new File(fileName);

            if(!file.getParentFile().exists()) {
                if(!file.getParentFile().mkdirs()) {
                    System.out.println("fail to create File！");
                }
            }

            bw = new BufferedWriter(new FileWriter(file, true));
            for(Map.Entry<String, SearchModel> smp : alluids.entrySet()) {
                String origUid = smp.getKey();
                SearchModel sm = smp.getValue();
                CacheModel cm = sm.getCacheModel();
                StringBuffer sb = new StringBuffer();
                sb.append(origUid).append("\t").append(sm.getNation()).append("\t").append(sm.getEvt3()).append("\t").append(sm.getEvt4()).append("\t").append(sm.getEvt5()).append("\t").append(cm.getUserTime()).append("\t").append(cm.getValue()).append("\n");
                bw.write(sb.toString());
            }

            System.out.println("-------------------------start to write to " + fileName);
            bw.flush();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bw.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void uploadToHdfs(String day) {
        try {
            Path src = null;
            Path dst = null;
            FileSystem fs = FileSystem.get(new Configuration());
            for(String type : types) {
                src =new Path("/data/log/ba/search/" + day + "/" + type + "/");
                dst = new Path(Constant.HDFS_SEARCH_PATH + day + "/" + type + "/");
//                FileSystem fs = dst.getFileSystem(new Configuration());
                fs.copyFromLocalFile(src, dst);
            }
System.out.println("------------------------------upload over---------------------------");
        } catch (IOException e) {
            e.printStackTrace();
        }

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

