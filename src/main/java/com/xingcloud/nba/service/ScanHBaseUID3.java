package com.xingcloud.nba.service;

import com.google.gson.internal.Pair;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.nba.hive.HiveJdbcClient;
import com.xingcloud.nba.model.CacheModel;
import com.xingcloud.nba.model.GroupModel;
import com.xingcloud.nba.model.SearchModel;
import com.xingcloud.nba.task.WriteFileWorker;
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
import java.util.concurrent.*;

public class ScanHBaseUID3 {

    private BasicDataSource ds;
    private static byte[] family = Bytes.toBytes("val");
    private static byte[] qualifier = Bytes.toBytes("val");
    private Connection conn = null;
    private String[] types = {Constant.NATION, Constant.EV3, Constant.EV4, Constant.EV5};

    public static BlockingQueue<String> CONTENT_QUEUE = new LinkedBlockingQueue<String>();

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
            test.alterTable(day);
        }
    }

    public String generateCacheKey(String start, String end, String ev3, String ev4, String ev5, String nation, String grp) {
        String cacheKey = "";
        String commHead = "COMMON,internet-1," + start + "," + end + ",pay.search2.";
        String totalUser = ",TOTAL_USER";
        String commTail = ",VF-ALL-0-0,PERIOD";
        String groupHead = "GROUP,internet-1," + start + "," + end + ",pay.search2.";
        String evtGroupTail = ",VF-ALL-0-0,EVENT,";
        String natGroupTail = ",VF-ALL-0-0,USER_PROPERTIES,nation";

        String events = "";
        StringBuffer sb = new StringBuffer();
        sb.append(ev3);
        if(ev5.equals("*")) {
            if(ev4.equals("*")) {
                if(ev3.equals("*")) {
                    events = sb.toString();
                } else {
                    sb.append(".*");
                    events = sb.toString();
                }
            } else {
                sb.append(".").append(ev4).append(".*");
                events = sb.toString();
            }
        } else {
            sb.append(".").append(ev4).append(".").append(ev5).append(".*");
            events = sb.toString();
        }

        sb = new StringBuffer();
        if(nation.equals("*")) {
            if(grp.equals("6")) {
                sb.append(commHead).append(events).append(totalUser).append(commTail);
            } else {
                sb.append(groupHead).append(events).append(totalUser);
                if(grp.equals("5")) {
                    sb.append(natGroupTail);
                } else {
                    sb.append(evtGroupTail).append(grp);
                }
            }
        } else {
            if(grp.equals("6")) {
                sb.append(commHead).append(events).append(",{\"nation\":\"").append(nation).append("\"}").append(commTail);
            } else {
                sb.append(groupHead).append(events).append(",{\"nation\":\"").append(nation).append("\"}").append(evtGroupTail).append(grp);
            }
        }
        cacheKey = sb.toString();

        return cacheKey;
    }

    public Map<String, Map<String,Number[]>> getResults(String day, String event, List projects) throws Exception {
        Map<String, Map<String,Number[]>> kv = new HashMap<String, Map<String,Number[]>>();
        String yesterdayKey = day + " 00:00";
        String scanDay = DateManager.getDaysBefore(day, 1);
        String valueKey = scanDay + " 00:00";
        String date = scanDay.replace("-","");
        String start = DateManager.getDaysBefore(day, 6);
        String end = DateManager.dayfmt.format(DateManager.dayfmt.parse(day));

        getHBaseUID(date, "pay.search2", projects);
        uploadToHdfs(date);
        alterTable(date);


        String sql = "select new.ev3, new.ev4, new.ev5, new.nation, new.grp, new.grpkey, count(distinct uid),sum(count),sum(value) from (select u.uid, mytable.ev3, mytable.ev4," +
                " mytable.ev5, mytable.nation, mytable.grp, mytable.grpkey, u.count, u.value from user_search u lateral view transEvent(events) mytable as ev3, ev4, ev5, nation, grp," +
                " grpkey  where day='" + date + "') new group by new.ev3, new.ev4, new.ev5, new.nation, new.grp, new.grpkey";
        Statement stmt = conn.createStatement();
//        System.out.print("------------------111-------------------");
        stmt.execute("add jar hdfs://ELEX-LA-WEB1:19000/user/hadoop/hive_udf/udf-1.jar");
//        System.out.print("------------------222-------------------");
        stmt.execute("create temporary function transEvent as 'com.elex.hive.udf.ExplodeMap' ");
        stmt.execute("set mapred.max.split.size=16000000");
        System.out.print("------------------333-------------------");
        ResultSet res = stmt.executeQuery(sql);
        System.out.print("------------------444-------------------");


        //hppp    cor     google  br      6       -       1       1       800
        String cachekey = "";
        Map<String, Number[]> grpMap = null;
        while (res.next()) {
            String ev3 = res.getString(1);
            String ev4 = res.getString(2);
            String ev5 = res.getString(3);
            String nation = res.getString(4);
            String grp = res.getString(5);
            String grpKey = res.getString(6);
            int num = res.getInt(7);
            int count = res.getInt(8);
            long value = res.getLong(9);

            cachekey = generateCacheKey(scanDay, scanDay, ev3, ev4, ev5, nation, grp);

            if(grp.equals("6")) {//common
                Map<String, Number[]> commMap = generateCacheValue(valueKey, count, BigDecimal.valueOf(value), num);
                kv.put(cachekey, commMap);
            } else {//group
                if(kv.get(cachekey) != null) {
                    grpMap = kv.get(cachekey);
                    grpMap.put(grpKey, new Number[]{count, value, num, 1.0});
                } else {
                    grpMap = new HashMap<String, Number[]>();
                    grpMap.put(grpKey, new Number[]{count, value, num, 1.0});
                    kv.put(cachekey, grpMap);
                }
            }

        }

        String startDay = start.replace("-", "");
        String endDay = date;
        sql = "select new.ev3, new.ev4, new.ev5, new.nation, new.grp, new.grpkey, count(distinct uid),sum(count),sum(value) from (select u.uid, mytable.ev3, mytable.ev4," +
                " mytable.ev5, mytable.nation, mytable.grp, mytable.grpkey, u.count, u.value from user_search u lateral view transEvent(events) mytable as ev3, ev4, ev5, nation, grp," +
                " grpkey  where day>='" + startDay + "' and day <= '" + endDay + "') new group by new.ev3, new.ev4, new.ev5, new.nation, new.grp, new.grpkey";
        stmt = conn.createStatement();
        stmt.execute("add jar hdfs://ELEX-LA-WEB1:19000/user/hadoop/hive_udf/udf-1.jar");
        stmt.execute("create temporary function transEvent as 'com.elex.hive.udf.ExplodeMap' ");
        stmt.execute("set mapred.max.split.size=32000000");
        res = stmt.executeQuery(sql);

        while (res.next()) {
            String ev3 = res.getString(1);
            String ev4 = res.getString(2);
            String ev5 = res.getString(3);
            String nation = res.getString(4);
            String grp = res.getString(5);
            String grpKey = res.getString(6);
            int num = res.getInt(7);
            int count = res.getInt(8);
            long value = res.getLong(9);

            cachekey = generateCacheKey(start, end, ev3, ev4, ev5, nation, grp);

            if(grp.equals("6")) {//common
                Map<String, Number[]> commMap = generateCacheValue(valueKey, count, BigDecimal.valueOf(value), num);
                kv.put(cachekey, commMap);
            } else {//group
                if(kv.get(cachekey) != null) {
                    grpMap = kv.get(cachekey);
                    grpMap.put(grpKey, new Number[]{count, value, num, 1.0});
                } else {
                    grpMap = new HashMap<String, Number[]>();
                    grpMap.put(grpKey, new Number[]{count, value, num, 1.0});
                    kv.put(cachekey, grpMap);
                }
            }

        }
        /*
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
        }*/

        return kv;
    }


    public Map<String,Number[]> generateCacheValue(String key, int count, BigDecimal value, int num){
        Map<String, Number[]> result  = new HashMap<String, Number[]>();
        result.put(key, new Number[]{count, value, num, 1.0});
        return result;
    }

    public void getHBaseUID(String day, String event, List projects) throws Exception{
        ExecutorService service = Executors.newFixedThreadPool(16);
//        List<Future<Map<String, Object>>> tasks = new ArrayList<Future<Map<String, Object>>>();
        String fileName = BAUtil.getSearchFileName(day);
        for(int i = 0; i < 16; i++){
            service.execute(new ScanUID("node" + i, day, event, projects));
        }

        new Thread(new WriteFileWorker(fileName)).start();

        service.shutdown();
        System.out.println("-------------------------write file over------------------ ");
    }

    public void alterTable(String day) throws SQLException {
        //alter table user_search add partition(day='20141207', type='nation') location '/hadoop/user/search/20141207/nation'
        Statement stmt = conn.createStatement();
        String sql = "alter table user_search add partition(day='" + day + "') location '/hadoop/user/search/" + day + "'";
        try{
            stmt.execute(sql);
        }catch (Exception e){
            e.printStackTrace();
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

            CONTENT_QUEUE.add(Constant.END);
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

                    String e3 = "*";
                    String e4 = "*";
                    String e5 = "*";
                    String nation = "*";
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

                    if(searchModelMap.get(truncUid) != null) {//这里有重复的truncUid,实际上是不同的uid
                        if(searchModelMap.get(truncUid).getCacheModel() != null) {
                            CacheModel cn = searchModelMap.get(truncUid).getCacheModel();
                            cn.incrSameUserInDifPro(cm);
                        }
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
            addToQueue(alluids);
        }

    }

    public void addToQueue(Map<String, SearchModel> alluids) {
        for(Map.Entry<String, SearchModel> smp : alluids.entrySet()) {
            String origUid = smp.getKey();
            SearchModel sm = smp.getValue();
            CacheModel cm = sm.getCacheModel();
            StringBuffer sb = new StringBuffer();
            sb.append(origUid).append("\t").append(sm.getEvt3()).append(",").append(sm.getEvt4()).append(",").append(sm.getEvt5()).append(",").append(sm.getNation()).append("\t").append(cm.getUserTime()).append("\t").append(cm.getValue()).append("\n");
            CONTENT_QUEUE.add(sb.toString());
        }

    }

    public void uploadToHdfs(String day) {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            Path src =new Path("/data/log/ba/search/" + day + "/");
            Path dst = new Path(Constant.HDFS_SEARCH_PATH + day + "/");
            fs.copyFromLocalFile(src, dst);
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

