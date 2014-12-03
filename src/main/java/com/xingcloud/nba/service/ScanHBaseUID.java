package com.xingcloud.nba.service;

import com.google.gson.internal.Pair;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.nba.model.CacheModel;
import com.xingcloud.nba.utils.BAUtil;
import com.xingcloud.nba.utils.DateManager;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

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

    public static void main(String[] args) throws Exception{
        ScanHBaseUID test = new ScanHBaseUID();
        List<String> proj = new ArrayList<String>();
        proj.add("v9");
        Map<String,CacheModel> res = test.getHBaseUID("20141130", "pay.search2", proj);
        int sum_num = 0;
        int sum_time = 0;
        BigDecimal sum_value = new BigDecimal(0);
        for(Map.Entry<String,CacheModel> nr : res.entrySet()) {
//            System.out.print(nr.getKey() + "---");
            CacheModel cm = nr.getValue();
            sum_num += cm.getUserNum();
            sum_time += cm.getUserTime();
            sum_value = sum_value.add(cm.getValue());
//            System.out.print(cm);
//            System.out.println();
        }
        System.out.println("sum values-------------" + sum_num + "#" + sum_time + "#" + sum_value);

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
        Map<String,CacheModel> res = getHBaseUID(date, event, projects);
        int sum_num = 0;
        int sum_time = 0;
        BigDecimal sum_value = new BigDecimal(0);
        Map<String, Number[]> groupResult  = new HashMap<String, Number[]>();
        for(Map.Entry<String,CacheModel> nr : res.entrySet()) {
            CacheModel cm = nr.getValue();
            sum_num += cm.getUserNum();
            sum_time += cm.getUserTime();
            sum_value = sum_value.add(cm.getValue());
            groupResult.put(nr.getKey(), new Number[]{cm.getUserTime(), cm.getValue(), cm.getUserNum(), 1.0});
        }
        String nationKey = "GROUP,internet-1," + scanDay + "," + scanDay + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,USER_PROPERTIES,nation";
        kv.put(nationKey, groupResult);

        String searchKey = "COMMON,internet-1," + scanDay + "," + scanDay + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
        Map<String, Number[]> result  = generateCacheValue(valueKey, sum_time, sum_value, sum_num);
        kv.put(searchKey, result);

        //清掉昨天的缓存
        String yes_nationKey = "GROUP,internet-1," + day + "," + day + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,USER_PROPERTIES,nation";
        groupResult = generateCacheValue("null", 0, new BigDecimal(0), 0);
        kv.put(yes_nationKey, groupResult);

        String yes_searchKey = "COMMON,internet-1," + day + "," + day + ",pay.search2.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
        result = generateCacheValue(yesterdayKey, 0, new BigDecimal(0), 0);
        kv.put(yes_searchKey, result);

        return kv;
    }

    public Map<String,Number[]> generateCacheValue(String key, int count, BigDecimal value, int num){
        Map<String, Number[]> result  = new HashMap<String, Number[]>();
        result.put(key, new Number[]{count, value, num, 1.0});
        return result;
    }

    public Map<String,CacheModel> getHBaseUID(String day, String event, List projects) throws Exception{
        ExecutorService service = Executors.newFixedThreadPool(16);
        List<Future<Map<String,CacheModel>>> tasks = new ArrayList<Future<Map<String,CacheModel>>>();

        for(int i=0;i<16;i++){
            tasks.add(service.submit(new ScanUID("node" + i, day, event, projects)));
        }

        Map<String,CacheModel> allResult = new HashMap<String, CacheModel>();
        for(Future<Map<String,CacheModel>> uids : tasks){
            try{
                Map<String,CacheModel> nodeResult = uids.get();
                for(Map.Entry<String,CacheModel> nr : nodeResult.entrySet()){
                    CacheModel cm = allResult.get(nr.getKey());
                    if(cm == null){
                        allResult.put(nr.getKey(), nr.getValue());
                    }else{
                        cm.incrDiffUser(nr.getValue());
                    }
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        service.shutdownNow();

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
//System.out.println(uids.size() + "--------------------------------------" + idmap.size());
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
        System.out.println(uids.size() + "--------------------------------------" + idmap.size());
        return idmap;
    }

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

class ScanUID implements Callable<Map<String,CacheModel>>{

    String node;
    byte[] startKey;
    byte[] endKey;
    List<String> projects;
    boolean maxVersion = false;

    public ScanUID(String node,String day,String event, List projects){
        this.node = node;
        this.startKey = Bytes.toBytes(day + event);
        this.endKey = Bytes.toBytes(BAUtil.asciiIncrease(day + event));
        this.projects = projects;
    }

    @Override
    public Map<String,CacheModel> call() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", node);
        conf.set("hbase.zookeeper.property.clientPort", "3181");

        Scan scan = new Scan();
        scan.setStartRow(startKey);
        scan.setStopRow(endKey);
        scan.setMaxVersions();
        scan.addColumn(family, qualifier);

        scan.setCaching(10000);
        Map<String,Pair<String,CacheModel>> alluids = new HashMap<String, Pair<String, CacheModel>>();
        for(String table : projects){
            scan(conf, scan, table, alluids);
        }

        /*for(Map.Entry<String,Pair<String,CacheModel>> nr : alluids.entrySet()) {
            System.out.print(nr.getKey() + "---");
            System.out.print(nr.getValue().first + "---");
            System.out.print(nr.getValue().second);
            System.out.println();
        }*/

        Map<String,CacheModel> results = new HashMap<String, CacheModel>();
        for(Pair<String,CacheModel> nations : alluids.values()){
            CacheModel cm = results.get(nations.first);
            if(cm == null){
                results.put(nations.first, nations.second);
            }else{
                cm.incrDiffUser(nations.second);
            }
        }
        return results;
    }

    private void scan(Configuration conf, Scan scan, String tableName, Map<String,Pair<String,CacheModel>> alluids) throws Exception{
        HTable table = new HTable(conf,"deu_" + tableName);
        ResultScanner scanner = table.getScanner(scan);

        Map<Long,CacheModel> cacheModelMap = new HashMap<Long,CacheModel>();
        Map<Long,Long> localTruncMap = new HashMap<Long, Long>();
        int count = 0;
        int countc = 0;
        int usercount = 0;
        try{

            for(Result r : scanner){
                long uid = BAUtil.transformerUID(Bytes.tail(r.getRow(), 5));

                CacheModel cm = new CacheModel();
                cm.setUserNum(1);
                for(KeyValue kv : r.raw()){
                    cm.incrSameUser(Bytes.toBigDecimal(kv.getValue()));
                    count++;
                }
                countc += cm.getUserTime();

                long truncUid = BAUtil.truncate(uid);
//                long truncUid = Bytes.toInt(Bytes.tail(r.getRow(),4));
                usercount ++;

                localTruncMap.put(truncUid,uid);
                if(cacheModelMap.get(truncUid) != null) {
                    CacheModel cn = cacheModelMap.get(truncUid);
                    cn.incrSameUserInDifPro(cm);
                } else {
                    cacheModelMap.put(truncUid,cm);
                }

            }

        }finally {
            scanner.close();
            table.close();
        }
        //truncUID ==> orig_uid
        Map<Long,String> origUids = executeSqlTrue(tableName,localTruncMap.keySet());
        System.out.println("origuid size:" + origUids.size() + ", uid size:" + localTruncMap.size() + ", usercount：" +usercount);
        //localUID ==> nation
        Map<Long,String> nations = getProperties(tableName, "nation", new HashSet<Long>(localTruncMap.values()), node);
        //merge
        int countB = 0;
        for(Map.Entry<Long,String> orig : origUids.entrySet()){
            Pair<String,CacheModel> nation = alluids.get(orig.getValue());
            long localid = localTruncMap.get(orig.getKey());

            if(nation == null){
                if(nations.get(localid) == null) {
                    System.out.println(" NA nation ");
                    nation = new Pair("NA", cacheModelMap.get(orig.getKey()));
                } else {
                    nation = new Pair(nations.get(localid), cacheModelMap.get(orig.getKey()));
                }

                alluids.put(orig.getValue(), nation);
            }else{
                nation.second.incrSameUserInDifPro(cacheModelMap.get(orig.getKey()));
            }
            countB += cacheModelMap.get(orig.getKey()).getUserTime();
        }

        System.out.println(node + "---------------------------" + count + ",countB=" + countB + ", countC=" +countc) ;

    }
}

}

