package com.xingcloud.nba.service;

import com.google.gson.internal.Pair;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.nba.model.CacheModel;
import com.xingcloud.nba.utils.BAUtil;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

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
    private static byte[] family = Bytes.toBytes("'");
    private static byte[] qualifier = Bytes.toBytes("'");


    public Set<String> getHBaseUID(String day, String event, String[] projects) throws Exception{
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

        return null;
    }


    public Map<Long, String> getProperties(String project, String property, Set<Long> uids, String node) throws SQLException {
        if (uids.size() == 0) {
            return new HashMap<Long, String>();
        }
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        StringBuilder sql = new StringBuilder("select t.uid, t.val from `deu_");
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
        StringBuilder sql = new StringBuilder("select t.id, t.orig_id from `vf_");
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

    private ScanHBaseUID() {
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
    ScanHBaseUID query ;
    String[] projects;
    boolean     maxVersion = false;

    public ScanUID(String node,String day,String event, String[] projects){
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
        scan.addColumn(family,qualifier);

        scan.setCaching(10000);
        Set<String> uids = new HashSet<String>();
        Map<String,Pair<String,CacheModel>> alluids = new HashMap<String, Pair<String, CacheModel>>();
        for(String table : projects){
            uids.addAll(scan(conf, scan, table,alluids));
        }
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

    private Set<String> scan(Configuration conf, Scan scan, String tableName, Map<String,Pair<String,CacheModel>> alluids) throws Exception{
        HTable table = new HTable(conf,"deu_" + tableName);
        ResultScanner scanner = table.getScanner(scan);

        Map<Long,CacheModel> cacheModelMap = new HashMap<Long,CacheModel>();
        Set<String> uids = new HashSet<String>();
        Map<Long,Long> localTruncMap = new HashMap<Long, Long>();

        try{
            for(Result r : scanner){
                long uid = BAUtil.transformerUID(Bytes.tail(r.getRow(), 5));

                CacheModel cm = new CacheModel();
                for(KeyValue kv : r.raw()){
                    cm.incrSameUser(Bytes.toBigDecimal(kv.getValue()));
                }

                long truncUid = BAUtil.truncate(uid);

                localTruncMap.put(truncUid,uid);
                cacheModelMap.put(truncUid,cm);
            }
        }finally {
            scanner.close();
            table.close();
        }
        //truncUID ==> orig_uid
        Map<Long,String> origUids = query.executeSqlTrue(tableName,localTruncMap.keySet());
        //localUID ==> nation
        Map<Long,String> nations = query.getProperties(tableName, "nation", new HashSet<Long>(localTruncMap.values()), node);

        //merge
        for(Map.Entry<Long,String> orig : origUids.entrySet()){
            Pair<String,CacheModel> nation = alluids.get(orig.getValue());
            long localid = localTruncMap.get(orig.getKey());

            if(nation == null){
                nation = new Pair(nations.get(localid), cacheModelMap.get(orig.getKey()));
                alluids.put(orig.getValue(), nation);
            }else{
                nation.second.incrSameUser(cacheModelMap.get(orig.getKey()).getValue());
            }
        }

        return uids;
    }
}

}

