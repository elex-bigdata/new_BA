package com.xingcloud.nba.service;

import com.xingcloud.nba.utils.BAUtil;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
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

    public Set<String> getHBaseUID(String day, String event, String[] projects) throws Exception{
        ExecutorService service = Executors.newFixedThreadPool(16);
        List<Future<Set<String>>> tasks = new ArrayList<Future<Set<String>>>();

        ScanHBaseUID shu = new ScanHBaseUID();
        for(int i=0;i<16;i++){
            tasks.add(service.submit(new ScanUID("node" + i, day, event, projects)));
        }
        Set<String> allUid = new HashSet<String>();
        for(Future<Set<String>> uids : tasks){
            try{
                allUid.addAll(uids.get());
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        service.shutdownNow();

        return allUid;
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



class ScanUID implements Callable<Set<String>>{

    String node;
    byte[] startKey;
    byte[] endKey;
    ScanHBaseUID query;
    String[] projects;
    boolean     maxVersion = false;

    public ScanUID(String node,String day,String event, String[] projects){
        this.node = node;
        this.startKey = Bytes.toBytes(day + event);
        this.endKey = Bytes.toBytes(BAUtil.asciiIncrease(day + event));
        this.projects = projects;
    }

    @Override
    public Set<String> call() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", node);
        conf.set("hbase.zookeeper.property.clientPort", "3181");

        Scan scan = new Scan();
        scan.setStartRow(startKey);
        scan.setStopRow(endKey);
        if(maxVersion){
            scan.setMaxVersions();
        }else{
            scan.setMaxVersions(1);
            scan.setFilter(new KeyOnlyFilter());
        }
        scan.setCaching(10000);
        Set<String> uids = new HashSet<String>();
        for(String table : projects){
            uids.addAll(scan(conf, scan, table));
        }
        return uids;
    }

    private Set<String>  scan(Configuration conf, Scan scan, String tableName) throws Exception{

        HTable table = new HTable(conf,"deu_" + tableName);
        ResultScanner scanner = table.getScanner(scan);

        Set<Long> localUIDs = new HashSet<Long>();
        Set<String> uids = new HashSet<String>();
        try{
            for(Result r : scanner){
                long uid = BAUtil.transformerUID(Bytes.tail(r.getRow(), 5));
                long truncUid = BAUtil.truncate(uid);
                localUIDs.add(truncUid);
            }
        }finally {
            scanner.close();
            table.close();
        }
        Map<Long,String> orgUids = query.executeSqlTrue(tableName,localUIDs);
        uids.addAll(orgUids.values());

        return uids;
    }
}

}

