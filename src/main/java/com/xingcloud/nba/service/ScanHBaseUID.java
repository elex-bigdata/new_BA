package com.xingcloud.nba.service;

import com.xingcloud.nba.utils.BAUtil;
import com.xingcloud.nba.utils.Constant;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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

    public static void main(String[] args) throws Exception{
        ScanHBaseUID test = new ScanHBaseUID();
        Map<String, List<String>> specialProjectList = getSpecialProjectList();
        Set<String> res = test.getHBaseUID("20141129", "pay.search2", specialProjectList.get(Constant.INTERNET1));
        System.out.println(res.size());
    }

    public Set<String> getHBaseUID(String day, String event, List projects) throws Exception{
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



class ScanUID implements Callable<Set<String>>{

    String node;
    byte[] startKey;
    byte[] endKey;
    ScanHBaseUID query;
    List<String> projects;
    boolean     maxVersion = false;

    public ScanUID(String node,String day,String event, List projects){
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
        scan.setMaxVersions();

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
                //r.getValue(family, qualifier);
                long truncUid = BAUtil.truncate(uid);
                localUIDs.add(truncUid);
            }
        }finally {
            scanner.close();
            table.close();
        }
        System.out.println("localuid sizes:========================= " + localUIDs.size());
        Map<Long,String> orgUids = query.executeSqlTrue(tableName,localUIDs);
        System.out.println("orgUids sizes:========================= " + orgUids.size());
        uids.addAll(orgUids.values());

        return uids;
    }
}

}

