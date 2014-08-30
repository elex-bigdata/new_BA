package com.xingcloud.nba.task;

import com.xingcloud.nba.hive.HiveJdbcClient;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.Callable;

/**
 * 用于并发的执行无返回值的SQL
 * Author: liqiang
 * Date: 14-8-27
 * Time: 下午2:18
 */
public class PlainSQLExcecutor implements Callable<String> {

    String[] sql = null;
    long begin = System.currentTimeMillis();

    public PlainSQLExcecutor(String[] sql){
        this.sql = sql;
    }

    @Override
    public String call() throws Exception {
        Connection conn = HiveJdbcClient.getInstance().getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("add jar hdfs://ELEX-LA-WEB1:19000/user/hadoop/liqiang/udf.jar");
        stmt.execute("create temporary function md5uid as 'com.elex.hive.udf.MD5UID' ");

        for(String s : sql){
            try{
            stmt.executeQuery(s);
            System.out.println(" Speed " + (System.currentTimeMillis() - begin) + " to execute : " + s);
            }catch(Exception e){
                System.out.println("error when execute: " +  s);
                e.printStackTrace();
            }
        }

        stmt.close();
        return "success";
    }
}