package com.xingcloud.nba.task;

import com.xingcloud.nba.hive.HiveJdbcClient;
import org.apache.log4j.Logger;

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

    private static final Logger LOGGER = Logger.getLogger(PlainSQLExcecutor.class);
    String[] sqls = null;

    public PlainSQLExcecutor(String[] sqls){
        this.sqls = sqls;
    }

    @Override
    public String call() throws Exception {
        Connection conn = HiveJdbcClient.getInstance().getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("add jar hdfs://ELEX-LA-WEB1:19000/user/hadoop/hive_udf/udf.jar");
        stmt.execute("create temporary function md5uid as 'com.elex.hive.udf.MD5UID' ");

        for(String sql : sqls){
            try{
                LOGGER.debug(sql);
                stmt.executeQuery(sql);
            }catch(Exception e){
                LOGGER.error("error when execute: " +  sql,e);
            }
        }

        stmt.close();
        return "success";
    }
}