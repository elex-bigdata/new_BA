package com.xingcloud.nba.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Author: liqiang
 * Date: 14-8-26
 * Time: 上午11:32
 */
public class HiveJdbcClient {

    private static HiveJdbcClient instance = null;
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private HiveJdbcClient(){
        try {
            Class.forName(driverName);
//            conn = DriverManager.getConnection("jdbc:hive2://69.28.58.30:10000/default", "", "");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static synchronized HiveJdbcClient getInstance(){
        if(instance == null){
            instance = new HiveJdbcClient();
        }
        return instance;
    }

    public synchronized Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:hive2://69.28.58.30:10000/default", "", "");
    }

}