package com.xingcloud.nba;

        import com.xingcloud.nba.hive.HiveJdbcClient;

        import java.sql.Connection;
        import java.sql.DriverManager;
        import java.sql.ResultSet;
        import java.sql.Statement;

/**
 * Created by Administrator on 14-7-29.
 */
public class Test {
    public static void main(String[] args) throws Exception {
//        Connection conn = HiveJdbcClient.getInstance().getConnection();
//        Statement stmt = conn.createStatement();
        /*String querySQL="SELECT * FROM default.user_register_time limit 3";

        ResultSet res = stmt.executeQuery(querySQL);

        while (res.next()) {
            System.out.println(res.getString(1));
        }*/


        /*for(int i = 9; i < 26; i++) {
            String command = "load data inpath '/user/hadoop/offline/uid/internet/201408";
            if(i < 10) {
                command = command + "0" + i + "/part*' overwrite into table user_visit partition(pid='internet',day='2014-08-" + "0" + i + "')";
            } else {
                command = command + i + "/part*' overwrite into table user_visit partition(pid='internet',day='2014-08-" + i + "')";
            }
            System.out.println(command);
            stmt.execute(command);
        }*/

        /*String command = "load data inpath '/user/hadoop/offline/uid/internet-1/20140805/part*' overwrite into table user_visit  partition(pid='internet-1',day='2014-08-05')";
        stmt.execute(command);*/

        Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection("jdbc:hive://69.28.58.30:10000/default", "", "");
        Statement stmt = conn.createStatement();
        String command = args[0];
        stmt.execute("add jar /home/hadoop/liqiang/udf.jar");
        stmt.execute("create temporary function md5uid as 'com.elex.hive.udf.MD5UID'");

        stmt.close();
    }
}
