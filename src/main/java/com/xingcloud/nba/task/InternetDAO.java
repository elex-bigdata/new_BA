package com.xingcloud.nba.task;

import com.xingcloud.nba.hive.HiveJdbcClient;
import com.xingcloud.nba.utils.Constant;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 单线程执行
 * Author: liqiang
 * Date: 14-8-27
 * Time: 下午2:39
 */
public class InternetDAO {

    private Connection conn = null;
    public InternetDAO() {
        try {
            conn = HiveJdbcClient.getInstance().getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    //stream_log加入分区
    public void alterTable(List<String> pids, String day) throws SQLException {
        //alter table user_event add partition(day='2014-08-25',pid='%s')   location '/user/hadoop/stream_log/pid/2014-08-25/%s'
        Statement stmt = conn.createStatement();
        for(String pid: pids){
            String sql = "alter table user_event add partition(day='"+day+"'," +
                    "pid='"+pid+"')   location '/user/hadoop/stream_log/pid/"+day + "/" +pid+"'";
            try{
                stmt.execute(sql);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    //活跃(日，周，月)
    public long countActiveUser(String[] day, String project) throws Exception {


        String daySQL = "day = '"+day[0]+"'";
        if(day.length == 2){
            daySQL = "day > '"+day[0]+"' and day<='"+day[1]+"'";
        }

        String sql =  "select count(*) from user_visit where "+ daySQL +" and pid = '" + project + "'";

        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        long count = 0;
        if (res.next()) {
            count = res.getLong(1);
        }
        return count;
    }

    //新用户
    public long countNewUser(String day, String project) throws Exception {

        //将2014-08-26格式转换为 20140826
        String time = day.replaceAll("-","");

        String sql = "select count(*) from user_register_time  where min_reg_time='"+time+"' and pid = '" + project + "'";

        System.out.println(sql);
        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        long count = 0;
        if (res.next()) {
            count = res.getLong(1);
        }
        return count;
    }

    //internet-1覆盖 即当天注册的用户在其他项目已经注册过
    public long countNewCoverUser(String day,String project) throws SQLException {
        String time = day.replaceAll("-","");
        String sql = "select count(*) from user_register_time where pid='"+project+"' and max_reg_time = '"+time+"' and min_reg_time < max_reg_time";
        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        long count = 0;
        if (res.next()) {
            count = res.getLong(1);
        }
        return count;
    }

    //留存 Retention (2日 7日 一周)
    public long countRetentionUser(String regDay, String[] visitDay, String project) throws Exception {

        String daySQL = "uv.day = '"+visitDay[0]+"'";
        if(visitDay.length == 2){
            daySQL = "uv.day >= '"+visitDay[0]+"' and uv.day<='"+visitDay[1]+"'";
        }

        String sql =  "select count(*) from user_visit  uv join user_register_time ur on uv.orig_id = ur.orig_id " +
                    "and uv.pid = ur.pid and uv.pid = '"+project+"' and ur.min_reg_time = '"+regDay+"' and " + daySQL;

        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        long count = 0;
        if (res.next()) {
            count = res.getLong(1);
        }
        return count;
    }

    //COMMON,age,2014-08-25,2014-08-25,visit.*,{"geoip":"ua","register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,PERIOD
    //COMMON,age,2014-08-26,2014-08-26,visit.*,{"geoip":"ua","register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,PERIOD

    //GROUP,age,2014-08-25,2014-08-25,visit.*,{"register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,USER_PROPERTIES,geoip
    //GROUP,age,2014-08-26,2014-08-26,visit.*,{"register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,USER_PROPERTIES,geoip

    public Map<String,Long> countNewUserByGeoip(String day, String project) throws Exception {

        Map<String,Long> result = new HashMap<String, Long>();
        //将2014-08-26格式转换为 20140826
        String time = day.replaceAll("-","");

        String sql =  "select ug.val,count(*) from user_register_time ur join user_geoip ug on ur.origid = ug.origid " +
                    "and ur.pid = ug.pid and ur.pid = '"+project+" and where min_reg_time='"+time+" group by ug.val  ";

        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            String geoip = res.getString(1);
            Long count = res.getLong(2);
            result.put(geoip,count);
        }
        return result;
    }

    public Map<String,Long> countRetentionUserByGeoip(String regDay, String[] visitDay, String project) throws Exception {

        Map<String,Long> result = new HashMap<String, Long>();

        String daySQL = "uv.day = '"+visitDay[0]+"'";
        if(visitDay.length == 2){
            daySQL = "uv.day >= '"+visitDay[0]+"' and uv.day<='"+visitDay[1]+"'";
        }

        String sql =  "select ug.val , count(*) from user_visit  uv join user_register_time ur on uv.orig_id = ur.orig_id join user_geoip ug on ur.origid = ug.origid  " +
                    "and uv.pid = ur.pid and ur.pid = ug.pid and uv.pid = '"+project+"' and ur.min_reg_time = '"+regDay+"' and " + daySQL + " group by ug.val  ";

        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            String geoip = res.getString(1);
            Long count = res.getLong(2);
            result.put(geoip,count);
        }
        return result;
    }

}
