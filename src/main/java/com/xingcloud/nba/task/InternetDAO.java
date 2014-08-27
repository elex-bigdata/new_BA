package com.xingcloud.nba.task;

import com.xingcloud.nba.hive.HiveJdbcClient;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;


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
            stmt.execute(sql);
        }
    }

    //活跃(日，周，月)
    public long countActiveUser(String[] day, String[] project) throws Exception {

        if(project.length ==0 || project.length > 2 || day.length ==0 || day.length> 2){
            throw new Exception("Invalid project size");
        }
        String sql = "";

        String daySQL = "day = '"+day[0]+"'";
        if(day.length == 2){
            daySQL = "day >= '"+day[0]+"' and day<='"+day[1]+"'";
        }

        if(project.length == 1){ //internet1 or internet2
            sql =  "select count(*) from user_visit where "+ daySQL +" and pid = '" + project[0] + "'";
        }else if(project.length == 2){ //internet
            sql = "select count(distinct orig_id) user_visit where "+ daySQL + " and pid in ('" + project[0] + "','" + project[1] + "') ";
        }

        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        long count = 0;
        if (res.next()) {
            count = res.getLong(1);
        }
        return count;
    }

    //新用户
    public long countNewUser(String day, String... project) throws Exception {

        int len = project.length;
        if(len ==0 || len > 2 ){
            throw new Exception("Invalid project size");
        }

        //将2014-08-26格式转换为 20140826
        String time = day.replaceAll("-","");

        String sql = "";
        if(project.length == 1){ //internet1 or internet2
            sql =  "select count(*) from user_register_time  where min_reg_time='"+time+"' and pid = '" + project[0] + "'";
        }else if(project.length == 2){ //internet
            sql = "select count(distinct orig_id) user_register_time  where min_reg_time='"+time+"' and pid in ('" + project[0] + "','" + project[1] + "') ";
        }

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
    public long countRetentionUser(String regDay, String[] visitDay, String... project) throws Exception {
        //TODO: 不限制长度，internet-N
        if(project.length ==0 || project.length > 2 || visitDay.length ==0 || visitDay.length> 2){
            throw new Exception("Invalid project size");
        }
        String sql = "";

        String daySQL = "uv.day = '"+visitDay[0]+"'";
        if(visitDay.length == 2){
            daySQL = "uv.day >= '"+visitDay[0]+"' and uv.day<='"+visitDay[1]+"'";
        }

        String pidSQL = " uv.pid = " + project[0];
        if(project.length ==2){
            pidSQL = " uv.pid in ('" + project[0] + "','" + project[1] + "') ";
        }

        if(project.length == 1){ //internet1 or internet2
            sql =  "select count(*) from user_visit  uv join user_register_time ur on uv.orig_id = ur.orig_id " +
                    "and uv.pid = ur.pid and uv.pid = '"+project[0]+"' and ur.min_reg_time = '"+regDay+"' and " + daySQL;
        }else if(project.length == 2){ //internet
            sql = "select count(distinct uv.orig_id) from user_visit  uv join user_register_time ur on uv.orig_id = ur.orig_id " +
                    "and uv.pid = ur.pid and uv.pid in ('"+project[0]+"','"+project[1]+"') and ur.min_reg_time = '"+regDay+"' and " + daySQL;
        }

        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        long count = 0;
        if (res.next()) {
            count = res.getLong(1);
        }
        return count;
    }

}
