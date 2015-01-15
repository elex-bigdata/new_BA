package com.xingcloud.nba.task;

import com.xingcloud.nba.hive.HiveJdbcClient;
import com.xingcloud.nba.utils.Constant;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * 单线程执行
 * Author: liqiang
 * Date: 14-8-27
 * Time: 下午2:39
 */
public class InternetDAO {

    private static final Logger LOGGER = Logger.getLogger(InternetDAO.class);

    private Connection conn = null;
    public InternetDAO() {
        try {
            conn = HiveJdbcClient.getInstance().getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    //建表
    public void initTable()  throws SQLException {
        //mysql 属性表，pid为小项目ID，如：sof-isafe
        String userProperty = "create external table if not exists user_property(uid string, val string) partitioned by (pid string, prop string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'";
        //当日的stream log，pid为小项目ID
        String userEvent = "create external table if not exists user_event(p string,uid string,event string,value double,ts string) partitioned by (day string, pid string)   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'";
        //将所有ID转换为原始UID并去重的visit事件表，pid为大项目名：如internet-1
        String userVisit = "create table if not exists user_visit(orig_id string) partitioned by(pid string,day string)    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'";
        //将ID转换为原始UID并去重的用户注册时间表，pid为大项目名
        String userRegisterTime = "create table if not exists user_register_time(orig_id string, max_reg_time string, min_reg_time string) partitioned by(pid string) stored as rcfile";
        //将用户属性UID转换，并合并去重UID的属性表，pid为大项目名，attr为属性名，如：geoip、nation
        String userAttribute = "create table if not exists user_attribute(orig_id string, val string) partitioned by (pid string, attr string)  stored as rcfile";

        Statement stmt = conn.createStatement();
        stmt.execute(userProperty);
        stmt.execute(userEvent);
        stmt.execute(userVisit);
        stmt.execute(userRegisterTime);
        stmt.execute(userAttribute);
        stmt.close();
    }

    public void initPartition(List<String> pids, String[] attrs) throws SQLException {
        Statement stmt = conn.createStatement();
        String sql = "";
        for(String pid : pids){
            for(String attr : attrs){
                try{
                    sql = "alter table user_property add partition(pid='" + pid + "', prop='"+attr+"')  location '/user/hadoop/mysql/"+pid+"/"+attr+"'";
                    stmt.execute(sql);
                    System.out.println("success:" + sql);
                }catch(Exception e){
                    System.out.println("fail:" + sql);
                }
            }
            try{
                sql = "alter table user_id add partition(pid='"+pid+"')  location '/user/hadoop/mysqlidmap/vf_"+pid+"'";
                stmt.execute(sql);
                System.out.println("success:" + sql);
            }catch(Exception e){
                System.out.println("fail:" + sql);
            }
        }
        stmt.close();
        //alter table user_property add partition(pid='%s', prop='%s')   location '/user/hadoop/mysql/%s/%s'
    }

    // TODO: 需不需要close rs stmt conn？
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
        stmt.close();
    }

    //活跃(日，周，月)
    public long countActiveUser(String project,String[] day) throws Exception {

        String daySQL = "day = '"+day[0]+"'";
        if(day.length == 2){
            daySQL = "day > '"+day[0]+"' and day<='"+day[1]+"'";
        }

        String sql =  "select count(distinct lower(orig_id)) from user_visit where "+ daySQL +" and pid = '" + project + "'";

        LOGGER.debug(sql);
        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        long count = 0;
        if (res.next()) {
            count = res.getLong(1);
        }

        return count;
    }

    //新用户
    public long countNewUser(String project, String day) throws Exception {

        //将2014-08-26格式转换为 20140826
        String time = day.replaceAll("-","");
        String sql = "select count(distinct lower(orig_id)) from user_register_time  where min_reg_time='"+time+"' and pid = '" + project + "'";

        LOGGER.debug(sql);
        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        long count = 0;
        if (res.next()) {
            count = res.getLong(1);
        }
        return count;
    }

    //internet-1覆盖 即当天注册的用户在其他项目已经注册过 int1cover:覆盖 int1totalnew: 覆盖加新增
    //COMMON,internet-1,2014-08-24,2014-08-24,int1totalnew.*,{"register_time":{"$gte":"2014-08-24","$lte":"2014-08-24"}},VF-ALL-0-0,PERIOD
    //COMMON,internet-1,2014-08-24,2014-08-24,int1cover.*,{"register_time":{"$gte":"2014-08-24","$lte":"2014-08-24"}},VF-ALL-0-0,PERIOD
    public long countNewCoverUser(String project, String day) throws SQLException {
        String time = day.replaceAll("-","");
        String sql = "select count(distinct lower(orig_id)) from user_register_time where pid='"+project+"' and max_reg_time = '"+time+"' and min_reg_time < max_reg_time";
        LOGGER.debug(sql);
        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        long count = 0;
        if (res.next()) {
            count = res.getLong(1);
        }
        return count;
    }

    //留存 Retention (2日 7日 一周)
    public long countRetentionUser(String project, String regDay, String[] visitDay) throws Exception {

        String daySQL = "uv.day = '"+visitDay[0]+"'";
        if(visitDay.length == 2){
            daySQL = "uv.day >= '"+visitDay[0]+"' and uv.day<='"+visitDay[1]+"'";
        }

        regDay = regDay.replaceAll("-","");
        String sql =  "select count(distinct lower(uv.orig_id)) from user_visit  uv join user_register_time ur on lower(uv.orig_id) = lower(ur.orig_id) " +
                    "and uv.pid = ur.pid where uv.pid = '"+project+"' and ur.min_reg_time = '"+regDay+"' and " + daySQL;

        LOGGER.debug(sql);
        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        long count = 0;
        if (res.next()) {
            count = res.getLong(1);
        }
        return count;
    }

    //GROUP,v9,2015-01-07,2015-01-07,visit.*,TOTAL_USER,VF-ALL-0-0,USER_PROPERTIES,geoip
    //COMMON,v9,2014-12-02,2015-01-01,visit.*,{"geoip":"us"},VF-ALL-0-0,PERIOD
    public Map<String,Long> countActiveUserByAttr(String project, String attribute, String[] day) throws Exception {

        Map<String,Long> result = new HashMap<String, Long>();

        String daySQL = "uv.day = '"+day[0]+"'";
        if(day.length == 2){
            daySQL = "uv.day > '"+day[0]+"' and uv.day<='"+day[1]+"'";
        }

        String sql =  "select COALESCE(ua.val,'XA-NA'),count(distinct lower(uv.orig_id)) from user_visit uv left outer join user_attribute ua on lower(uv.orig_id) = lower(ua.orig_id) " +
                "and uv.pid = ua.pid and ua.attr='" + attribute + "' where "+ daySQL +" and uv.pid = '" + project + "'" + " group by COALESCE(ua.val,'XA-NA')  ";

        LOGGER.debug(sql);
        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);

        while (res.next()) {
            String geoip = res.getString(1);
            Long count = res.getLong(2);
            result.put(geoip,count);
        }
        return result;
    }

    //COMMON,age,2014-08-25,2014-08-25,visit.*,{"geoip":"ua","register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,PERIOD
    //COMMON,age,2014-08-26,2014-08-26,visit.*,{"geoip":"ua","register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,PERIOD

    //GROUP,age,2014-08-25,2014-08-25,visit.*,{"register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,USER_PROPERTIES,geoip
    //GROUP,age,2014-08-26,2014-08-26,visit.*,{"register_time":{"$gte":"2014-08-25","$lte":"2014-08-25"}},VF-ALL-0-0,USER_PROPERTIES,geoip
    public Map<String,Long> countNewUserByAttr(String project, String attribute, String... regDay) throws Exception {

        Map<String,Long> result = new HashMap<String, Long>();

        String daySQL = "ur.min_reg_time = '"+regDay[0].replaceAll("-","")+"'";
        if(regDay.length == 2){
            daySQL = "ur.min_reg_time >= '" + regDay[0].replaceAll("-","") + "' and ur.min_reg_time<='" + regDay[1].replaceAll("-","") + "'";
        }

        String sql =  "select COALESCE(ua.val,'XA-NA'),count(distinct lower(ur.orig_id)) from user_register_time ur left outer join user_attribute ua on lower(ur.orig_id) = lower(ua.orig_id) " +
                    "and ur.pid = ua.pid and ua.attr='"+attribute+"' where ur.pid = '"+project+"' and "+ daySQL +" group by COALESCE(ua.val,'XA-NA')  ";

        LOGGER.debug(sql);
        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);

        while (res.next()) {
            String geoip = res.getString(1);
            Long count = res.getLong(2);
            result.put(geoip,count);
        }
        return result;
    }

    public Map<String,Long> countRetentionUserByAttr(String project, String attribute, String regDay, String[] visitDay) throws Exception {

        Map<String,Long> result = new HashMap<String, Long>();

        String daySQL = "uv.day = '"+visitDay[0]+"'";
        if(visitDay.length == 2){
            daySQL = "uv.day >= '"+visitDay[0]+"' and uv.day<='"+visitDay[1]+"'";
        }

        regDay = regDay.replaceAll("-","");
        String sql =  "select COALESCE(ua.val,'XA-NA'), count(distinct lower(uv.orig_id)) from user_visit  uv join user_register_time ur on lower(ur.orig_id) = lower(uv.orig_id) and uv.pid = ur.pid " +
                        " left outer join user_attribute ua on lower(ua.orig_id) = lower(ur.orig_id)  and ur.pid = ua.pid and  ua.attr='"+attribute+"'" +
                        " where uv.pid = '"+project+"' and ur.min_reg_time = '"+regDay+"' and " + daySQL + " group by COALESCE(ua.val,'XA-NA')  ";

        LOGGER.debug(sql);
        Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            result.put(res.getString(1),res.getLong(2));
        }
        return result;
    }

}
