package com.xingcloud.nba.main;

import com.xingcloud.nba.service.BAService;
import com.xingcloud.nba.service.ScanHBaseUID3;
import com.xingcloud.nba.utils.Constant;
import com.xingcloud.nba.utils.DateManager;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghaixing on 14-12-26.
 */
public class PaySearchWork {
    private String mode;
    private String date;

    Options buildOptions() {
        Options options = new Options();
        Option option;

        option = OptionBuilder.withDescription("Print this help message").withLongOpt("help").create("?");
        options.addOption(option);

        option = OptionBuilder.hasArg().withDescription("Select mode to run").withLongOpt("mode").create("mode");
        options.addOption(option);

        option = OptionBuilder.hasArg().withDescription("Input a specific date").withLongOpt("date").create();
        options.addOption(option);

        return options;
    }

    public boolean parseArgs(String[] args) throws ParseException {
        Options options = buildOptions();
        CommandLine cli = new GnuParser().parse(options, args);

        if (!cli.hasOption("mode")) {
            return false;
        } else {
            mode = cli.getOptionValue("mode");
        }

        if (!cli.hasOption("date")) {
            return false;
        } else {
            date = cli.getOptionValue("date");
        }

        return true;
    }

    public static void main(String[] args) throws Exception {
        BAService service = new BAService();
        PaySearchWork psw = new PaySearchWork();
        boolean isOk = psw.parseArgs(args);
        if (!isOk) {
            return;
        }

        Map<String, List<String>> specialProjectList = getSpecialProjectList();
        String day = psw.getDate();
        if (psw.getMode().equals("searchday")) {
            paySearchJob(service, specialProjectList, day, false, Constant.DAY);
        } else if (psw.getMode().equals("searchweek")) {
            paySearchJob(service, specialProjectList, day, false, Constant.WEEK);
        } else if (psw.getMode().equals("generate")) {
            paySearchJob(service, specialProjectList, day, true, null);
        }

    }

    public static void paySearchJob(BAService service,Map<String,List<String>> projects, String day, boolean query, String range) throws Exception {
        Map<String, Map<String,Number[]>> allResult = new HashMap<String, Map<String,Number[]>>();
        //internet-1的搜索相关
        ScanHBaseUID3 shu = new ScanHBaseUID3();
        List<String> projs = projects.get(Constant.INTERNET1);
        projs.add("newtab2");
        if(query) {
            String scanDay = DateManager.getDaysBefore(day, 1);
            String date = scanDay.replace("-","");
            shu.getHBaseUID(date, Constant.EVENT, projs);
        } else {
            if (Constant.DAY.equals(range)) {
                allResult.putAll(shu.getDayResults(day));
            } else if(Constant.WEEK.equals(range)) {
                allResult.putAll(shu.getWeekResults(day));
            }
            service.storeToRedis(allResult);
            service.cleanup();
        }
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

    public String getMode() { return mode; }

    public String getDate() {
        return date;
    }
}
