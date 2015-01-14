import org.junit.Test;

/**
 * Created by Administrator on 14-7-29.
 */
public class Test1 {

    public static void main(String[] args) {
        Test1 t = new Test1();
        //sc      *       google  *       3       cor     7       23      13560
        //sc      *       *       ru      6       -       1       1       522
        //hppp    cor     google  *       6       -       1       1       800
        //*      *       *       ru      6       -       1       1       522
        // *       exp     *       *       2       hppp
        //*       *       *       d       5
        String ev3 = "*";
        String ev4 = "*";
        String ev5 = "*";
        String nation = "d";

        String grp = "6";
        String date = "2014-12-19";

        String key = t.generateCacheKey2(date, date, ev3, ev4, ev5, nation, grp);
        System.out.println(key);
    }

    public String generateCacheKey(String scanDay, String ev3, String ev4, String ev5, String nation, String grp) {
        String cacheKey = "";
        String commHead = "COMMON,internet-1," + scanDay + "," + scanDay + ",pay.search2.";
        String totalUser = ",TOTAL_USER";
        String commTail = ",VF-ALL-0-0,PERIOD";
        String groupHead = "GROUP,internet-1," + scanDay + "," + scanDay + ",pay.search2.";
        String evtGroupTail = ",VF-ALL-0-0,EVENT,";
        String natGroupTail = ",VF-ALL-0-0,USER_PROPERTIES,nation";

        String events = "";
        StringBuffer sb = new StringBuffer();
        sb.append(ev3);
        if(ev5.equals("*")) {
            if(ev4.equals("*")) {
                if(ev3.equals("*")) {
                    events = sb.toString();
                } else {
                    sb.append(".*");
                    events = sb.toString();
                }
            } else {
                sb.append(".").append(ev4).append(".*");
                events = sb.toString();
            }
        } else {
            sb.append(".").append(ev4).append(".").append(ev5).append(".*");
            events = sb.toString();
        }

        sb = new StringBuffer();
        if(nation.equals("*")) {
            if(grp.equals("6")) {
                sb.append(commHead).append(events).append(totalUser).append(commTail);
            } else {
                sb.append(groupHead).append(events).append(totalUser);
                if(grp.equals("5")) {
                    sb.append(natGroupTail);
                } else {
                    sb.append(evtGroupTail).append(grp);
                }
            }
        } else {
            if(grp.equals("6")) {
                sb.append(commHead).append(events).append(",{\"nation\":\"").append(nation).append("\"}").append(commTail);
            } else {
                sb.append(groupHead).append(events).append(",{\"nation\":\"").append(nation).append("\"}").append(evtGroupTail).append(grp);
            }
        }
        cacheKey = sb.toString();

        return cacheKey;
    }

    public String generateCacheKey2(String start, String end, String ev3, String ev4, String ev5, String nation, String grp) {
        System.out.println(start + "\t" + end + "\t" + ev3 + "\t" + ev4 + "\t" + ev5 + "\t" + nation + "\t" + grp);
        String cacheKey = "";
        String commHead = "COMMON,internet-1," + start + "," + end + ",pay.search2.";
        String totalUser = ",TOTAL_USER";
        String commTail = ",VF-ALL-0-0,PERIOD";
        String groupHead = "GROUP,internet-1," + start + "," + end + ",pay.search2.";
        String evtGroupTail = ",VF-ALL-0-0,EVENT,";
        String natGroupTail = ",VF-ALL-0-0,USER_PROPERTIES,nation";

        String events = "";
        StringBuffer sb = new StringBuffer();
        sb.append(ev3);
        if(ev5.equals("*")) {
            if(ev4.equals("*")) {
                if(ev3.equals("*")) {
                    events = sb.toString();
                } else {
                    sb.append(".*");
                    events = sb.toString();
                }
            } else {
                sb.append(".").append(ev4).append(".*");
                events = sb.toString();
            }
        } else {
            sb.append(".").append(ev4).append(".").append(ev5).append(".*");
            events = sb.toString();
        }

        sb = new StringBuffer();
        if(nation.equals("*")) {
            if(grp.equals("6") && start.equals(end)) {
                sb.append(commHead).append(events).append(totalUser).append(commTail);
            } else {
                sb.append(groupHead).append(events).append(totalUser);
                if(grp.equals("5")) {
                    sb.append(natGroupTail);
                } else {
                    sb.append(evtGroupTail).append(grp);
                }
            }
        } else {
            if(grp.equals("6") && start.equals(end)) {
                sb.append(commHead).append(events).append(",{\"nation\":\"").append(nation).append("\"}").append(commTail);
            } else {
                sb.append(groupHead).append(events).append(",{\"nation\":\"").append(nation).append("\"}").append(evtGroupTail).append(grp);
            }
        }
        cacheKey = sb.toString();

        return cacheKey;
    }
}
