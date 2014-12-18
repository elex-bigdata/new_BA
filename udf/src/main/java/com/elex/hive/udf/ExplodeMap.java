package com.elex.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wanghaixing on 14-12-15.
 */
public class ExplodeMap extends GenericUDTF {
    private ObjectInspector inputOI = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        /*if (args.length != 1) {
            throw new UDFArgumentLengthException("ExplodeMap takes only one argument");
        }

        if (args[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException("ExplodeMap takes list<string> as a parameter");
        }*/

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        inputOI = (ListObjectInspector)args[0];
        System.out.println("---------------------------" + inputOI.getCategory());

        fieldNames.add("ev3");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("ev4");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("ev5");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("nation");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("grp");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("key");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        System.out.println("-------------aaaa--------------");
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        System.out.println("-------------bbbb--------------");
        if(inputOI.getCategory() == ObjectInspector.Category.LIST) {
            System.out.println("-------------ccc--------------");
            ListObjectInspector listOI = (ListObjectInspector)inputOI;
            System.out.println("-------------dd--------------");
            List list = listOI.getList(args[0]);
//            List<String> list = (List<String>)args[0];
            System.out.println("-------------ee--------------");
            if (list == null) {
                return;
            } else {
                List<String[]> results = transRows(list);
                for(String[] row : results) {
                    forward(row);
                }
            }
        }
    }

    public void setKey(String[] row, String evts[]) {
        int len = row.length;
        String grp = row[len-2];
        switch(Integer.valueOf(grp)) {
            case 2:
                row[len-1] = evts[0];
                break;
            case 3:
                row[len-1] = evts[1];
                break;
            case 4:
                row[len-1] = evts[2];
                break;
            case 5:
                row[len-1] = evts[3];
                break;
            case 6:
                row[len-1] = "-";
                break;
        }
    }

    public String[] generateAll(int len, int resLen) {
        String[] row = new String[resLen];
        for(int i = 0; i < len; i++) {  //0 items
            row[i] = "*";
        }
        row[len] = "6";
        return row;
    }

    public List<String[]> transRows(List list) {
        List<String[]> resList = new ArrayList<String[]>();
        int len = list.size();
        int resLen = len + 2;

        String[] result = new String[resLen];
        for(int i = 0; i < len; i++) {
            result[i] = list.get(i).toString();
        }

        boolean flag = true;    //判断是否有缺省事件
        for(int i = 0; i < len; i++) {
            if(result[i].equals("*")) {
                flag = false;
                break;
            }
        }

        //细分项：2--第三层, 3--第四层, 4--第五层, 5--nation, 6--无细分common
        String[] group = {"2", "3", "4", "5", "6"};
        if(flag) {  //4个值都不为空
            for(int i = 0; i < len; i++) {  //1 item
                String[] row = new String[resLen];
                row[i] = result[i];
                for(int j = 0; j < len; j++) {
                    if(i != j) {
                        row[j] = "*";
                    }
                }
                for(int k = 0; k <= len; k++) {
                    if(k != i) {
                        row[len] = group[k];
                        setKey(row, result);
                        String[] t = new String[resLen];
                        for(int x = 0; x < resLen; x++) {
                            t[x] = row[x];
                        }
                        resList.add(t);
                    }
                }
            }

            for(int i = 0; i < len-1; i++) {    //2 items
                String[] row = new String[resLen];
                row[i] = result[i];
                for(int j = i+1; j < len; j++) {
                    row[j] = result[j];
                    for(int k = 0; k < len; k++) {
                        if(k != i && k != j) {
                            row[k] = "*";
                        }
                    }
                    for(int m = 0; m <= len; m++) {
                        if(m != i && m != j) {
                            row[len] = group[m];
                            setKey(row, result);
                            String[] t = new String[resLen];
                            for(int x = 0; x < resLen; x++) {
                                t[x] = row[x];
                            }
                            resList.add(t);
                        }
                    }
                }
            }

            for(int i = 0; i < len-2; i++) {    //3 items
                String[] row = new String[resLen];
                row[i] = result[i];
                for(int j = i+1; j < len-1; j++) {
                    row[j] = result[j];
                    for(int k = j+1; k < len; k++) {
                        row[k] = result[k];
                        for(int h = 0; h < len; h++) {
                            if(h != i && h != j && h != k) {
                                row[h] = "*";
                            }
                        }
                        for(int m = 0; m <= len; m++) {
                            if(m != i && m != j && m != k) {
                                row[len] = group[m];
                                setKey(row, result);
                                String[] t = new String[resLen];
                                for(int x = 0; x < resLen; x++) {
                                    t[x] = row[x];
                                }
                                resList.add(t);
                            }
                        }
                    }
                }
            }

            String[] row = new String[resLen];
            for(int i = 0; i < len; i++) {  //4 items
                row[i] = result[i];
            }
            row[len] = "6";
            setKey(row, result);
            resList.add(row);

            String[] rowAll = generateAll(len, resLen);
            setKey(rowAll, result);
            resList.add(rowAll);

        } else {    //有某一项为空
            if(result[0].equals("*")) { //没有ev3
                String[] row = new String[resLen];
                for(int x = 0; x < len-1; x++) {
                    row[x] = "*";
                }
                row[len-1] = result[len-1]; //nation
                for(int m = 0; m <= len; m++) {
                    row[len] = group[m];
                    setKey(row, result);
                    String[] t = new String[resLen];
                    for(int x = 0; x < resLen; x++) {
                        t[x] = row[x];
                    }
                    resList.add(t);
                }

                if(!result[3].equals("*")) {
                    String[] rowAll = generateAll(len, resLen);
                    setKey(rowAll, result);
                    resList.add(rowAll);
                }

            } else if(result[1].equals("*")) {  //没有ev4
                for(int i = 0; i < len; i++) {  //1 item
                    if(i != 1 && i != 2) {
                        String[] row = new String[resLen];
                        row[i] = result[i];
                        for(int j = 0; j < len; j++) {
                            if(i != j) {
                                row[j] = "*";
                            }
                        }
                        for(int k = 0; k <= len; k++) {
                            if(k != i) {
                                row[len] = group[k];
                                setKey(row, result);
                                String[] t = new String[resLen];
                                for(int x = 0; x < resLen; x++) {
                                    t[x] = row[x];
                                }
                                resList.add(t);
                            }
                        }
                    }
                }

                if(!result[3].equals("*")) {
                    String[] row = new String[resLen];
                    row[0] = result[0];
                    row[3] = result[3];
                    for(int j = 1; j < len-1; j++) {
                        row[j] = "*";
                    }
                    for(int k = 1; k <= len; k++) {
                        if(k != 3) {
                            row[len] = group[k];
                            setKey(row, result);
                            String[] t = new String[resLen];
                            for(int x = 0; x < resLen; x++) {
                                t[x] = row[x];
                            }
                            resList.add(t);
                        }
                    }

                    String[] rowAll = generateAll(len, resLen);
                    setKey(rowAll, result);
                    resList.add(rowAll);
                }

            } else if(result[2].equals("*")) {  //没有ev5

                for(int i = 0; i < len; i++) {  //1 item
                    if(i != 2) {
                        String[] row = new String[resLen];
                        row[i] = result[i];
                        for(int j = 0; j < len; j++) {
                            if(i != j) {
                                row[j] = "*";
                            }
                        }
                        for(int k = 0; k <= len; k++) {
                            if(k != i) {
                                row[len] = group[k];
                                setKey(row, result);
                                String[] t = new String[resLen];
                                for(int x = 0; x < resLen; x++) {
                                    t[x] = row[x];
                                }
                                resList.add(t);
                            }
                        }
                    }
                }

                for(int i = 0; i < len-2; i++) {    //2 items
                    String[] row = new String[resLen];
                    row[i] = result[i];
                    for(int j = i+1; j < len; j++) {
                        if(j != 2) {
                            if(!result[j].equals("*")) {
                                row[j] = result[j];
                                for(int k = 0; k < len; k++) {
                                    if(k != i && k != j) {
                                        row[k] = "*";
                                    }
                                }
                                for(int m = 0; m <= len; m++) {
                                    if(m != i && m != j) {
                                        row[len] = group[m];
                                        setKey(row, result);
                                        String[] t = new String[resLen];
                                        for(int x = 0; x < resLen; x++) {
                                            t[x] = row[x];
                                        }
                                        resList.add(t);
                                    }
                                }
                            }
                        }

                    }
                }

                if(!result[3].equals("*")) {
                    String[] row = new String[resLen];
                    row[0] = result[0];
                    row[1] = result[1];
                    row[3] = result[3];
                    for(int j = 2; j < len-1; j++) {
                        row[j] = "*";
                    }
                    for(int k = 2; k <= len; k++) {
                        if(k != 3) {
                            row[len] = group[k];
                            setKey(row, result);
                            String[] t = new String[resLen];
                            for(int x = 0; x < resLen; x++) {
                                t[x] = row[x];
                            }
                            resList.add(t);
                        }
                    }

                    String[] rowAll = generateAll(len, resLen);
                    setKey(rowAll, result);
                    resList.add(rowAll);
                }
            }
        }

        System.out.println(resList.size());

        return resList;
    }

    @Override
    public void close() throws HiveException {

    }

    public static void main(String[] args) {
        ExplodeMap em = new ExplodeMap();
        List<String> t = new ArrayList<String>();
        t.add("ds");
        t.add("b");
        t.add("*");
        t.add("*");

        List<String[]> rl = em.transRows(t);
        for(String[] row : rl) {
            for(int i = 0; i < row.length; i++) {
                System.out.print(row[i] + "   ");
            }
            System.out.println();
        }
    }
}
