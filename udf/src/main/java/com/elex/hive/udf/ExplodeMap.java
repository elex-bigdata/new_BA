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
        inputOI = args[0];
        System.out.println("---------------------------" + inputOI.getCategory());
        /*ListObjectInspector listOI = (ListObjectInspector)inputOI;
        List<?> list = listOI.getList(args[0]);
        for(Object s : list) {
            System.out.print(s + "---------------");
        }
        System.out.println();*/

        fieldNames.add("ev3");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("ev4");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("ev5");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("nation");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {

        if(inputOI.getCategory() == ObjectInspector.Category.LIST) {
            ListObjectInspector listOI = (ListObjectInspector)inputOI;
            List<?> list = listOI.getList(args[0]);
//            List<String> list = (List<String>)args[0];

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

    public static List<String[]> transRows(List<?> list) {
        int len = list.size();
        String[] result = null;
        if(4 == len) {
            result = new String[len];
            for(int i = 0; i < len; i++) {
                result[i] = (String)list.get(i);
            }
        }

        List<String[]> resList = new ArrayList<String[]>();

        for(int i = 0; i < len; i++) {  //one item
            String[] row = new String[len];
            row[i] = result[i];
            for(int j = 0; j < len; j++) {
                if(i != j) {
                    row[j] = "XA-NA";
                }
            }
            resList.add(row);
        }

        for(int i = 0; i < len-1; i++) {
            String[] row = new String[len];
            row[i] = result[i];
            for(int j = i+1; j < len; j++) {
                row[j] = result[j];
                for(int k = 0; k < len; k++) {
                    if(k != i && k != j) {
                        row[k] = "XA-NA";
                    }
                }
                String[] t = new String[len];
                for(int x = 0; x < row.length; x++) {
                    t[x] = row[x];
                }

                resList.add(t);
            }
        }

        for(int i = 0; i < len-2; i++) {
            String[] row = new String[len];
            row[i] = result[i];
            for(int j = i+1; j < len-1; j++) {
                row[j] = result[j];
                for(int k = j+1; k < len; k++) {
                    row[k] = result[k];
                    for(int h = 0; h < len; h++) {
                        if(h != i && h != j && h != k) {
                            row[h] = "XA-NA";
                        }
                    }
                    String[] t = new String[len];
                    for(int x = 0; x < row.length; x++) {
                        t[x] = row[x];
                    }

                    resList.add(t);
                }
            }
        }

        String[] r = new String[len];
        for(int i = 0; i < len; i++) {
            r[i] = result[i];
        }
        resList.add(r);

        System.out.println(resList.size());

        return resList;
    }

    @Override
    public void close() throws HiveException {

    }

    public static void main(String[] args) {
        List<String> t = new ArrayList<String>();
        t.add("a");
        t.add("b");
        t.add("c");
        t.add("d");

        List<String[]> rl = transRows(t);
        for(String[] row : rl) {
            for(int i = 0; i < row.length; i++) {
                System.out.print(row[i] + "   ");
            }
            System.out.println();
        }
    }
}
