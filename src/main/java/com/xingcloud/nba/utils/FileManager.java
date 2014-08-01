package com.xingcloud.nba.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by wanghaixing on 14-8-1.
 */
public class FileManager {
    /**
     * delete the file in the local disk or hdfs
     *
     * @param filePath
     *            the file tou want to delete
     * @return weather the file can be delete
     * @throws java.io.IOException
     *             the file could not delete
     */
    public static Boolean deleteFile(String filePath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(filePath), conf);
        Path path = new Path(filePath);
        if (fs.exists(path)) {
            return fs.delete(path, true);
        }

        return false;
    }

    public static Boolean makeDir(String filePath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(filePath), conf);
        return fs.mkdirs(new Path(filePath));
    }

}
