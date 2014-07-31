package com.xingcloud.nba.mr.inputformat;

import com.xingcloud.nba.utils.Constant;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * Created by wanghaixing on 14-7-31.
 */
public class CombineFileLineRecordReader extends RecordReader<LongWritable, Text> {

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private LongWritable key = null;
    private Text value = null;
    private TaskAttemptContext context;
    private CombineFileSplit split;
    private int idx;

    private static Log LOG = LogFactory.getLog(CombineFileLineRecordReader.class);

    public CombineFileLineRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer idx) {
        this.split = split;
        this.context = context;
        this.idx = idx;
    }

    @Override
    // 参照自LineRecordReader
    public void initialize(InputSplit combineFileSplit, TaskAttemptContext context) throws IOException {
        CombineFileSplit split = (CombineFileSplit) combineFileSplit;

        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        start = split.getOffset(idx);
        end = start + split.getLength(idx);
        final Path file = split.getPath(idx);
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);

        String strFilePath = split.getPath(idx).toString();
        key = new LongWritable();
        if(strFilePath.contains("/user/hadoop/stream_log/pid/")) {
            key.set(Constant.KEY_FOR_EVENT_LOG);
        } else if(strFilePath.contains("/user/hadoop/mysqlidmap/")) {
            key.set(Constant.KEY_FOR_IDMAP);
        } else {
            key.set(Constant.KEY_USELESS);
        }
        //LOG.info("add file : " + strFilePath) ;
        FSDataInputStream fileIn = fs.open(split.getPath(idx));
        boolean skipFirstLine = false;
        if (codec != null)
        {
            in = new LineReader(codec.createInputStream(fileIn), job);
            end = Long.MAX_VALUE;
        }
        else
        {
            if (start != 0)
            {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            in = new LineReader(fileIn, job);
        }
        if (skipFirstLine)
        { // skip first line and re-establish "start".
            start += in.readLine(new Text(), 0,
                    (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (value == null) {
            value = new Text();
        }
        int newSize = 0;
        while (pos < end) {
            newSize = in.readLine(value, maxLineLength, Math.max((int)Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));

            if (newSize == 0) {
                break;
            }
            pos += newSize;
            if (newSize < maxLineLength) {
                break;
            }
        }
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

}
