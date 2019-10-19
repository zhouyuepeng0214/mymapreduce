package com.atguigu.myinputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<Text, BytesWritable> {

//    private boolean isRead = false;
//    private Text key = new Text();
//    private BytesWritable value = new BytesWritable();
//
//    private FSDataInputStream fis;
//    private FileSplit fs;

    private boolean isRead = false;
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    private FileSplit fs;
    private FSDataInputStream fis;

    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        //开流
//        fs = (FileSplit) split;
//        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
//        fis = fileSystem.open(fs.getPath());
        fs = (FileSplit) split;
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        fis = fileSystem.open(fs.getPath());

    }

    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (!isRead) {
//            String path = fs.getPath().toString();
//            key.set(path);
//            byte[] buf = new byte[(int) fs.getLength()];
//            int read = fis.read(buf);
//            value.set(buf, 0, read);
//            isRead = true;
//            return true;
            String path = fs.getPath().toString();
            key.set(path);
            byte[] buf = new byte[(int) fs.getLength()];
            int read = fis.read(buf);
            value.set(buf, 0, read);
            isRead = true;
            return true;
        } else {
            return false;
        }
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return isRead ? 1 : 0;
    }

    public void close() throws IOException {
        IOUtils.closeStream(fis);
    }
}
