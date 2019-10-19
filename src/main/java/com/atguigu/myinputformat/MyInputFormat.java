package com.atguigu.myinputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

//public class MyInputFormat extends FileInputFormat<Text, BytesWritable> {
////
////    public RecordReader<Text,BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
////        return new MyRecordReader();
////    }
////}
public class MyInputFormat extends FileInputFormat<Text,BytesWritable>{

    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new MyRecordReader();
    }
}
