package com.atguigu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//
//import java.io.IOException;
//public class WcMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
//    private Text word = new Text();
//    private IntWritable one = new IntWritable(1);
//    @Override
//    protected void map(LongWritable key, Text value, Context context) throws IOException,
// InterruptedException {
//        String line = value.toString();
//        String[] words = line.split(" ");
//        for (String word : words) {
//            this.word.set(word);
//            context.write(this.word, one);
//        }
//    }
public class WcMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            this.word.set(word);
            context.write(this.word,one);
        }
    }
}
