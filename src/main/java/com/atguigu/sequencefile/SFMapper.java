package com.atguigu.sequencefile;

import com.atguigu.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SFMapper extends Mapper<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void map(Text key, FlowBean value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
