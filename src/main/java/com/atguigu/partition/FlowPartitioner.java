package com.atguigu.partition;

import com.atguigu.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner extends Partitioner<Text, FlowBean> {

    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        int i = 4;
        switch (text.toString().substring(0, 3)) {
            case "136":
                i = 0;
                break;
            case "137":
                i = 1;
                break;
            case "138":
                i = 2;
                break;
            case "139":
                i = 3;
        }
        return i;
    }
}
