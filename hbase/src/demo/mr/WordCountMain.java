package demo.mr;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class WordCountMain {

    public static void main(String[] args) throws Exception {
        //指定Zookeeper地址
        //指定的配置信息: ZooKeeper
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.157.111");

        //创建一个任务
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountMain.class);

        //定义一个扫描器只读取 content:info这个列的数据
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("content"), Bytes.toBytes("info"));

        //指定mapper
        TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("word"),      //输入的表
                scan,    //定义一个扫描器，过滤我们想要处理的数据
                WordCountMapper.class,
                Text.class,
                IntWritable.class,
                job);

        //指定reducer
        TableMapReduceUtil.initTableReducerJob("stat", WordCountReducer.class, job);

        //执行任务
        job.waitForCompletion(true);
    }
}








