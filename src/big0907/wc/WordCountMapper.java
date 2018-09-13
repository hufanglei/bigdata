package big0907.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {
        /*
         * context: map的上下文
         * 上文：HDFS
         * 下文：Reducer
         */
        //得到数据   I love Beijing
        String data = value1.toString();

        //分词
        String[] words = data.split(" ");

        //输出    k2     v2
        for(String w:words){
            //             k2            v2
            context.write(new Text(w), new IntWritable(1));
        }
    }
}
