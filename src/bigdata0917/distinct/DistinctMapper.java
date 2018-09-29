package bigdata0917.distinct;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//                                                             k2 职位job
public class DistinctMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key1, Text value1, Context context)
            throws IOException, InterruptedException {
        //数据：7499,ALLEN,SALESMAN,7698,1981/2/20,1600,300,30
        String data = value1.toString();

        //分词
        String[] words = data.split(",");

        //输出：把职位job作为key2
        context.write(new Text(words[2]+" "+ words[3]), NullWritable.get());
    }
}
