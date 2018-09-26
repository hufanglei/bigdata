package big0907.groupby;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupMapper extends Mapper<LongWritable, Text, IntWritable,IntWritable> {


    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        String[] strings = v1.toString().split(",");
        context.write(new IntWritable(Integer.parseInt(strings[7])),new IntWritable(Integer.parseInt(strings[5])));
    }
}
