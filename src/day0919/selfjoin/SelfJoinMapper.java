package day0919.selfjoin;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class SelfJoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        // 数据: 7566,JONES,MANAGER,7839,1981/4/2,2975,0,20
        String data = v1.toString();
        //分词
        String[] words = data.split(",");
        //输出数据
        //1.作为老板
        context.write(new IntWritable(Integer.parseInt(words[0])), new Text("*"+words[1]));
        //作为员工表
        context.write(new IntWritable(Integer.parseInt(words[3])), new Text(words[1]));
        /*
         * 注意一个问题：如果数据存在非法数据，一定处理一下（数据清洗）
         * 如果产生例外，一定捕获
         */

    }


}
