package bigdata0912.combiner;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgSalaryMain {

    public static void main(String[] args) throws Exception {
        //1、创建任务、指定任务的入口
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(AvgSalaryMain.class);

        //2、指定任务的map和map输出的数据类型
        job.setMapperClass(AvgSalaryMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //加入Combiner
        job.setCombinerClass(AvgSalaryReducer.class);

        //3、指定任务的reducer和reducer输出的类型
        job.setReducerClass(AvgSalaryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //4、指定任务输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //5、执行任务
        job.waitForCompletion(true);

    }

}
