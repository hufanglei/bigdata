package day0919.revertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class RevertedIndexMain {

    public static void main(String[] args) throws Exception {
        //1、创建一个任务
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(RevertedIndexMain.class); //任务的入口

        //2、指定任务的map和map输出的数据类型
        job.setMapperClass(RevertedIndexMapper.class);
        job.setMapOutputKeyClass(Text.class);  //k2的数据类型
        job.setMapOutputValueClass(Text.class);  //v2的类型

        //指定任务的Combiner
        job.setCombinerClass(RevertedIndexCombiner.class);

        //3、指定任务的reduce和reduce的输出数据的类型
        job.setReducerClass(RevertedIndexReducer.class);
        job.setOutputKeyClass(Text.class); //k4的类型
        job.setOutputValueClass(Text.class); //v4的类型

        //4、指定任务的输入路径、任务的输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //5、执行任务
        job.waitForCompletion(true);
    }
}
