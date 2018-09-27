package bigdata0912.partition;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyPartitionerMain {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(MyPartitionerMain.class);

        job.setMapperClass(MyPartitionerMapper.class);
        job.setMapOutputKeyClass(IntWritable.class); //k2 是部门号
        job.setMapOutputValueClass(Emp.class);  // v2输出就是员工对象

        //加入分区规则
        job.setPartitionerClass(MyPartitioner.class);
        //指定分区的个数
        job.setNumReduceTasks(3);

        job.setReducerClass(MyPartitionerReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Emp.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
