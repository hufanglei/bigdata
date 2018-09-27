package bigdata0912.partition;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

//自定义的分区规则：按照部门号进行分区            k2 部门号      v2  员工对象
public class MyPartitioner extends Partitioner<IntWritable, Emp> {

    /**
     * numTask：分区的个数
     */
    @Override
    public int getPartition(IntWritable k2, Emp v2, int numTask) {
        // 建立我们的分区规则
        //得到该员工的部门号
        int deptno = v2.getDeptno();

        if(deptno == 10){
            //放入一号分区
            return 1%numTask;
        }else if(deptno == 20){
            //放入二号分区
            return 2%numTask;
        }else{
            //30号部门，放入零号分区
            return 3%numTask;
        }
    }

}

