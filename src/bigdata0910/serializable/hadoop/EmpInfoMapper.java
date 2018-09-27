package bigdata0910.serializable.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EmpInfoMapper extends Mapper<LongWritable, Text, IntWritable, Emp> {

    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        // 数据：7654,MARTIN,SALESMAN,7698,1981/9/28,1250,1400,30
        String data = v1.toString();
        String[] words = data.split(",");
        //生成员工对象
        Emp emp = new Emp();
        emp.setEmpno(Integer.parseInt(words[0]));
        emp.setEname(words[1]);
        emp.setJob(words[2]);
        emp.setMgr(Integer.parseInt(words[3]));
        emp.setHiredate(words[4]);
        emp.setSal(Integer.parseInt(words[5]));
        emp.setComm(Integer.parseInt(words[6]));
        emp.setDeptno(Integer.parseInt(words[7]));
        //输出员工对象  k2:员工号
        context.write(new IntWritable(emp.getEmpno()), emp);
    }
}
