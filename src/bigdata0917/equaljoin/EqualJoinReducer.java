package bigdata0917.equaljoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class EqualJoinReducer  extends Reducer<IntWritable, Text, Text, Text> {

    @Override
    protected void reduce(IntWritable k3, Iterable<Text> v3, Context context)
            throws IOException, InterruptedException {
        // 处理v3：可能是部门名称、也可能是员工的姓名
        String dname = "";
        String empNameList = "";

        for(Text value:v3){
            String str = value.toString();
            //判断是否存在*
            int index = str.indexOf("*");
            if(index >= 0){
                //代表是部门的名称
                dname = str.substring(1);
            }else{
                //代表是员工的名称
                empNameList = str + ";" + empNameList;
            }
        }

        //输出
        context.write(new Text(dname), new Text(empNameList));
    }

}
