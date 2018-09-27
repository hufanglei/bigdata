package bigdata0912.combiner;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//                                                                     v4：平均工资
public class AvgSalaryReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text k3, Iterable<IntWritable> v3,Context context) throws IOException, InterruptedException {
        int total = 0;
        int count = 0;

        for(IntWritable salary:v3){
            //工资求和
            total = total + salary.get();
            //人数加一
            count ++;
        }

        //输出
        context.write(new Text("The avg salary is :"), new DoubleWritable(total/count));
    }

}