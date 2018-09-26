package big0907.groupby;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.BiConsumer;

public class GroupReducer extends Reducer<IntWritable, IntWritable, IntWritable, DoubleWritable> {

    @Override
    protected void reduce(IntWritable k3,Iterable<IntWritable> v3, Context context) throws IOException, InterruptedException {
        int total = 0;
        int count =0;
        for (IntWritable item: v3){
            count = count +1;
            total = total + item.get();
        }
        double decimal = total/count;
        context.write(k3, new DoubleWritable(decimal));
    }

}
