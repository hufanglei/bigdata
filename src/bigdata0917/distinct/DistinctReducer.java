package bigdata0917.distinct;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistinctReducer  extends Reducer<Text, NullWritable, Text, NullWritable>{

    @Override
    protected void reduce(Text k3, Iterable<NullWritable> v3,Context context) throws IOException, InterruptedException {
        // 直接把k3输出即可
        context.write(k3, NullWritable.get());
    }

}
