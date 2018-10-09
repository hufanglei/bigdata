package demo.mr;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//这时候处理的就是HBase表中的一条数据                                                          k2      v2
// <k1  v1>代表输入，现在输入是：表中的一条记录
public class WordCountMapper extends TableMapper<Text, IntWritable> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value,Context context)
            throws IOException, InterruptedException {
        // 获取数据： I love Beijing
        String data = Bytes.toString(value.getValue(Bytes.toBytes("content"), Bytes.toBytes("info")));

        //分词
        String[] words = data.split(" ");

        for(String w:words){
            context.write(new Text(w), new IntWritable(1));
        }
    }
}
