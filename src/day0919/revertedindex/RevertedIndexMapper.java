package day0919.revertedindex;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class RevertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    protected void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {
        //数据：/indexdata/data01.txt
        //得到对应文件名
        String path = ((FileSplit)context.getInputSplit()).getPath().toString();
        //解析出文件名
        //得到最后一个斜线的位置
        int index = path.lastIndexOf("/");
        String fileName = path.substring(index+1);
        //数据：I love Beijing and love Shanghai
        String data = value1.toString();
        String[] words = data.split(" ");
        //输出 live:data01.txt    1
        for(String word:words){
            context.write(new Text(word+":"+fileName), new Text("1"));
        }
    }
}
