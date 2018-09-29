package day0919.revertedindex;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RevertedIndexCombiner  extends Reducer<Text, Text, Text, Text> {
    // k2                  v2                k21     v21
    // love:data01.txt    (1,1)      ==>     love:   data01.txt:2
    // love:data02.txt       （1）       ==》   love  ：   data02.txt:1
    @Override
    protected void reduce(Text k21, Iterable<Text> v21, Context context) throws IOException, InterruptedException {
        int total = 0;
        for (Text v:v21){
            total = total +  Integer.parseInt(v.toString());
        }
        //k21是：love:data01.txt
        String data = k21.toString();
        //找到：冒号的位置
        int index = data.indexOf(":");

        String word = data.substring(0, index);        //单词
        String fileName = data.substring(index + 1);   //文件名

        //输出：
        context.write(new Text(word), new Text(fileName+":"+total));

    }
}
