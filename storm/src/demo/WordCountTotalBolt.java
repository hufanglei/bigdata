package demo;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCountTotalBolt extends BaseRichBolt {

    private OutputCollector collector;

    //定义一个Map集合来保存结果
    private Map<String, Integer> result = new HashMap<>();

    @Override
    public void execute(Tuple tuple) {
        // 对每个单词进行计数
        //取出数据
        String word = tuple.getStringByField("word");
        int count = tuple.getIntegerByField("count");
        if(result.containsKey(word)){
            //如果包含，进行累加
            int total = result.get(word);
            result.put(word, total+count);
        }else{
            //这个单词第一次出现
            result.put(word, count);
        }
        //打印在屏幕上
        System.out.println("统计的结果是: " + result);
        //把结果继续发送给下一个bolt组件
        this.collector.emit(new Values(word,result.get(word)));
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector = collector;
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declare) {
        // TODO Auto-generated method stub
        declare.declare(new Fields("word","total"));
    }
}
