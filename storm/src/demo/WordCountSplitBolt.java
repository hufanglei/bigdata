package demo;


import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

//第二级组件，是bolt组件，用于单词的拆分
public class WordCountSplitBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void execute(Tuple tuple) {
        //如何处理上一级组件发来的数据: I love Beijing
        String data = tuple.getStringByField("sentence");
        //分词
        String[] words = data.split(" ");
        //输出
        for(String w:words){
            collector.emit(new Values(w,1));
        }
    }
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        // 对Bolt进行初始化
        this.collector = collector;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declare) {
       //申明Tuple的格式
        declare.declare(new Fields("word","count"));
    }
}
