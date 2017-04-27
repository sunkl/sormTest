package Test.stormTest.storm.operator;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * @author  sunkl: 
 * @date 创建时间：2017年4月27日 下午2:30:13 
 */
public class SplitSentenceBolt extends BaseRichBolt{
	private static final long serialVersionUID = 1L;
	OutputCollector collector;
	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for(String word:words){
			this.collector.emit(new Values(word));
		}
	}

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declear) {
		declear.declare(new Fields("word"));
		
	}

}
