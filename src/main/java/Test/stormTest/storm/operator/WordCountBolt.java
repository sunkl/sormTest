package Test.stormTest.storm.operator;

import java.util.HashMap;
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
 * @date 创建时间：2017年4月27日 下午2:48:58 
 */
public class WordCountBolt extends BaseRichBolt{
	private OutputCollector collector;
	private Map<String,Long> counts = null;
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = this.counts.get(word);
		if(count == null){
			count = 0L;
		}
		count++;
		this.counts.put(word, count);
		this.collector.emit(new Values(word,count));
	}

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.counts = new HashMap<String, Long>();
	}

	public void declareOutputFields(OutputFieldsDeclarer declear) {
		declear.declare(new Fields("word","count"));
	}

}
