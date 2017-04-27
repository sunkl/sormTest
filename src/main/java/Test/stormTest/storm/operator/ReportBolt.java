package Test.stormTest.storm.operator;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

/**
 * @author  sunkl: 
 * @date 创建时间：2017年4月27日 下午3:00:08 
 */
public class ReportBolt extends BaseRichBolt{
	private HashMap<String, Long> counts = null;
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = tuple.getLongByField("count");
		this.counts.put(word, count);
	}

	public void prepare(Map config, TopologyContext content, OutputCollector collector) {
		this.counts = new HashMap<String, Long>();
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declear) {
	}
	
	@Override
	public void cleanup() {
		System.out.println("----final counts---");
		Set<Entry<String, Long>> set = this.counts.entrySet();
		for(Entry<String, Long> entry :set){
			System.out.println("key:"+entry.getKey()+",value:"+entry.getValue());
		}
		System.out.println("--------------------");
	}
}
