package Test.stormTest.storm.operator;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * @author  sunkl: 
 * @date 创建时间：2017年4月27日 下午2:14:10 
 */
public class SentenceSpout extends BaseRichSpout{
	private SpoutOutputCollector collector ;
	private String[] sentences = {
			"hello world !",
			"it is my first storm progress",
			"i want to finish it at once",
			"come on !"
	};
	private int index = 0;
	public void nextTuple() {
		this.collector.emit(new Values(this.sentences[index]));
		index++;
		if(index>= this.sentences.length){
			index = 0;
		}
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declear) {
		declear.declare(new Fields("sentence"));
	}

}
