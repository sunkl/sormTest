package Test.stormTest.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import Test.stormTest.storm.operator.ReportBolt;
import Test.stormTest.storm.operator.SentenceSpout;
import Test.stormTest.storm.operator.SplitSentenceBolt;
import Test.stormTest.storm.operator.WordCountBolt;

/**
 * @author  sunkl: 
 * @date 创建时间：2017年4月27日 下午2:05:46 
 */
public class Main {
	private static final String Sentence_spout_id = "sentence_spout";
	private static final String Split_bolt_id = "split_bolt";
	private static final String count_bold_id = "count_bolt";
	private static final String Report_bolt_id = "report_bolt";
	private static final String Topology_Name = "word_count_topology";
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(Sentence_spout_id, new SentenceSpout());
		builder.setBolt(Split_bolt_id, new SplitSentenceBolt()).shuffleGrouping(Sentence_spout_id);
		builder.setBolt(count_bold_id, new WordCountBolt()).fieldsGrouping(Split_bolt_id,new Fields("word"));
		builder.setBolt(Report_bolt_id, new ReportBolt()).globalGrouping(count_bold_id);
		Config config = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(Topology_Name, config, builder.createTopology());
		try {
			Thread.sleep(1000*60);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		cluster.killTopology(Topology_Name);
		cluster.shutdown();
	}
}
