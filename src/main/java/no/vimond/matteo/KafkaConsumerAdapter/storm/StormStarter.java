package no.vimond.matteo.KafkaConsumerAdapter.storm;

import java.util.UUID;

import no.vimond.matteo.KafkaConsumerAdapter.storm.bolts.BasicBolt;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class StormStarter
{
	private static final String brokerZkStr = "0.0.0.0:2181";
	private static final String topic = "test-topic-multi";

	public void startTopology()
	{

		BrokerHosts hosts = new ZkHosts(brokerZkStr);
		SpoutConfig conf = new SpoutConfig(hosts, topic, "/" + topic, UUID
				.randomUUID().toString());
		
	
		KafkaSpout kafkaSpout = new KafkaSpout(conf);
		KafkaSpout kafkaSpout1 = new KafkaSpout(conf);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("kafka-spout", kafkaSpout);
		builder.setBolt("kafka-bolt", new BasicBolt(), 2).shuffleGrouping("kafka-spout");
		
		Config topConfig = new Config();
		topConfig.setDebug(true);
		topConfig.setNumWorkers(2);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("HelloStorm", topConfig , builder.createTopology());
	}
}
