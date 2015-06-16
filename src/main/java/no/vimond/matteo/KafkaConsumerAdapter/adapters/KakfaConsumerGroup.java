package no.vimond.matteo.KafkaConsumerAdapter.adapters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import no.vimond.matteo.KafkaConsumerAdapter.interfaces.ConsumerGroup;
import no.vimond.matteo.KafkaConsumerAdapter.interfaces.MessageProcessor;
import no.vimond.matteo.KafkaConsumerAdapter.utils.GlobalConstants;

import org.apache.log4j.Logger;


public class KakfaConsumerGroup implements ConsumerGroup
{
	private final Logger LOG = Logger.getLogger(KakfaConsumerGroup.class);
	
	private String _groupId;
	private AtomicBoolean _running;
	private ConsumerConnector _clusterConnector;
	private Set<String> _topics;
	private ExecutorService _executor;
	private MessageProcessor _processor;
	//TODO hardcoded just for test purposes
	private int _totNumberOfPartitions = 2;
	private int _executorSize = 1;

	public KakfaConsumerGroup(Properties props, Set<String> topics, MessageProcessor processor)
	{
		this._groupId = props.getProperty(GlobalConstants._groupIdKey);
		this._topics = new HashSet<String>();
		this._running = new AtomicBoolean(false);
		this._clusterConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		// TODO substitute it with a more appropriate class which uses a custom thread factory
		this._executor = Executors.newFixedThreadPool(_executorSize);
		this._processor = processor;
		
		for(String topic : topics)
			this.subscribe(topic);
	}

	public void subscribe(String topic)
	{
		this._topics.add(topic);
	}

	public void unsubscribe(String topic)
	{
		boolean result = this._topics.remove(topic);
		if (result != true)
		{
			System.err.println("Topic not found, impossible to unsubscribe!");
		}
	}

	public Set<String> getAllSubscribedTopic()
	{
		return this._topics;
	}

	public void start()
	{
		LOG.info(this.toString() + ": start listening on topics " + this._topics);
		
		Map<String, Integer> topicsCountMap = new HashMap<String, Integer>();
		//two stream for a topic --> each topic has two partition
		for (String topic : this._topics)
			topicsCountMap.put(topic, 1);

		// map of topic, list of kafkastream
		Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = this._clusterConnector.createMessageStreams(topicsCountMap);

		for (final List<KafkaStream<byte[], byte[]>> topicStreams : topicMessageStreams.values())
		{
			for (final KafkaStream<byte[], byte[]> stream : topicStreams)
			{
				this._processor.setStream(stream);
				this._executor.submit(this._processor);
			}
		}
		this._running.set(true);
	}

	public void stop()
	{
		this._executor.shutdown();
		this._running.set(false);
	}

	public AtomicBoolean isRunning()
	{
		return this._running;
	}
	
	public synchronized void processMessage(MessageAndMetadata<byte[], byte[]> msgAndMetadata)
	{
		System.out.println(this.toString() + ": received message " + new String(msgAndMetadata.message()));
		LOG.info(this.toString() + ": received message " + new String(msgAndMetadata.message()));
	}
	
	public String toString()
	{
		return "KafkaConsumerGroup - " + this._groupId;
	}

}
