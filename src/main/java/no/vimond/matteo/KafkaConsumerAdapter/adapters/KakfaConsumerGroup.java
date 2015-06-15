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
import no.vimond.matteo.KafkaConsumerAdapter.utils.GlobalConstants;

public class KakfaConsumerGroup implements ConsumerGroup
{
	private String _groupId;
	private AtomicBoolean _running;
	private ConsumerConnector _clusterConnector;
	private Set<String> _topics;
	private ExecutorService _executor;

	// hardcoded just for test purposes
	private final int _numThreads = 3;

	public KakfaConsumerGroup(Properties props, Set<String> topics)
	{
		this._groupId = props.getProperty(GlobalConstants._groupIdKey);
		this._topics = new HashSet<String>();
		this._running = new AtomicBoolean(false);
		this._clusterConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		// TODO substitute it with a more appropriate class which uses a custom thread factory
		this._executor = Executors.newFixedThreadPool(_numThreads);
		
		for(String topic : topics)
			this.subscribe(topic);
	}

	// ---------------GETTERS AND SETTERS-----------

	public String get_groupId()
	{
		return _groupId;
	}

	public void set_groupId(String _groupId)
	{
		this._groupId = _groupId;
	}

	public ConsumerConnector get_clusterConnector()
	{
		return _clusterConnector;
	}

	public void set_clusterConnector(ConsumerConnector _clusterConnector)
	{
		this._clusterConnector = _clusterConnector;
	}

	public Set<String> get_topics()
	{
		return _topics;
	}

	public void set_topics(Set<String> _topics)
	{
		this._topics = _topics;
	}

	public int get_numThreads()
	{
		return _numThreads;
	}

	// ---------------INHERITED METHODS-------------

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
		Map<String, Integer> topicsCountMap = new HashMap<String, Integer>();
		for (String topic : this._topics)
			topicsCountMap.put(topic, 1);

		// map of topic, list of kafkastream
		Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = this._clusterConnector
				.createMessageStreams(topicsCountMap);

		for (final List<KafkaStream<byte[], byte[]>> topicStreams : topicMessageStreams.values())
		{
			for (final KafkaStream<byte[], byte[]> stream : topicStreams)
			{
				this._executor.submit(new Runnable()
				{
					public void run()
					{
						for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream)
						{
							processMessage(msgAndMetadata);
						}
					}
				});
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
		System.out.println(" Received message: (" +
				  new String(msgAndMetadata.message()) + ")");
	}

}
