package no.vimond.matteo.KafkaConsumerAdapter.adapters;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import no.vimond.matteo.KafkaConsumerAdapter.interfaces.ConsumerGroup;
import no.vimond.matteo.KafkaConsumerAdapter.processors.DummyProcessor;
import no.vimond.matteo.KafkaConsumerAdapter.properties.KafkaProperties;
import no.vimond.matteo.KafkaConsumerAdapter.utils.GlobalConstants;

import org.apache.log4j.Logger;

public class KafkaConsumerHandler {
	
	private final Logger LOG = Logger.getLogger(KafkaConsumerHandler.class);

	private KafkaProperties _properties;
	private Set<ConsumerGroup> _consumerGroups;
	private AtomicLong _counter;
	
	public KafkaConsumerHandler()
	{
		this._properties = new KafkaProperties();
		this._consumerGroups  = new HashSet<ConsumerGroup>();
		this._counter = new AtomicLong(0);
	}
	
	public void registerConsumerGroup(Set<String> topics)
	{
		this.registerConsumerGroup(null, topics);
	}
	
	public void registerConsumerGroup(String groupName, Set<String> topics)
	{
		if(groupName == null)
			this._properties.addOrUpdateProperty(GlobalConstants._groupIdKey, "group" + this._counter.getAndIncrement());
		else
			this._properties.addOrUpdateProperty(GlobalConstants._groupIdKey, groupName);
		
		groupName = this._properties.getProperty(GlobalConstants._groupIdKey);
		
		//TODO replace the processor argument with a factory method
		KakfaConsumerGroup group = new KakfaConsumerGroup(_properties, topics, new DummyProcessor(groupName));
		this._consumerGroups.add(group);
		LOG.info("KafkaConsumerHandler: added new group " + groupName + "to the topics: " + topics);	
	}
	
	public void startListening()
	{
		for(ConsumerGroup group : this._consumerGroups)
			group.start();
	}
	
	
	
}
