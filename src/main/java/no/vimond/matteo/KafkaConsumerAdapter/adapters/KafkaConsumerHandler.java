package no.vimond.matteo.KafkaConsumerAdapter.adapters;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import no.vimond.matteo.KafkaConsumerAdapter.interfaces.ConsumerGroup;
import no.vimond.matteo.KafkaConsumerAdapter.properties.KafkaProperties;
import no.vimond.matteo.KafkaConsumerAdapter.utils.GlobalConstants;

public class KafkaConsumerHandler {

	private KafkaProperties _properties;
	private Set<ConsumerGroup> _consumerGroups;
	
	public KafkaConsumerHandler()
	{
		this._properties = new KafkaProperties();
		System.out.println(this._properties);
		this._consumerGroups  = new HashSet<ConsumerGroup>();
	}
	
	public void registerConsumerGroup(Set<String> topics)
	{
		this._properties.addOrUpdateProperty(GlobalConstants._groupIdKey, "group1");
		KakfaConsumerGroup group = new KakfaConsumerGroup(_properties, topics);
		this._consumerGroups.add(group);
	}
	
	public void startListening()
	{
		for(ConsumerGroup group : this._consumerGroups)
			group.start();
	}
	
	
	
}
