package no.vimond.matteo.KafkaConsumerAdapter.adapters;

import java.util.HashSet;
import java.util.Set;

import no.vimond.matteo.KafkaConsumerAdapter.interfaces.ConsumerGroup;
import no.vimond.matteo.KafkaConsumerAdapter.properties.KafkaProperties;

public class KafkaConsumerHandler {

	private KafkaProperties _properties;
	private Set<ConsumerGroup> _consumerGroups;
	
	public KafkaConsumerHandler()
	{
		this._properties = new KafkaProperties();
		this._consumerGroups  = new HashSet<ConsumerGroup>();
	}
	
	public registerConsumerGroup()
	{
		KakfaConsumerGroup group = new KakfaConsumerGroup();
		this._consumerGroups.add(group);
		group.startListening();
	}
	
	
	
}
