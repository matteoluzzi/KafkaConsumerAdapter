package no.vimond.matteo.KafkaConsumerAdapter.processors;

import java.util.HashMap;
import java.util.Map;

import no.vimond.matteo.KafkaConsumerAdapter.interfaces.MessageProcessor;
import no.vimond.matteo.KafkaConsumerAdapter.utils.MessageProcessorType;

public class MessageProcessorFactory
{
	private Map<MessageProcessorType, MessageProcessor> _registeredMessageProcessors;
	//singleton instance of the factory
	private static MessageProcessorFactory instance = new MessageProcessorFactory();
	
	private MessageProcessorFactory()
	{
		this._registeredMessageProcessors = new HashMap<MessageProcessorType, MessageProcessor>();
	}
	
	public static MessageProcessorFactory getFactory()
	{
		return instance;
	}
	
	public void registerMessageProcessor(MessageProcessorType type, MessageProcessor processor)
	{
		this._registeredMessageProcessors.put(type, processor);
	}
	
	public MessageProcessor createMessageProcessor(MessageProcessorType type, String group)
	{
		return this._registeredMessageProcessors.get(type).createMessageProcessor(group);
	}
}
