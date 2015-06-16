package no.vimond.matteo.KafkaConsumerAdapter.processors;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import no.vimond.matteo.KafkaConsumerAdapter.interfaces.MessageProcessor;

import org.apache.log4j.Logger;


public class DummyProcessor implements MessageProcessor
{
	private Logger LOG = Logger.getLogger(DummyProcessor.class);
	private String _consumerGroup;
	private KafkaStream<byte[], byte[]> _stream;
	
	public DummyProcessor(String group)
	{
		this._consumerGroup = group;
		this._stream = null;
	}

	
	public KafkaStream<byte[], byte[]> getStream()
	{
		return this._stream;
	}
	
	public void setStream(KafkaStream<byte[], byte[]> stream)
	{
		this._stream = stream;
	}
	
	public void run()
	{	
		for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : this._stream)
		{
			try
			{
				process(msgAndMetadata);
			}
			catch(IllegalArgumentException e)
			{
				return;
			}
			
		}
	}

	public void process(MessageAndMetadata<byte[], byte[]> messageAndMetadata)
	{
		String key =  messageAndMetadata.key() != null ? new String(messageAndMetadata.key()) : "none";
		String message = new String(messageAndMetadata.message());
		if(DummyValidator.validate(message))
		{
			LOG.info(this.toString() + ": processed message " + "\"{key:" + key + ",message:" + message + "}\"");
		}
		else
		{
			LOG.warn("Message not valid, going to exit");
			throw new IllegalArgumentException();
		}	
	}
	
	public String toString()
	{
		return "MessageProcessor - ConsumerGroup " + this._consumerGroup;
	}
	
	private static class DummyValidator
	{
		public static boolean validate(Object message)
		{
			return true;
		}
	}
}
