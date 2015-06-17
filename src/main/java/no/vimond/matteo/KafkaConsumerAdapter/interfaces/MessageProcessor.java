package no.vimond.matteo.KafkaConsumerAdapter.interfaces;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public interface MessageProcessor extends Runnable
{
	public void process(MessageAndMetadata<byte[], byte[]> messageAndMetadata);
	
	public void setStream(KafkaStream<byte[], byte[]> stream);
	
	public MessageProcessor createMessageProcessor(String group);

}
