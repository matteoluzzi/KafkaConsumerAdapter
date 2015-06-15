package no.vimond.matteo.KafkaConsumerAdapter.interfaces;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public interface ConsumerGroup {
	
	public void start();
	
	public void stop();
	
	public AtomicBoolean isRunning();
	
	public void subscribe(String topic);
	
	public void unsubscribe(String topic);
	
	public Set<String> getAllSubscribedTopic();

}
