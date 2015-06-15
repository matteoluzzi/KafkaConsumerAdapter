package no.vimond.matteo.KafkaConsumerAdapter;

import java.util.HashSet;
import java.util.Set;

import no.vimond.matteo.KafkaConsumerAdapter.adapters.KafkaConsumerHandler;

public class App 
{	
    public static void main( String[] args )
    {
    	KafkaConsumerHandler handler = new KafkaConsumerHandler();
    	Set<String> topics = new HashSet<String>();
    	topics.add("test-topic");
    //	topics.add("test-topic-matteo1");
    	handler.registerConsumerGroup(topics);
    	handler.startListening();
    	
    }
}
