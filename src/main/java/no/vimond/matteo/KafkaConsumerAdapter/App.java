package no.vimond.matteo.KafkaConsumerAdapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import no.vimond.matteo.KafkaConsumerAdapter.adapters.KafkaConsumerHandler;
import no.vimond.matteo.KafkaConsumerAdapter.processors.DummyProcessor;
import no.vimond.matteo.KafkaConsumerAdapter.processors.MessageProcessorFactory;
import no.vimond.matteo.KafkaConsumerAdapter.utils.GlobalConstants;
import no.vimond.matteo.KafkaConsumerAdapter.utils.MessageProcessorType;
import no.vimond.matteo.KafkaConsumerAdapter.utils.Utility;

public class App 
{	
    public static void main( String[] args )
    {
    	//register processors used by factory method
    	registerProcessor();
    	
    	//load system properties
    	Properties systemProperties = App.loadSystemProperties();
    	
    	//initialize handler for connection with zookeeper
    	KafkaConsumerHandler handler = new KafkaConsumerHandler();
    	
    	String topicsList = (String) systemProperties.get(GlobalConstants._topics);
    	String[] topicsArr = topicsList.split("\\|");
    	
    	Integer numOfGroups = Integer.parseInt((String) systemProperties.get(GlobalConstants._numOfConsumerGroup));
    	
    	while(numOfGroups > 0)
    	{
    		handler.registerConsumerGroup(new HashSet<String>(Arrays.asList(topicsArr)));
    		numOfGroups--;
    	}
   
    	handler.startListening();
    	
    }
    
    public static Properties loadSystemProperties()
    {
    	Properties props = new Properties();
    	InputStream streamProps = Utility.loadPropertiesFileFromClassPath(GlobalConstants._appPropertiesFile);
    	try
		{
			props.load(streamProps);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
    	return props;
    }
    
    public static void registerProcessor()
    {
    	MessageProcessorFactory.getFactory().registerMessageProcessor(MessageProcessorType.DUMMY, new DummyProcessor(null));
    }
}
