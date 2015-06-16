package no.vimond.matteo.KafkaConsumerAdapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import no.vimond.matteo.KafkaConsumerAdapter.adapters.KafkaConsumerHandler;
import no.vimond.matteo.KafkaConsumerAdapter.utils.GlobalConstants;
import no.vimond.matteo.KafkaConsumerAdapter.utils.Utility;

public class App 
{	
    public static void main( String[] args )
    {
    	
    	//initialize handler for connection with zookeeper
    	KafkaConsumerHandler handler = new KafkaConsumerHandler();
    	
    	//hardcoded for test purposes
    	Set<String> topics = new HashSet<String>();
    	topics.add("test-topic");
    	
    	Properties systemProperties = App.loadSystemProperties();
    	
    	Integer numOfGroups = Integer.parseInt((String) systemProperties.get(GlobalConstants._numOfConsumerGroup));
    	
    	while(numOfGroups > 0)
    	{
    		handler.registerConsumerGroup(topics);
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
}
