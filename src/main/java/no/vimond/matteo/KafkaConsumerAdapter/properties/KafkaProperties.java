package no.vimond.matteo.KafkaConsumerAdapter.properties;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Map.Entry;

public class KafkaProperties extends Properties {

	private static final long serialVersionUID = 1L;
	
	
	public KafkaProperties()
	{
		this.defaults = new Properties();
		try 
		{
			Thread currentThread = Thread.currentThread();
			ClassLoader contextClassLoader = currentThread.getContextClassLoader();
			InputStream propertiesStream = contextClassLoader.getResourceAsStream("kafkasettings.properties");
			this.defaults.load(propertiesStream);
			for(Entry<Object, Object> entry : defaults.entrySet())
				this.setProperty((String) entry.getKey(), (String) entry.getValue());
			
		}catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	public void addOrUpdateProperty(String key, String value)
	{
		this.defaults.put(key, value);
	}
}
