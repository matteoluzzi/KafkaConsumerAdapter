package no.vimond.matteo.KafkaConsumerAdapter.properties;

import java.io.IOException;
import java.util.Properties;

import no.vimond.matteo.KafkaConsumerAdapter.utils.GlobalConstants;

public class KafkaProperties extends Properties {

	private static final long serialVersionUID = 1L;
	
	
	public KafkaProperties()
	{
		super();
		this.defaults = new Properties();
		try 
		{
			this.defaults.load(this.getClass().getResourceAsStream(GlobalConstants._propertiesFile));
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
