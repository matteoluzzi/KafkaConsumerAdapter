package no.vimond.matteo.KafkaConsumerAdapter.properties;

import java.io.IOException;
import java.util.Properties;
import java.util.Map.Entry;

import no.vimond.matteo.KafkaConsumerAdapter.utils.GlobalConstants;
import no.vimond.matteo.KafkaConsumerAdapter.utils.Utility;

public class KafkaProperties extends Properties {

	private static final long serialVersionUID = 1L;
	
	
	public KafkaProperties()
	{
		this.defaults = new Properties();
		try 
		{
			this.defaults.load(Utility.loadPropertiesFileFromClassPath(GlobalConstants._propertiesFile));
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
