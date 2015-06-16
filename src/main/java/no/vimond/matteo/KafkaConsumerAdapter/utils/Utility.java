package no.vimond.matteo.KafkaConsumerAdapter.utils;

import java.io.InputStream;

public class Utility
{

	public static synchronized InputStream loadPropertiesFileFromClassPath(String filename)
	{
		Thread currentThread = Thread.currentThread();
		ClassLoader contextClassLoader = currentThread.getContextClassLoader();
		InputStream propertiesStream = contextClassLoader.getResourceAsStream(filename);
		return propertiesStream;
	}
	
	
}
