package no.vimond.matteo.KafkaConsumerAdapter.adapters;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import no.vimond.matteo.KafkaConsumerAdapter.interfaces.ConsumerGroup;

public class KakfaConsumerGroup implements ConsumerGroup
{
	private AtomicBoolean _running;
	private Set<String> _topics;
	//hardcoded just for test purposes
	private final int _numThreads = 3;

	public KakfaConsumerGroup()
	{
		this._topics = new HashSet<String>();
		this._running = new AtomicBoolean(false);
	}

	public void subscribe(String topic)
	{
		this._topics.add(topic);
	}

	public void unsubscribe(String topic)
	{
		boolean result = this._topics.remove(topic);
		if (result != true)
		{
			System.err.println("Topic not found, impossible to unsubscribe!");
		}
	}

	public Set<String> getAllSubscribedTopic()
	{
		return this._topics;
	}

	public void processMessage(String message)
	{
		System.out.println(message);
	}

	public void start()
	{
		this._running.set(true);
		
	}

	public void stop()
	{
		this._running.set(false);
		
	}

	public AtomicBoolean isRunning()
	{
		return this._running;
	}

}
