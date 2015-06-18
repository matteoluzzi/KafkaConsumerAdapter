package no.vimond.matteo.KafkaConsumerAdapter.storm.bolts;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BasicBolt extends BaseRichBolt
{

	private static final long serialVersionUID = 1L;
	
	private OutputCollector collector;
	private static final Logger LOG = Logger.getLogger(BasicBolt.class);

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector)
	{
		this.collector = collector;
	}

	public void execute(Tuple input)
	{
		byte[] raw_sentence = extractValueFromTuple(input, 0);
		String sentence = new String(raw_sentence);
		String[] tokens = sentence.split(" ");
		for(String token : tokens)
		{
			Values outputTuple = new Values(token, 1);
			this.collector.emit(outputTuple);
			printLog(outputTuple.toString());
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("word", "count"));
	}
	
	@SuppressWarnings("unchecked")
	private static <T> T extractValueFromTuple(Tuple input, int pos)
	{
		return (T) input.getValue(pos);
	}
	
	private void printLog(String message)
	{
		LOG.info(message);
	}

}
