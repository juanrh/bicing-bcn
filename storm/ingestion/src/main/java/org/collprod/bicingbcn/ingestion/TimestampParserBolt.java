package org.collprod.bicingbcn.ingestion;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.mutable.MutableLong;
import org.collprod.bicingbcn.ingestion.tsparser.TimeStampParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.atlassian.fugue.Either;
import com.google.auto.value.AutoValue;
import com.google.common.base.Optional;

/**
 * Parses the timestamp of the data and drops that data which was already downloaded, i.e., 
 * which has a timestamp less or equal to the last timestamp
 * 
 * In case of failure is not a big loss to lose this timestamp, as that would just imply
 * that a single file would be emitted twice: altough this could be a problem for the 
 * next bolts depending on their behaviour. Hence the timestamp is also emitted 
 * in case the next bolts also want to control the timestamp 
 * */
public class TimestampParserBolt extends BaseRichBolt {

		// Auto generated by Eclipse
	private static final long serialVersionUID = 9174174434118277771L;
	
	private final static Logger LOGGER = LoggerFactory.getLogger(TimestampParserBolt.class);
	
	public static final String TIMESTAMP_FIELD = "TIMESTAMP_FIELD";
	
	/**
	 * For each data source stores a DatasourceState object, with the parser for that 
	 * datasource and the last timestamp downloaded for the data source
	 * */
	private Map<String, DatasourceState> datasourceStates;
	
	@AutoValue
	static abstract class DatasourceState {
		DatasourceState() {}
		public static DatasourceState create(Either<String, TimeStampParser> timestampParser, MutableLong lastTimestamp) {
	        return new AutoValue_TimestampParserBolt_DatasourceState(timestampParser, lastTimestamp);
	      }
		/**
		 * Either the class name for a TimeStampParser (left), or a TimeStampParser object (right) 
		 * */
		public abstract  Either<String, TimeStampParser> timestampParser();
		
		/**
		 * Last timestamp found for the data
		 * */
		public abstract MutableLong lastTimestamp();
	}
	
	/**
	 * Storm collector to emit tuples
	 * */
	private OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.datasourceStates = new HashMap<String, DatasourceState>(); 
		try {
			Map<String, Configuration> datasourcesConfigurations = 
					IngestionTopology.deserializeConfigurations((Map<String, String>) stormConf.get(IngestionTopology.DATASOURCE_CONF_KEY));
			for (Map.Entry<String, Configuration> datasourceConfig : datasourcesConfigurations.entrySet()) {
				Either<String, TimeStampParser> newTimestampParser = Either.left(datasourceConfig.getValue().getString("timestamp_parser_class"));
					// initialize timestampst to -1, as nothing has been download yet
				this.datasourceStates.put(datasourceConfig.getKey(), 
											DatasourceState.create(newTimestampParser, new MutableLong(-1)));
			}
			
		} catch (ConfigurationException ce) {
			LOGGER.error("Error parsing datasource configurations: " + ce.getMessage());
			throw new RuntimeException(ce);
		}
	}
	
	/**
	 * Get the TimeStampParser for datasourceState, creating a new instance and storing it if necessary
	 * */
	private TimeStampParser getParser(String datasource, DatasourceState datasourceState) {
		Either<String, TimeStampParser> timestampParser = datasourceState.timestampParser(); 
		if (timestampParser.isLeft()) {
			try {
				timestampParser =  Either.right((TimeStampParser) Class.forName(timestampParser.left().get()).newInstance());
			} catch (InstantiationException ie) {
				LOGGER.error("Error creating TimeStampParser for data source " + datasource 
						+ ": " + ie.getMessage());
				throw new RuntimeException(ie);
			} catch (IllegalAccessException iae) {
				LOGGER.error("Error creating TimeStampParser for data source " + datasource 
						+ ": " + iae.getMessage());
				throw new RuntimeException(iae);
			} catch (ClassNotFoundException cnfe) {
				LOGGER.error("Error creating TimeStampParser for data source " + datasource 
						+ ": " + cnfe.getMessage());
				throw new RuntimeException(cnfe);
			}
		} 
		return timestampParser.right().get();
	}

	@Override
	public void execute(Tuple inputTuple) {
		/*
		Get the time stamp parser
		 
		Create a new parser if needed. If this is too slow we
		then a timeout will be raised and the warranteed processing
		mechanism will replay the tuple, but now the parser will be ready 
		*/
		String datasource = inputTuple.getStringByField(RestIngestionSpout.DATASOURCE_ID);
		String data = inputTuple.getStringByField(RestIngestionSpout.CONTENT_FIELD);
		
		DatasourceState datasourceState = this.datasourceStates.get(datasource);
		TimeStampParser timestampParser = getParser(datasource, datasourceState);
		Optional<Long> newTimestamp = timestampParser.apply(data);
		if (newTimestamp.isPresent()) {
			// only emit the tuple if we obtain a new timestamp, so the data is new 
			MutableLong lastTimestamp = datasourceState.lastTimestamp();
			if (newTimestamp.get() > lastTimestamp.longValue()) {
				// emit
				this.collector.emit(new Values(newTimestamp, data));
				// update timestamp
				lastTimestamp.setValue(newTimestamp.get());
			}
		}
		else {
			LOGGER.warn("Could not parse timestamp for data source " + datasource 
						+ ", tuple will be replayed" );
			this.collector.fail(inputTuple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(TIMESTAMP_FIELD, RestIngestionSpout.CONTENT_FIELD));
	}
}
