package org.collprod.bicingbcn.ingestion;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Request;
import org.collprod.bicingbcn.ingestion.tsparser.TimeStampParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.atlassian.fugue.Either;
import com.google.auto.value.AutoValue;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.math.LongMath;

/**
 * @author Juan Rodriguez Hortala <juan.rodriguez.hortala@gmail.com>
 * 
 * Emits tuples of the following shape:
 *  - DATASOURCE_ID :: String
 *  - TIMESTAMP_FIELD :: Long
 *  - CONTENT_FIELD :: String
 *  
 * This spouts parses the timestamp of the data and drops that data which was already downloaded, i.e., 
 * which has a timestamp less or equal to the last timestamp. This parsing is not performed in a 
 * separate bolt because we cannot guarantee that the order of emision of tuples is preserved,
 * and we don't want a delayed tuple to be rejected because it arrived to a timestamp control bolt
 * after a tuple which was emited later 
 * 
 * The mutable state for this spout is stored as a DatasourceState object per data source
 * assigned to this spout. The only mutable state per data source is a Stopwatch object, a 
 * TimestampParser and the last timestamp.
 * That mutable state could be persisted in some data store, but that is not needed because 
 * if the state is lost and the spout restarted that only affects the first call to nextTuple(), 
 * which maybe would download a file twice, but then the later calls would be ok
 * 
 * Guaranteed processing strategy:
 * - each file is stored in Redis as a String when download from the consumed service. Default Redis RDB
 * persistance and the capacity of up to 512 MB per Redis string (http://redis.io/topics/data-types) 
 * should be enough: if the service generates files bigger than that then this approach is not sound 
 * (another database and several instances of this spout taking turns to consume the service, and also
 * local grouping for connecting components, could be an option)
 * - a fresh UUID is used for each file to generate a fresh Redis key
 * - in case of processing failure (e.g. in the Spout that stores the file) the tuple is emitted again; 
 * in case of ack its key is deleted  
 * */
public class RestIngestionSpout extends BaseRichSpout {
	
	public static final String CONTENT_FIELD = "CONTENT_FIELD";  
	public static final String TIMESTAMP_FIELD = "TIMESTAMP_FIELD";
	public static final String DATASOURCE_ID = "DATASOURCE_ID"; 
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RestIngestionSpout.class);
	
	// Auto generated by Eclipse
	private static final long serialVersionUID = -3787506061156020464L;
	
	/**
	 * Storm collector to emit tuples
	 * */
	private SpoutOutputCollector collector;
	
	/**
	 * Prefix to use for all the Redis keys, composed by this class
	 * name together with the Storm topology Id 
	 * */
	private String redisKeyPrefix; 
	
	/**
	 * Connection to Redis
	 * */
	private Jedis dbConnection;
	
	/**
	 * Tuples will be emitted with this refresh rate in miliseconds, which corresponds to the 
	 * greatest common divisor for the set of refresh rates for all sources
	 * 
	 * Using miliseconds as that is the resolution of storm Utils.sleep()
	 * */
	private long refreshRate;
	
	/**
	 * Configuration for each data source
	 * */
	private DatasourceState[] assignedDatasourcesConfs;

	/**
	 * Class used to store the configuration (inmutable state) and the mutable state 
	 * for each data source 
	 * */
	/*
	 * AutoValue doesn't support private nested classes
	 * 
	 * The documentation says "AutoValue does not and will not support creating mutable value types" 
	 * (https://github.com/google/auto/tree/master/value#restrictions-and-non-features). As can be seen
	 * in the generated code, this means the generated class has no setters, but an attribute may have a 
	 * mutable type: this is similar to what happens with Python tuples, once created the tuple components
	 * are fixed, but you are free to mutate a single component (e.g. t = ([], 1); t[0].append(1))
	 * */
	@AutoValue
	static abstract class DatasourceState {
		DatasourceState() {}
		public static DatasourceState create(String datasourceId, Request request, 
				float refreshRate, int dowloadRetries, Stopwatch stopwatch,
				Either<String, TimeStampParser> timestampParser, MutableLong lastTimestamp) {
	        return new AutoValue_RestIngestionSpout_DatasourceState(datasourceId, request, 
	        														refreshRate, dowloadRetries, stopwatch,
	        														timestampParser, lastTimestamp);
	      }
		/**
		 * Id of the datasource corresponding to this configuration
		 * Kind: inmutable configuration
		 * */
		public abstract String datasourceId();
		/**
		 * Request object to obtain files for this data source
		 * Kind: inmutable configuration
		 * */
	    public abstract Request request();
	    /**
	     * Refresh rate in miliseconds for requesting a new file for this data source
	     * 
	     * Kind: inmutable configuration
	     * */
	    public abstract float refreshRate();
	    
	    /**
	     * How many times a download is attempted in case of request failure 
	     * 
	     * Kind: inmutable configuration
	     * */
	    public abstract int dowloadRetries();
	    /**
	     * Counts the number of miliseconds since the last request to the service associated 
	     * to the data source.
	     * 
	     * Kind: mutable state
	     * */
	    public abstract Stopwatch stopwatch();
	    
	    /**
		 * Either the class name for a TimeStampParser (left), or a TimeStampParser object (right)
		 * 
		 * Kind: mutable state
		 * */
		public abstract Either<String, TimeStampParser> timestampParser();
		
		/**
		 * Last timestamp found for the data
		 * 
		 * Kind: mutable state
		 * */
		public abstract MutableLong lastTimestamp();
	}

	/**
	 * Find out which datasources correspond to this spout get the relevant configuration data 
	 * 
	 * @return a DatasourceConfig array with the state for each data source. Stopwatches are created stopped
	 * @throws RuntimeException if no datasource is assigned to this spout, which happens when there are more
	 * spout instances than data souces
	 * */
	private static DatasourceState [] getAssignedDatasources(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		// number of spouts
		int spoutSize = context.getComponentTasks(context.getThisComponentId()).size();
		// id for this spout
		int thisTaskIndex = context.getThisTaskIndex();
	    Map<String, String> datasourcesConfigurations = (Map<String, String>) conf.get(IngestionTopology.DATASOURCE_CONF_KEY);
	    
	    List<DatasourceState> assignedDatasourcesConfs = new LinkedList<DatasourceState>();
		int i = 0;
		for (Map.Entry<String, String> datasourceConf : datasourcesConfigurations.entrySet()) {
			if (i % spoutSize == thisTaskIndex) {
				Configuration newProperties;
				try {
					newProperties = IngestionTopology.deserializeConfiguration(datasourceConf.getValue());
				} catch (ConfigurationException ce) {
					LOGGER.error("Error parsing properties for datasource: " + ce.getMessage());
					ce.printStackTrace();
					throw new RuntimeException(ce);
				}
				Either<String, TimeStampParser> newTimestampParser = Either.left(newProperties.getString("timestamp_parser_class"));
				assignedDatasourcesConfs.add(DatasourceState.create(datasourceConf.getKey(), 
						Request.Get(newProperties.getString("datasource_url")),
						newProperties.getFloat("refresh_rate"),
						newProperties.getInt("dowload_retries"),
						Stopwatch.createUnstarted(),
						newTimestampParser, 
						new MutableLong(-1)
						));
			}
			i++;
		}
	  		
		if (assignedDatasourcesConfs.size() == 0) {
			String msg = "An spout instance for class " + RestIngestionSpout.class.getName() 
					+ " is not responsible for consuming any service";
			LOGGER.error(msg);
			throw new RuntimeException(msg);
		}
		
		return assignedDatasourcesConfs.toArray(new DatasourceState[assignedDatasourcesConfs.size()]);
	}
	
	/**
	 * Create an object for connecting to Redis according to conf, and use it to connect to Redis
	 * @param conf Configuration with the parameters to connect to Redis
	 * @return a Jedis object that can be used to talk to Redis
	 * */
	private static Jedis setupDbConnection(@SuppressWarnings("rawtypes") Map conf) {
		Jedis dbConnection = new Jedis(conf.get("redis.host").toString(),
									   Integer.parseInt(conf.get("redis.port").toString()));
		dbConnection.connect();
		return dbConnection;
	}
			
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {	
		// store collector to be able to emit later
		this.collector = collector;
		
		// Redis stuff
		this.redisKeyPrefix =  this.getClass().getName() + "_" + context.getStormId();
		this.dbConnection = setupDbConnection(conf);
		
		// Find out which data sources correspond to this spout and store the relevant configuration
		// data at this.assignedDatasourcesConfs
		this.assignedDatasourcesConfs = getAssignedDatasources(conf, context);
		
		// refreshRate should be the greatest common divisor for the set of refresh rates for all sources
		// source rates are floats in seconds, here we convert to miliseconds and round
		this.refreshRate = Math.round(this.assignedDatasourcesConfs[0].refreshRate() * 1000);
		for (int i = 1; i < this.assignedDatasourcesConfs.length; i++) {
			this.refreshRate = LongMath.gcd(this.refreshRate, Math.round(this.assignedDatasourcesConfs[i].refreshRate() * 1000));
		}
		
		// reste and start stopwatches
		for (DatasourceState datasourceSt : this.assignedDatasourcesConfs ) {
			datasourceSt.stopwatch().reset();
			datasourceSt.stopwatch().start();
		}
	}

	@Override
	public void close() {
		/*
		 * This method is useless in cluster mode, as stated by Storms documentation
		 * 
		 * http://nathanmarz.github.io/storm/doc-0.8.1/backtype/storm/topology/base/BaseRichSpout.html#close()
		 * 
		 * "Called when an ISpout is going to be shutdown. There is no guarentee that close will be called, 
		 * because the supervisor kill -9's worker processes on the cluster.
		 * The one context where close is guaranteed to be called is a topology is killed when running Storm in local mode."
		 * */
	}
	
	@Override
	protected void finalize() throws Throwable {
		this.dbConnection.close();
		super.close();
	}
	
	// TODO: move to independent class, by adding extra arguments
	/**
	 * Store in Redis a tuple as one or more key-value pairs, using an UUID as the 
	 * basis for the key.
	 * NOTE: all the tuple components will be stored as the String returned by toString()
	 * 
	 * @param tuple Storm tuple to be stored in Redis
	 * @param tupleId will be used to build the Redis key 
	 * */ 
	private void storeInRedis(Values tuple, UUID tupleId) {
		int i = 0;
		for (Object field : tuple) {
			this.dbConnection.set(this.redisKeyPrefix + tupleId + "_" + i, field.toString());
			i++;
		}
	}
	
	/**
	 * Restore a tuple stored by storeInRedis()
	 * @param UUID UUID used to store the tuple in Redis
	 * */
	private Values getFromRedis(Object tupleId) {
		Set<String> keys = this.dbConnection.keys(this.redisKeyPrefix + tupleId + "_" + "*");
		Object [] values = new String[keys.size()];
		int tupleIndex;
		for (String key : keys) {
			// NOTE as keys are returned as a set its order is not preserved, hence we
			// must exploit the definition of storeInRedis, and get the tuple component
			// index from the last character of the key
			tupleIndex = Integer.parseInt(key.substring(key.length() - 1));
			values[tupleIndex] = this.dbConnection.get(key);
		}
		return new Values(values);
	}
	
	/**
	 * This is basically a test method, that might be called before starting the topology
	 * to delete all the keys written by this spout
	 * @param conf Configuration with the parameters to connect to Redis
	 * */
	public static void clearDb(@SuppressWarnings("rawtypes") Map conf) {	
		Jedis dbConnection = setupDbConnection(conf);
		for (String key : dbConnection.keys(RestIngestionSpout.class.getName() + "_" + "*")) {
			dbConnection.del(key);
		} 
		dbConnection.close();
	}
	
	private static void logDownloadError(DatasourceState datasourceState, Exception e) {
		LOGGER.warn("Error downloading file for data source " 
						+  datasourceState.datasourceId() + " : "+ e.getMessage());
	}
	
	private static String downloadFile(DatasourceState datasourceState) {
		String newData = null;
		for (int i = 0; i < datasourceState.dowloadRetries(); i++) {
			try {
				newData = datasourceState.request().execute().returnContent().asString();
				break;
			} catch (ClientProtocolException cpe) {
				// return code different to 200
				logDownloadError(datasourceState, cpe);
			} catch (IOException ioe) {
				// bad URI or similar
				logDownloadError(datasourceState, ioe);
			}
		}
		return newData;
	} 
	
	/**
	 * Get the TimeStampParser for datasourceState, creating a new instance and storing it if necessary
	 * */
	private TimeStampParser getParser(DatasourceState datasourceState) {
		Either<String, TimeStampParser> timestampParser = datasourceState.timestampParser(); 
		if (timestampParser.isLeft()) {
			try {
				timestampParser =  Either.right((TimeStampParser) Class.forName(timestampParser.left().get()).newInstance());
			} catch (InstantiationException ie) {
				LOGGER.error("Error creating TimeStampParser for data source " + datasourceState.datasourceId() 
						+ ": " + ie.getMessage());
				throw new RuntimeException(ie);
			} catch (IllegalAccessException iae) {
				LOGGER.error("Error creating TimeStampParser for data source " + datasourceState.datasourceId() 
						+ ": " + iae.getMessage());
				throw new RuntimeException(iae);
			} catch (ClassNotFoundException cnfe) {
				LOGGER.error("Error creating TimeStampParser for data source " + datasourceState.datasourceId() 
						+ ": " + cnfe.getMessage());
				throw new RuntimeException(cnfe);
			}
		} 
		return timestampParser.right().get();
	}
	
	@Override
	public void nextTuple() {
		// Check which datasource/s should be polled
		for (DatasourceState datasourceState : this.assignedDatasourcesConfs) {
			if (datasourceState.stopwatch().elapsed(TimeUnit.MILLISECONDS) > datasourceState.refreshRate()) {
				// time to download a new file
				datasourceState.stopwatch().reset();

				// try downloading a new file
				String newData = downloadFile(datasourceState);
				
				// download error: will try later
				if (newData == null) {
					LOGGER.warn("Failed to download file for data source " 
							+ datasourceState.datasourceId() + " will retry in " 
							+ datasourceState.refreshRate() / 1000 + " seconds");
				} else {
					// successful download
					// Emit tuple only if it's new: if it has a new timestamp
					TimeStampParser timestampParser = getParser(datasourceState);
					Optional<Long> newTimestamp = timestampParser.apply(newData);
					if (newTimestamp.isPresent()) {
						// only emit the tuple if we obtain a new timestamp, so the data is new 
						MutableLong lastTimestamp = datasourceState.lastTimestamp();
						Long newTimestampValue = newTimestamp.get();
						if (newTimestampValue > lastTimestamp.longValue()) {
							// emit
							UUID tupleId = UUID.randomUUID();
							Values tuple = new Values(datasourceState.datasourceId(), newTimestampValue, newData);
								// store is Redis
							storeInRedis(tuple, tupleId);
								// emit a tuple with an id
							this.collector.emit(tuple, tupleId);							
								// update timestamp
							lastTimestamp.setValue(newTimestampValue);
						} else {
							LOGGER.info("Skipping file with repeated timestamp {}", newTimestampValue);
						}
					}
					else {
						LOGGER.warn("Could not parse timestamp for data source " + datasourceState.datasourceId());
					}
				}
				datasourceState.stopwatch().start();
			}
		}
		// sleep until next poll
			// Note Util.sleep accepts milliseconds
		Utils.sleep(this.refreshRate);
	}

	@Override
	public void ack(Object tupleId) {
		LOGGER.info("Finished processing tuple with id {}", tupleId);
		this.dbConnection.del(tupleId.toString());
	}

	@Override
	public void fail(Object tupleId) {
		Values tuple = getFromRedis(tupleId); 		
		String datasourceId = tuple.get(0).toString();
			// this type conversion is necessary as tuples are serialized in Redis
			// with a String in each component, and the bolts expect a Long value 
			// for that field
		tuple.set(1, Long.parseLong(tuple.get(1).toString()));
		LOGGER.warn("Failed to process data for data source {}, will retry", datasourceId);
		this.collector.emit(tuple, tupleId);
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(DATASOURCE_ID, TIMESTAMP_FIELD, CONTENT_FIELD));
	}

}
