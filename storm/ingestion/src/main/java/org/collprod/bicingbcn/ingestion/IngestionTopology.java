package org.collprod.bicingbcn.ingestion;

import java.io.CharArrayWriter;
import java.io.File;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author Juan Rodriguez Hortala <juan.rodriguez.hortala@gmail.com>
 * */
public class IngestionTopology {
	// For running using ${workspace_loc:storm-ingestion/src/main/resources} as working directory
	private static final String DEFAULT_INGESTION_PROPS="ingestion.properties";
	private static final String DEFAULT_DATASOURCE_PATH = "datasources";

	private final static Logger LOGGER = LoggerFactory.getLogger(IngestionTopology.class);

	// FIXME: this configuration stuff should be moved to its own class
	public static final String DATASOURCE_CONF_KEY = IngestionTopology.class.getName() + "DATASOURCE_CONF_KEY";
	/**
	 * @param datasourcesPath Local path of the data sources directory. Should contain several properties
	 * files with the configuration of each data source 
	 * 
	 * @return a map from the data source id as defined in the property "datasource_id" to the corresponding 
	 * configuration serialized as string, with an entry for each of the files at datasourcesPath. By serializing 
	 * as string we can then use the Map as a value in a backtupe.storm.Config object without getting an exception
	 * for "Topology conf is not json-serializable". The configuration object can be later reconstructed by 
	 * calling deserializeConfiguration()
	 * 
	 * @throws ConfigurationException If there was a problem parsing or serializing a configuration
	 * */
	private static Map<String, String> loadDatasources(String datasourcesPath) throws ConfigurationException  {
		CharArrayWriter charWriter = new CharArrayWriter();
		Map<String, String> configurations = new HashMap<String, String>();
		for (File datasourceConfigFile : new File(datasourcesPath).listFiles()) {
			PropertiesConfiguration datasourceConfig = new PropertiesConfiguration(datasourceConfigFile);
			charWriter.reset();
			datasourceConfig.save(charWriter);
			if (configurations.containsKey(datasourceConfig.getString("datasource_id"))) {
				throw new RuntimeException("Found duplicated configutation for datasource [" 
							+ datasourceConfig.getString("datasource_id") + "]");
			}
			configurations.put(datasourceConfig.getString("datasource_id"), charWriter.toString());
		}
		return configurations;
	}
	
	/**
	 * Takes a PropertiesConfiguration object serialized as string, and returns the corresponding configuration object
	 * 
	 * @throws ConfigurationException if there is a problem parsing the configuration
	 * */
	public static Configuration deserializeConfiguration(String serializedConfig) throws ConfigurationException {
		PropertiesConfiguration deserializedConfig = new PropertiesConfiguration();
		deserializedConfig.load(new StringReader(serializedConfig));
		return deserializedConfig;
	}
	
	/**
	 * @return the result of applying deserializeConfiguration() to each value in serializedConfigs	 * 
	 * @throws ConfigurationException if there is a problem parsing a configuration
	 * */
	public static Map<String, Configuration> deserializeConfigurations(Map<String, String> serializedConfigs) throws ConfigurationException  {
		Map<String, Configuration> deserializedConfigs = new HashMap<String, Configuration>();
		for (Map.Entry<String, String> datasourceConf : serializedConfigs.entrySet()) {
				deserializedConfigs.put(datasourceConf.getKey(), deserializeConfiguration(datasourceConf.getValue()));
		}
		
		return deserializedConfigs;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String ingestionPropertiesPath = DEFAULT_INGESTION_PROPS;
		String datasourcesPath = DEFAULT_DATASOURCE_PATH;
		String topologyName = null;
		Configuration ingestionConfig  = null;
		Map<String, String> datasourcesConfigurations = null;
		
		System.out.println("Usage: [ingestion properties path] [datatasource path]");
		if (args.length >= 1) {
			datasourcesPath  = args[0];
		}
		
		if (args.length >= 2) {
			datasourcesPath = args[1];
		}
		
		// Load configuration
		LOGGER.info("Loading configuration");
		try{
			ingestionConfig = new PropertiesConfiguration(new File(ingestionPropertiesPath));	
			datasourcesConfigurations = loadDatasources(datasourcesPath);
		} catch(ConfigurationException ce) {
			LOGGER.error("There was an error loading configuration, program will stop: " + ce.getMessage());
			ce.printStackTrace();
			System.exit(1);
		}
		topologyName = ingestionConfig.getString("topology.name");
		Config conf = new Config();
		conf.setDebug(true); // FIXME
		for (Map.Entry<Object, Object> entry : ConfigurationConverter.getMap(ingestionConfig).entrySet()) {
			conf.put(entry.getKey().toString(), entry.getValue());
		}
		conf.put(DATASOURCE_CONF_KEY, datasourcesConfigurations);
		LOGGER.info("Done loading configuration");
		
		// Clear Redis
		// TODO: this is mostly for testing, but maybe should be permanent, think about it
		RestIngestionSpout.clearDb(conf);
		
		// Build topology
		LOGGER.info("Building topology");
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		// create a Spout instance per data source
		topologyBuilder.setSpout(RestIngestionSpout.class.getName(), new RestIngestionSpout(), 
				datasourcesConfigurations.size());
		// TODO adjust parallelism
			// must group by data source id so the same TimestampParserBolt controls the 
			// timestamp of the last data which was downloaded
		topologyBuilder.setBolt(TimestampParserBolt.class.getName(), new TimestampParserBolt(),
				datasourcesConfigurations.size()).fieldsGrouping(RestIngestionSpout.class.getName(), 
																 new Fields(RestIngestionSpout.DATASOURCE_ID));
		
		// TODO try to use localOrShuffleGrouping when possible
	
		// Launch topology
		LOGGER.info("Launching topology");
		if(topologyName != null && !topologyName.equals("")) {
			// run in distributed mode
				// FIXME, but at least as many workers as data sources
			conf.setNumWorkers(datasourcesConfigurations.size() * 2); 
			try {
				StormSubmitter.submitTopology(topologyName, conf, topologyBuilder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("IngestionTopologyLocalTestRun", conf, topologyBuilder.createTopology());
		}
	}

}
