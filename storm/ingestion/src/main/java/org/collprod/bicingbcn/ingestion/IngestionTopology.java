package org.collprod.bicingbcn.ingestion;

import java.io.BufferedReader;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author Juan Rodriguez Hortala <juan.rodriguez.hortala@gmail.com>
 * 
 * Run in distributed mode by setting "ingestion.properties:topology.name" to a
 * not empty value and executing:
 * 
 * [cloudera@localhost ingestion]$ storm jar target/storm-ingestion-0.0.1-SNAPSHOT.jar org.collprod.bicingbcn.ingestion.IngestionTopology
 * */
public class IngestionTopology {
	// For running using ${workspace_loc:storm-ingestion/src/main/resources} as working directory
	private static final String DEFAULT_INGESTION_PROPS="/ingestion.properties";
	private static final String DEFAULT_DATASOURCE_PATH = "datasources"; // local as prefix for Reflections

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
		Set<String> files = new Reflections(datasourcesPath, new ResourcesScanner()).getResources(Pattern.compile(".*\\.properties"));
		for (String datasourceConfigFile : files) {
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
	
	public static void main(String[] args) {
		// FIXME: delete arguments parsing, only configuration is resources will be supported,
		// to easy use with Storm distributed. Also rename variables accordingly
		String ingestionPropertiesPath = IngestionTopology.class.getResource(DEFAULT_INGESTION_PROPS).getFile();
		String datasourcesPath = DEFAULT_DATASOURCE_PATH; // resolver later with Reflections
		String topologyName = null;
		PropertiesConfiguration ingestionConfig  = null;
		Map<String, String> datasourcesConfigurations = null;
				
		System.out.println("Usage: [ingestion properties configuration path] [datatasource path]");
		System.out.println("\tTopology will be executed in parallel iff the property 'topology.name' in " +
				"the ingestion configuration is not empty");
		System.out.println("");
		if (args.length >= 1) {
			datasourcesPath  = args[0];
		}
		
		if (args.length >= 2) {
			datasourcesPath = args[1];
		}
		
		// Load configuration
		LOGGER.info("Loading configuration");
		try{
			ingestionConfig = new PropertiesConfiguration();
			ingestionConfig.load(new BufferedReader(new InputStreamReader(IngestionTopology.class.getResourceAsStream(DEFAULT_INGESTION_PROPS))));
			LOGGER.info("main configuration loaded");
			System.out.println(ingestionConfig.isEmpty());
			datasourcesConfigurations = loadDatasources(datasourcesPath);
			LOGGER.info("data sources configuration loaded");
		} catch(ConfigurationException ce) {
			LOGGER.error("There was an error loading configuration, program will stop: " + ce.getMessage());
			ce.printStackTrace();
			System.exit(1);
		}
		topologyName = ingestionConfig.getString("topology.name");
		Config conf = new Config();
		
		// FIXME: add a property to ingestionPropertiesPath to turn on and off these stuff  
		conf.setDebug(true);
		// conf.put(Config.TOPOLOGY_DEBUG, true); // this is not working, this disables even info logs
		
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
			// FIXME: probably this should be computed as a multiple of a 
			// a property added to ingestionPropertiesPath. For now use to avoid crushing the VM 
		conf.put(Config.TOPOLOGY_WORKERS, 2);
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		// create a Spout instance / task per data source
		topologyBuilder.setSpout(RestIngestionSpout.class.getName(), new RestIngestionSpout(), 
				datasourcesConfigurations.size());
		topologyBuilder.setBolt(AvroWriterBolt.class.getName(), new AvroWriterBolt(),
				datasourcesConfigurations.size() * 2).localOrShuffleGrouping(RestIngestionSpout.class.getName());
		
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
