package org.collprod.bicingbcn.ingestion;

import java.io.BufferedReader;
import java.io.CharArrayWriter;
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
import backtype.storm.tuple.Fields;

/**
 * @author Juan Rodriguez Hortala <juan.rodriguez.hortala@gmail.com>
 * 
 * Run in distributed mode by setting "ingestion.properties:topology.name" to a
 * not empty value and executing the following command. When "topology.name" is 
 * empty that commands runs in local mode but using storm's libraries, which is 
 * also different to an eclipse local run
 * 
 * [cloudera@localhost ingestion]$ storm jar target/storm-ingestion-0.0.1-SNAPSHOT.jar org.collprod.bicingbcn.ingestion.IngestionTopology
 * 
 * */
public class IngestionTopology {
	// For running using ${workspace_loc:storm-ingestion/src/main/resources} as working directory
	private static final String DEFAULT_INGESTION_PROPS="ingestion.properties";
	private static final String DEFAULT_DATASOURCE_PATH = "datasources"; // local as prefix for Reflections
	
	private static final String LOCAL_TOPOLOGY_NAME = IngestionTopology.class.getName() + "-local_test";

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
	
	public static Config loadConfiguration() {
		PropertiesConfiguration ingestionConfig  = null;
		Map<String, String> datasourcesConfigurations = null;
		String topologyName = null;
		
		LOGGER.info("Loading configuration");
		Config conf = new Config();
		
		try{
			ingestionConfig = new PropertiesConfiguration();
			ingestionConfig.load(new BufferedReader(new InputStreamReader(
						IngestionTopology.class.getResourceAsStream("/" + DEFAULT_INGESTION_PROPS))));
			LOGGER.info("main configuration loaded");
			System.out.println(ingestionConfig.isEmpty());
			datasourcesConfigurations = loadDatasources(DEFAULT_DATASOURCE_PATH);
			LOGGER.info("data sources configuration loaded");
		} catch(ConfigurationException ce) {
			LOGGER.error("There was an error loading configuration, program will stop: " + ce.getMessage());
			ce.printStackTrace();
			System.exit(1);
		}
		
		// add ingestionConfig to conf
		for (Map.Entry<Object, Object> entry : ConfigurationConverter.getMap(ingestionConfig).entrySet()) {
			conf.put(entry.getKey().toString(), entry.getValue());
		}
		
		// add datasourcesConfigurations to conf
		conf.put(DATASOURCE_CONF_KEY, datasourcesConfigurations);
		
		// add topologyName to conf
		topologyName = ingestionConfig.getString("topology.name");
		if (topologyName.equals("")) {
			topologyName = LOCAL_TOPOLOGY_NAME;
		}
		conf.put(Config.TOPOLOGY_NAME, topologyName);
		
		// add other common fields to conf
		// FIXME: use the debug property for this 
		conf.setDebug(true);
		// conf.put(Config.TOPOLOGY_DEBUG, true); // this is not working, this disables even info logs

		LOGGER.info("Done loading configuration");
		
		return conf;
	} 
	
	public static void main(String[] args) {		
		System.out.println("Usage: configuration will be load from " + DEFAULT_INGESTION_PROPS 
								+ "add data sources to " + DEFAULT_DATASOURCE_PATH);
		System.out.println("\tTopology will be executed in parallel iff the property 'topology.name' in " +
				"the ingestion configuration is not empty");
		System.out.println("");

		// Load configuration
		Config conf = loadConfiguration();
		
		// Clear Redis
		// TODO: this is mostly for testing, but maybe should be permanent, think about it
		RestIngestionSpout.clearDb(conf);
		
		// Build topology
		LOGGER.info("Building topology");
		@SuppressWarnings("rawtypes")
		int numDatasources = ((Map) conf.get(IngestionTopology.DATASOURCE_CONF_KEY)).size();
		LOGGER.info("Found {} different datasources", numDatasources);
			// TODO: use a sensible value
		conf.put(Config.TOPOLOGY_WORKERS, 1);
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		// Create a Spout instance / task per data source, to handle only that connection
		topologyBuilder.setSpout(RestIngestionSpout.class.getName(), new RestIngestionSpout(), 
								 numDatasources);
		// We use fieldsGrouping so tuples for the same datasource go to the same AvroWriterBolt, which 
		// opens an HDFS file for the data source and each month. This prevents the situation where two 
		// different  AvroWriterBolts try to open the same file, which would replay the tuple until it
		// goes to the AvroWriterBolt which opened the file first
		// This also implies we need an executor per datasource
		topologyBuilder.setBolt(AvroWriterBolt.class.getName(), new AvroWriterBolt(),
								numDatasources).fieldsGrouping(RestIngestionSpout.class.getName(), 
																new Fields(RestIngestionSpout.DATASOURCE_ID));
								
		// Launch topology
		LOGGER.info("Launching topology");
		if(! conf.get(Config.TOPOLOGY_NAME).equals(LOCAL_TOPOLOGY_NAME)) {
			// run in distributed mode
				// FIXME, but at least as many workers as data sources
			conf.setNumWorkers(numDatasources * 2); 
			try {
				StormSubmitter.submitTopology(conf.get(Config.TOPOLOGY_NAME).toString(), conf, topologyBuilder.createTopology());
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
