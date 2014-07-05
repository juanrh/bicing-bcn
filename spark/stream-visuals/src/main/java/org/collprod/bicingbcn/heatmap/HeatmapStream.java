package org.collprod.bicingbcn.heatmap;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.http.client.fluent.Request;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.collprod.bicingbcn.BicingStationDao;
import org.collprod.bicingbcn.BicingStationDao.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.atlassian.fugue.Pair;
import com.google.common.collect.Lists;

// See https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/KafkaWordCount.scala
public class HeatmapStream {
	// TODO use config
	private static final Logger LOGGER = LoggerFactory.getLogger(HeatmapStream.class);
	
	private static final String MAIN_PROPS="visuals.properties";
	private static final String CARTODB_PROPS = "cartodb.properties";
	/**
	 * List of pairs <description> - <properties file path> 
	 * */
	private static final List<Pair<String, String>> CONFIGURATIONS = 
			Lists.newArrayList(new Pair<String, String>("main", "visuals.properties"),
							  new Pair<String, String>("CartoDB", "cartodb.properties"));
	
	/**
	 * BicingDao for converting bicing XML file contents into stations data objects 
	 * 
	 * Following http://apache-spark-user-list.1001560.n3.nabble.com/Database-connection-per-worker-td1280.html
	 */
	private static BicingStationDao sharedBicingStationDao = new BicingStationDao();
	private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
	
//	1970-01-01T00:00:00Z
	
	/**
	 * Program configuration
	 * */
	private PropertiesConfiguration config;
	
	private void loadConfiguration() throws ConfigurationException {
		LOGGER.info("Loading configuration");
		this.config = new PropertiesConfiguration();
		for (Pair<String, String> descriptionPath : CONFIGURATIONS) {
			PropertiesConfiguration newConf = new PropertiesConfiguration();
			newConf.load(new BufferedReader(new InputStreamReader(
							HeatmapStream.class.getResourceAsStream("/" + descriptionPath.right()))));
			this.config.append(newConf);
			LOGGER.info("Done loading " + descriptionPath.left() + "configuration");
		}
		LOGGER.info("Done loading configuration");
	}
	
	// TODO JodaTime could be more efficient / convenient
	// TODO reusing the string buffer in a member method would be more efficient probably 
	public static Request valueToCartoDBStationsStateTable(BicingStationDao.Value stationValue, 
			String cartoDbSqlApiPrefix, String cartoDBSQLApiKey) {
		/* http://juanrh.cartodb.com/api/v2/sql?q=INSERT INTO station_state 
		(updatetime, the_geom, status) 
		VALUES ('1970-01-01', ST_GeomFromText('POINT(41.397953 2.180042)', 4326), 'OPN')&api_key=<API key>
		*/
		StringBuffer requestUrl = new StringBuffer(cartoDbSqlApiPrefix);
		requestUrl.append("?q=INSERT INTO station_station (updatetime, the_geom, status) ");
		String updatetime = DATE_FORMATTER.format(new Date(stationValue.updatetime() * 1000L));
		requestUrl.append("VALUES ('"); requestUrl.append(updatetime); requestUrl.append("', ");
		requestUrl.append("ST_GeomFromText('POINT(");requestUrl.append(stationValue.longitude());
		requestUrl.append(" "); requestUrl.append(stationValue.latitude()); requestUrl.append(")', 4326)");
		requestUrl.append("'");requestUrl.append(stationValue.status()); requestUrl.append("')");
		requestUrl.append("&api_key=");requestUrl.append(cartoDBSQLApiKey);
				
		return Request.Get(requestUrl.toString());
	}
	
	public static void main(String[] args) throws ConfigurationException {
		// Load program configuration
		HeatmapStream heatmapStream = new HeatmapStream();
		heatmapStream.loadConfiguration();
		
		final String cartoDBApiKey = heatmapStream.config.getString("cartodb.apikey");
		final String cartoDbSqlApiPrefix = heatmapStream.config.getString("cartodb.sql_api_prefix");
			
		// Connect to Spark cluster
		JavaStreamingContext jssc = new JavaStreamingContext(heatmapStream.config.getString("spark.master"), 
							heatmapStream.config.getString("spark.app_name"), 												
							new Duration(heatmapStream.config.getLong("spark.batch_duration")));
		
		// example of using an Spark broadcast variable from spark streaming 
		JavaSparkContext jsc = jssc.sparkContext();
		
		/* 
		 * Alternative to HeatmapStream.sharedBicingStationDao
		 * 
		 * Each node uses its own copy, this works in local mode
		 * Nevertheless this is a bad practive as "the object v should not be modified after it
		 * is broadcast in order to ensure that all nodes get the same value of the broadcast variable 
		 * (e.g. if the variable is shipped to a new node later)." (http://spark.apache.org/docs/latest/api/java/org/apache/spark/broadcast/Broadcast.html)
		 * Although in this case there is no problem, maybe this technique would not be suitable for database 
		 * connection  as it could throw away data locality */
		// final Broadcast<BicingStationDao> sharedBicingStationDao = jsc.broadcast(new  BicingStationDao());

		// Connect to Kafka
		Map<String,Integer> kafkaTopics = new HashMap<String, Integer>();
		kafkaTopics.put(heatmapStream.config.getString("kafka.bicing_topic"), 1);
		/* - First element is the Kafka partition key (see org.collprod.bicingbcn.ingestion.KafkaWriterBolt to
		 *  check that the partition key is datasource + timestamp.toString(), e.g. test_bicing_station_data1404244632)
		 * - Second element is the message itself
		*/
		JavaPairReceiverInputDStream<String,String> kafkaStream = KafkaUtils.createStream(jssc, 
				heatmapStream.config.getString("kafka.zookeeper.quorum"),
				heatmapStream.config.getString("kafka.groupid"),
				kafkaTopics);
		
		// Parse XML data into BicingStationDao.Value objects
		JavaDStream<BicingStationDao.Value> bicingStationValues = kafkaStream.flatMap(new FlatMapFunction<Tuple2<String, String>, BicingStationDao.Value>() {
			// generated by Eclipse
			private static final long serialVersionUID = -164175401233776623L;

			/**
			 * - First element is the Kafka partition key (see org.collprod.bicingbcn.ingestion.KafkaWriterBolt to
			 * check that the partition key is datasource + timestamp.toString(), e.g. test_bicing_station_data1404244632)
			 * - Second element is the message itself
			 * */
			@Override
			public Iterable<Value> call(Tuple2<String, String> kafkakeyMessage)
					throws Exception {
				// return sharedBicingStationDao.value().parse(kafkakeyMessage._2); // works in local mode
				return HeatmapStream.sharedBicingStationDao.parse(kafkakeyMessage._2);
			}
		});
		
		// Send data to CartoDB station_state table
		bicingStationValues.foreachRDD(new Function<JavaRDD<BicingStationDao.Value>, Void>(){
			// Generated by Eclipse
			private static final long serialVersionUID = -5584974315184238995L;

			@Override
			public Void call(JavaRDD<BicingStationDao.Value> valuesRDD) throws Exception {
				valuesRDD.foreach(new VoidFunction<BicingStationDao.Value>(){
					// Generated by Eclipse
					private static final long serialVersionUID = -8663136695628471792L;

					@Override
					public void call(BicingStationDao.Value stationValue) throws Exception {
						int responseStatusCode = HeatmapStream.valueToCartoDBStationsStateTable(stationValue, cartoDbSqlApiPrefix, cartoDBApiKey)
													.execute().returnResponse().getStatusLine().getStatusCode();
						// TODO: if different to 200 then error instead of ingo
						// TODO: test request separately
						System.out.println("Sent record to CartoDB stations_state table, response code " + responseStatusCode);
						LOGGER.info("Sent record to CartoDB stations_state table, response code {}", responseStatusCode);
					}
					
				});
				return null;
			}
			
		});
		
		
//		 Request valueToCartoDBStateTable(BicingStationDao.Value stationValue, 
//					String cartoDbSqlApiPrefix, String cartoDBSQLApiKey)
//		// Request.Get(newProperties.getString("datasource_url")),
//		newData = datasourceState.request().execute().returnContent().asString();

		
		/*
		 * Example insert in cartoDB:
		 * 
		 * As a GET
		 * http://juanrh.cartodb.com/api/v2/sql?q=INSERT INTO station_state (updatetime, the_geom, status) VALUES ('1970-01-01', ST_GeomFromText('POINT(41.397953 2.180042)', 4326), 'OPN')&api_key=<API key>
		 * 
		 * */
		
		
		// FIXME
//		bicingStationValues.print();
		
		// Start Spark stream and await for termination
		jssc.start();
		jssc.awaitTermination();
	}
}
