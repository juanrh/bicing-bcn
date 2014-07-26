package org.collprod.bicingbcn.heatmap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.collprod.bicingbcn.BicingStationDao;
import org.collprod.bicingbcn.BicingStationDao.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.atlassian.fugue.Pair;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

/**
 * This code is still under development 
 * */
/*
 * TODO: no need to use a window, as we just need the last value of the batch RDD ==> change as that has a 
 * huge impact in the performance
 * */
// See https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/KafkaWordCount.scala
public class HeatmapStream {
	// TODO use config
	private static final Logger LOGGER = LoggerFactory.getLogger(HeatmapStream.class);
	
	/**
	 * List of pairs <description> - <properties file path> 
	 * */
	@SuppressWarnings("unchecked")
	private static final List<Pair<String, String>> CONFIGURATIONS = 
			Lists.newArrayList(new Pair<String, String>("main", "visuals.properties"),
							  new Pair<String, String>("CartoDB", "cartodb.properties"));
	
	/**
	 * Program configuration
	 * */
	private static PropertiesConfiguration config;
	
	private static URIBuilder cartoDBCommonURI = null;
	private static String cartoDbSqlApiScheme;
	private static String cartoDbSqlApiHost;
	private static String cartoDbSqlApiPath;
	private static String cartoDBApiKey; 
	
	/**
	 * BicingDao for converting bicing XML file contents into stations data objects 
	 * 
	 * Following http://apache-spark-user-list.1001560.n3.nabble.com/Database-connection-per-worker-td1280.html
	 */
	private static BicingStationDao sharedBicingStationDao = new BicingStationDao();
//	private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
	
	private static void loadConfiguration() throws ConfigurationException {
		LOGGER.info("Loading configuration");
		HeatmapStream.config = new PropertiesConfiguration();
		for (Pair<String, String> descriptionPath : CONFIGURATIONS) {
			PropertiesConfiguration newConf = new PropertiesConfiguration();
			newConf.load(new BufferedReader(new InputStreamReader(
							HeatmapStream.class.getResourceAsStream("/" + descriptionPath.right()))));
			config.append(newConf);
			LOGGER.info("Done loading " + descriptionPath.left() + "configuration");
		}
		cartoDbSqlApiScheme = config.getString("cartodb.sql_api.scheme");
		cartoDbSqlApiHost = config.getString("cartodb.sql_api.host");
		cartoDbSqlApiPath = config.getString("cartodb.sql_api.path");
		cartoDBApiKey = config.getString("cartodb.apikey");
		
		LOGGER.info("Done loading configuration");
	}
	

	public static URIBuilder getCartoDBCommonURI() {
		if (cartoDBCommonURI == null) {
			cartoDBCommonURI = new URIBuilder().setScheme(cartoDbSqlApiScheme).setHost(cartoDbSqlApiHost)
					.setPath(cartoDbSqlApiPath)
					.setParameter("api_key", cartoDBApiKey);
		}
		
		return cartoDBCommonURI;
	}
	
	// TODO reusing the string buffer in a member method would be more efficient probably 
	public static Request insertValueToCartoDBStationsStateTable(BicingStationDao.Value stationValue) throws URISyntaxException {
		/* http://juanrh.cartodb.com/api/v2/sql?q=INSERT INTO station_state (the_geom, status) 
		 * VALUES (ST_GeomFromText('POINT(2.180042 41.397952)', 4326),'OPN')&api_key=<API key>
		*/
		StringBuffer qParam = new StringBuffer("INSERT INTO station_state (the_geom, status) ");
		qParam.append(" VALUES (ST_GeomFromText('POINT(");
		qParam.append(stationValue.longitude()); qParam.append(" "); qParam.append(stationValue.latitude()); 
		qParam.append(")', 4326),"); 
		qParam.append("'");qParam.append(stationValue.status()); qParam.append("')");
		
		URI requestUri = getCartoDBCommonURI().setParameter("q", qParam.toString()).build();
		
		return Request.Get(requestUri);
	}

	public static void executeInsertValueToCartoDBStationsStateTable(BicingStationDao.Value stationValue) throws URISyntaxException, ClientProtocolException, IOException {
		// WARNING: don't log the request, it contains the credentials
		Request cartoDBInsertRequest = HeatmapStream.insertValueToCartoDBStationsStateTable(stationValue);
		LOGGER.info("Sending data to CartoDB:stations_state");
		int responseStatusCode = cartoDBInsertRequest.execute().returnResponse().getStatusLine().getStatusCode();
		if (responseStatusCode == 200) {
			LOGGER.info("Success sending data to CartoDB:stations_state");
		} else {
			LOGGER.error("Response code {} sending data to CartoDB:stations_state", responseStatusCode);
		}
	}
	
	public static Request clearCartoDBStationsStateTable() throws URISyntaxException {
		URI requestUri = getCartoDBCommonURI().setParameter("q", "DELETE FROM station_state").build();
		return Request.Get(requestUri);
	}	
	
	
	public static Request updateCartoDBStationsStateTable(Iterable<BicingStationDao.Value> stationValues) throws URISyntaxException, ClientProtocolException, IOException {	
		// see http://blog.cartodb.com/post/53301057653/faster-data-updates-with-cartodb
		StringBuffer qParam = new StringBuffer("WITH n(the_geom, status, num_available_bikes) AS (VALUES ");
		qParam.append(
				Joiner.on(",").join(FluentIterable.from(stationValues)
						              .transform(new com.google.common.base.Function<BicingStationDao.Value, String>() {
			@Override
			@Nullable
			public String apply(@Nullable BicingStationDao.Value stationValue) {
				StringBuffer buffer = new StringBuffer();
				buffer.append("(ST_GeomFromText('POINT(");
				buffer.append(stationValue.longitude()); buffer.append(" "); buffer.append(stationValue.latitude()); 
				buffer.append(")', 4326),"); 
				buffer.append("'"); buffer.append(stationValue.status()); buffer.append("'");
				buffer.append(", "); buffer.append(stationValue.bikes()); buffer.append(")"); 
				
				return buffer.toString(); 
			}
		})));
		qParam.append("), ");
		// update existing rows
		qParam.append("upsert AS ( UPDATE station_state o ");
		qParam.append("SET the_geom=n.the_geom, status=n.status, num_available_bikes=n.num_available_bikes");
		qParam.append(" FROM n WHERE o.the_geom = n.the_geom");
		qParam.append(" RETURNING o.the_geom )");
		// insert missing rows
		qParam.append(" INSERT INTO station_state (the_geom, status, num_available_bikes)" );
		qParam.append(" SELECT n.the_geom, n.status, n.num_available_bikes FROM n");
		qParam.append(" WHERE n.the_geom NOT IN (SELECT the_geom FROM upsert)");
		
		URI requestUri = HeatmapStream.getCartoDBCommonURI().setParameter("q", qParam.toString()).build();
		return Request.Get(requestUri);
	}
	
	public static void executeupdateCartoDBStationsStateTable(Iterable<BicingStationDao.Value> stationValues) throws URISyntaxException, ClientProtocolException, IOException {
		// WARNING: don't log the request, it contains the credentials
		Request cartoDBRequest = HeatmapStream.updateCartoDBStationsStateTable(stationValues);
		LOGGER.info("Sending data to CartoDB:stations_state");
		
		int responseStatusCode;
		try {
			responseStatusCode = cartoDBRequest.execute().returnResponse().getStatusLine().getStatusCode();
			if (responseStatusCode == 200) {
				LOGGER.info("Success sending data to CartoDB:stations_state");
			}
			 else {
				LOGGER.error("Response code {} sending data to CartoDB:stations_state", responseStatusCode);
			}
		} catch (ClientProtocolException cpe) {
			LOGGER.error("Exception sending data to CartoDB:stations_state {}", ExceptionUtils.getStackTrace(cpe));
		} catch (IOException ioe) {
			LOGGER.error("Exception sending data to CartoDB:stations_state {}", ExceptionUtils.getStackTrace(ioe));
		}
	}
	
	
	// FIXME: common code with executeInsertValueToCartoDBStationsStateTable
	/**
	 * @return true iff the process was executed with success
	 * */
	public static boolean executeClearCartoDBStationsStateTable() throws URISyntaxException, ClientProtocolException, IOException {
		// WARNING: don't log the request, it contains the credentials

		Request cartoDBInsertRequest = HeatmapStream.clearCartoDBStationsStateTable();
		LOGGER.info("Clearing table CartoDB:stations_state");
		int responseStatusCode = cartoDBInsertRequest.execute().returnResponse().getStatusLine().getStatusCode();
		if (responseStatusCode == 200) {
			LOGGER.info("Success clearing table CartoDB:stations_state");
		} else {
			LOGGER.error("Response code {} clearing table CartoDB:stations_state", responseStatusCode);
		}
		return (responseStatusCode == 200);
	}
	
	public static void main(String[] args) throws ConfigurationException {
		// Load program configuration
		HeatmapStream.loadConfiguration();	
		
		// Connect to Spark cluster
		JavaStreamingContext jssc = new JavaStreamingContext(HeatmapStream.config.getString("spark.master"), 
							HeatmapStream.config.getString("spark.app_name"), 												
							new Duration(HeatmapStream.config.getLong("spark.batch_duration")));
		
		// jssc.checkpoint("sparkCheckpoint");
		// example of using an Spark broadcast variable from spark streaming 
//		JavaSparkContext jsc = jssc.sparkContext();
		
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
		// See https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/streaming/JavaKafkaWordCount.java
		// that 1 means 1 thread of the Kafka consumer for consuming this topic, TODO put in configuration 
		kafkaTopics.put(HeatmapStream.config.getString("kafka.bicing_topic"), 2);
		/* - First element is the Kafka partition key (see org.collprod.bicingbcn.ingestion.KafkaWriterBolt to
		 *  check that the partition key is datasource + timestamp.toString(), e.g. test_bicing_station_data1404244632)
		 * - Second element is the message itself
		*/
		JavaPairReceiverInputDStream<String,String> kafkaStream = KafkaUtils.createStream(jssc, 
				HeatmapStream.config.getString("kafka.zookeeper.quorum"),
				HeatmapStream.config.getString("kafka.groupid"),
				kafkaTopics);
		
		// Parse XML data into BicingStationDao.Value objects
		JavaDStream<BicingStationDao.Value> bicingStationStates = kafkaStream.flatMap(new FlatMapFunction<Tuple2<String, String>, BicingStationDao.Value>() {
			// generated by Eclipse
			private static final long serialVersionUID = -164175401233776623L;

			/**
			 * - First element is the Kafka partition key (see org.collprod.bicingbcn.ingestion.KafkaWriterBolt to
			 * check that the partition key is datasource + timestamp.toString(), e.g. test_bicing_station_data1404244632)
			 * - Second element is the message itself
			 * */
			@Override
			public Iterable<BicingStationDao.Value> call(Tuple2<String, String> kafkakeyMessage)
					throws Exception {
				// return sharedBicingStationDao.value().parse(kafkakeyMessage._2); // works in local mode
				return HeatmapStream.sharedBicingStationDao.parse(kafkakeyMessage._2);
			}
		});
				
		JavaPairDStream<Integer, BicingStationDao.Value> idStatePairs = bicingStationStates.mapToPair(new PairFunction<BicingStationDao.Value, Integer, BicingStationDao.Value>() {

			private static final long serialVersionUID = 4808961521304738950L;

			@Override
			public Tuple2<Integer, BicingStationDao.Value> call(BicingStationDao.Value bicingStationValue) throws Exception {
				return new Tuple2<Integer, BicingStationDao.Value>(bicingStationValue.id(), bicingStationValue);
			}
		});
		
		/*
		 * TODO: no need to use a window, as we just need the last value of the batch RDD ==> change as that has a 
		 * huge impact in the performance
		 * */

		// Get just last status in the window
		// TODO: change durations to 60000 and 30000
		JavaPairDStream<Integer, BicingStationDao.Value> lastStatePairs = idStatePairs.reduceByKeyAndWindow(new Function2<BicingStationDao.Value, BicingStationDao.Value, BicingStationDao.Value>() {

			private static final long serialVersionUID = -4248339957716659672L;

			@Override
			public BicingStationDao.Value call(BicingStationDao.Value state1, BicingStationDao.Value state2) throws Exception {
				// take the newest value in the window
				return (state1.updatetime() > state2.updatetime() ? state1 : state2);				
			}
		}, new Duration(60000), new Duration(90000));
		// new Duration(300), new Duration(100));
			
		lastStatePairs.foreachRDD(new Function<JavaPairRDD<Integer,BicingStationDao.Value>, Void>() {
			private static final long serialVersionUID = 3551715203820036276L;
			@Override
			public Void call(JavaPairRDD<Integer, BicingStationDao.Value> rdd) throws Exception {
				// This is a pattern in the code examples that come with Spark, I guess it's
				// common to have empty batches from time to time
				if (rdd.count() > 0) {
					HeatmapStream.executeupdateCartoDBStationsStateTable(
							FluentIterable.from(rdd.collect())
							.transform(new com.google.common.base.Function<Tuple2<Integer, BicingStationDao.Value>, BicingStationDao.Value>(){
								@Override
								@Nullable
								public BicingStationDao.Value apply(
										@Nullable Tuple2<Integer, BicingStationDao.Value> input) {
									// TODO Auto-generated method stub
									return input._2;
								}
							})
						);
				} 
				return null;
			}
		});
//		
//		lastStatePairs.print();
		
//		// Send data to CartoDB station_state table
//		// TODO: optimize with a single SQL batch transaction instead one per station 
//		bicingStationValues.foreachRDD(new Function<JavaRDD<BicingStationDao.Value>, Void>(){
//			// Generated by Eclipse
//			private static final long serialVersionUID = -5584974315184238995L;
//
//			@Override
//			public Void call(JavaRDD<BicingStationDao.Value> valuesRDD) throws Exception {
//				// This is a pattern in the examples, I guess it's common to have 
//				// empty batches from time to time
//				if (valuesRDD.count() > 0) {
//					List<BicingStationDao.Value> values = valuesRDD.collect();
//					System.out.println("\n\n\n\n\n" + values +"\n\n\n\n\n" );
//				}
////				
////				// Clear the table
////				boolean clearTableOK = HeatmapStream.executeClearCartoDBStationsStateTable();
////				
////				long count = values.size();
////				LOGGER.info("Found data for {} bicing stations", count);
////				System.out.println("\n\n\n\n\n" + "Found data for "  + count + " bicing stations" + "\n\n\n\n\n");
//
//				// If ok then insert the current values in the table. Do nothing on else, 
//				// HeatmapStream.executeClearCartoDBStationsStateTable() already
//				// writes a message
////				if (clearTableOK) {
////					// Populate the table with the new values					
////					valuesRDD.foreach(new VoidFunction<BicingStationDao.Value>(){
////						// Generated by Eclipse
////						private static final long serialVersionUID = -8663136695628471792L;
////
////						@Override
////						public void call(BicingStationDao.Value stationValue) throws Exception {
////							HeatmapStream.executeInsertValueToCartoDBStationsStateTable(stationValue);
////						}
////					});
////				} 
//				return null;
//			}
//
//		});
			
		
		// FIXME
//		bicingStationValues.print();
		
		// Start Spark stream and await for termination
		jssc.start();
	
		
		
		
		jssc.awaitTermination();
	}
}
