package org.collprod.bicingbcn.etl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
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
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;

/**
 * Run to YARN with (start YARN from Cloudera Manager before) 
 * 
[cloudera@localhost spark-1.0.0-bin-cdh4]$ pwd
/home/cloudera/Sistemas/Spark/spark-1.0.0-bin-cdh4
[cloudera@localhost spark-1.0.0-bin-cdh4]$ ./bin/spark-submit --class org.collprod.bicingbcn.etl.EtlStream --master localhost /home/cloudera/git/bicing-bcn/spark/stream-visuals/target/spark-stream-visuals-0.0.1-SNAPSHOT.jar
 * 
 * */

public class EtlStream {

	private static final Logger LOGGER = LoggerFactory.getLogger(EtlStream.class);

	// FIXME: names
	@SuppressWarnings("unchecked")
	private static final List<Pair<String, String>> CONFIGURATIONS = 
			Lists.newArrayList(new Pair<String, String>("main", "visuals.properties"));
	

	private static PropertiesConfiguration loadConfiguration() throws ConfigurationException {
		LOGGER.info("Loading configuration");
		PropertiesConfiguration config = new PropertiesConfiguration();
		for (Pair<String, String> descriptionPath : CONFIGURATIONS) {
			PropertiesConfiguration newConf = new PropertiesConfiguration();
			newConf.load(new BufferedReader(new InputStreamReader(
					EtlStream.class.getResourceAsStream("/" + descriptionPath.right()))));
			config.append(newConf);
			LOGGER.info("Done loading " + descriptionPath.left() + "configuration");
		}
		LOGGER.info("Done loading configuration");
		
		return config;
	}
	
	public static void main(String[] args) throws ConfigurationException {
		// TODO: change project name in maven to spark-stream and regenerate Eclipse project
		
		// Load program configuration
		PropertiesConfiguration config = loadConfiguration();
		
		// Connect to Spark cluster
		JavaStreamingContext jssc = new JavaStreamingContext(config.getString("spark.master"), 
				config.getString("spark.app_name"), 												
				new Duration(config.getLong("spark.batch_duration")));
		
		// FIXME: add to configuration
		// FIXME: although followed http://spark.apache.org/docs/latest/hadoop-third-party-distributions.html, 
		// I get http://qnalist.com/questions/4957822/hdfs-server-client-ipc-version-mismatch-while-trying-to-access-hdfs-files-using-spark-0-9-1
		// http://comments.gmane.org/gmane.comp.lang.scala.spark.user/106
		
		// This doesn't works in local mode (e.g. master="local[2]"), but for example it works in YARN 
		jssc.checkpoint("hdfs://localhost:8020/user/cloudera/bicing/streaming_checkpoints");
		// Works both in local mode (e.g. master="local[2]") and YARN
		// jssc.checkpoint("/home/cloudera/bicing/streaming_checkpoints");
		
		// Connect to Kafka
		Map<String,Integer> kafkaTopics = new HashMap<String, Integer>();
		// See https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/streaming/JavaKafkaWordCount.java
		// that 1 means 1 thread of the Kafka consumer for consuming this topic, TODO put in configuration 
		kafkaTopics.put(config.getString("kafka.bicing_topic"), 2);
		/* - First element is the Kafka partition key (see org.collprod.bicingbcn.ingestion.KafkaWriterBolt to
		 *  check that the partition key is datasource + timestamp.toString(), e.g. test_bicing_station_data1404244632)
		 * - Second element is the message itself
		 */
		JavaPairReceiverInputDStream<String,String> kafkaStream = KafkaUtils.createStream(jssc, 
				config.getString("kafka.zookeeper.quorum"),
				config.getString("kafka.groupid"),
				kafkaTopics);
			
		final Broadcast<BicingStationDao> bicingStationDao = jssc.sparkContext().broadcast(new BicingStationDao());
		
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
				return bicingStationDao.value().parse(kafkakeyMessage._2);
			}
		});
			
		// Group station data per station
		JavaPairDStream<Integer, BicingStationDao.Value> stationStatePairs = 
				bicingStationStates.mapToPair(new PairFunction<BicingStationDao.Value, Integer, BicingStationDao.Value>() {
					private static final long serialVersionUID = 3493170766978489850L;

					@Override
					public Tuple2<Integer, BicingStationDao.Value> call(BicingStationDao.Value stationInfo) throws Exception {
						return new Tuple2<Integer, BicingStationDao.Value>(stationInfo.id(), stationInfo);
					}
				});
		// stationStatePairs.print();
		
		// FIXME hardcoded, load from config and broadcast
		// JDBC driver name and database URL
		final String JDBC_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
		final String DB_URL = "jdbc:phoenix:localhost";
		
		// Note: trusting in Kafka ordering warranties	

		// the state is just an integer with the number of bikes in the station for 
		// the last batch
		JavaPairDStream<Integer, Integer> stateUpdates = stationStatePairs.updateStateByKey(new Function2<List<BicingStationDao.Value>, Optional<Integer>, Optional<Integer>>() {

			private static final long serialVersionUID = 1869498697659731642L;

			/**
			 * @param lastBikeCountState bike count for the most recent station info in the last batch
			 * @param stationsInfo station info instances for the station corresponding to this key, for this batch
			 * 
			 * @return number of stations in the most recent station info of this batch
			 * */
			@Override
			public Optional<Integer> call(List<BicingStationDao.Value> stationsInfo, Optional<Integer> lastBikeCountState) throws Exception {				
				// Prepare DB connection
				
				LOGGER.info("Updating BICING_FACT and BICING_DIM_TIME");
				
				// Register JDBC driver
				Class.forName(JDBC_DRIVER);
				Connection con = DriverManager.getConnection(DB_URL);
				// Statement stmt = con.createStatement();
				String upsertString = "UPSERT INTO BICING_FACT VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
				PreparedStatement stmt = con.prepareStatement(upsertString);
				
				// get last state
				int lastBikeCount = lastBikeCountState.or(-1);
				
				// sort by time stamp and generate a batch of upserts
				List<BicingStationDao.Value> stationsInfoByUpdateTime = new Ordering<BicingStationDao.Value>() {

					@Override
					public int compare(@Nullable Value left, @Nullable Value right) {
						return Longs.compare(left.updatetime(), right.updatetime());
					}
				}.sortedCopy(stationsInfo);
				
//				StringBuffer sqlStmt = new StringBuffer();
//				Joiner joiner = Joiner.on(", ");
				for (BicingStationDao.Value stationInfo : stationsInfoByUpdateTime) {
					// generate upsert for BICING_FACT
//					sqlStmt.setLength(0);
//					sqlStmt.append("UPSERT INTO BICING_FACT VALUES (");
//					sqlStmt.append(
//							joiner.join(stationInfo.id(), 
//									new Timestamp(stationInfo.updatetime()), 
//									"'" + stationInfo.status() + "'", stationInfo.slots(),
//								//  Number of bikes available, 0 if station is not open
//								stationInfo.status().equals("OPN") ? stationInfo.bikes() : 0,
//								// Number of bikes missing, independent of the station being open or closed
//								stationInfo.slots() - stationInfo.bikes(),
//								// Number of bikes lent for this station since the previous update
//									// if lastBikeCount is negative then we don't have info and we return as nothing
//									// if we have more bikes now than in the previous update we assume no bike has been lent,  
//									// this implies an error if bikes are returned and taken between updates
//								(lastBikeCount < 0) ? 0 : Math.max(lastBikeCount - stationInfo.bikes(), 0),
//								// Number of bikes returned to this station since the previous update
//									// same compromises as the previous value
//								(lastBikeCount < 0) ? 0 : Math.max(stationInfo.bikes() - lastBikeCount, 0)
//							)
//					);
//					sqlStmt.append(")");
//					stmt.executeUpdate(sqlStmt.toString());
					stmt.setInt(1, stationInfo.id());
					stmt.setTimestamp(2, new Timestamp(stationInfo.updatetime() * 1000)); // TODO ensure correct translation 
					stmt.setString(3,  stationInfo.status());
					stmt.setInt(4, stationInfo.slots());
					stmt.setInt(5, stationInfo.status().equals("OPN") ? stationInfo.bikes() : 0);
					stmt.setInt(6,  Math.max(stationInfo.slots() - stationInfo.bikes(), 0)); // FIXME: there should be no negative anyway
					stmt.setInt(7, (lastBikeCount < 0) ? 0 : Math.max(lastBikeCount - stationInfo.bikes(), 0));
					stmt.setInt(8, (lastBikeCount < 0) ? 0 : Math.max(stationInfo.bikes() - lastBikeCount, 0));
					stmt.executeUpdate();
					
					// TODO generate upsert for BICING_DIM_TIME
					
					// update state
					lastBikeCount = stationInfo.bikes();
				}
				/*
				 * CREATE TABLE IF NOT EXISTS BICING_FACT (
    -- FK for BICING_DIM_STATION
    STATION UNSIGNED_LONG NOT NULL,
    -- NOTE: remember bicing ingests UNIX time in seconds
    -- FK for BICING_DIM_TIME
    TIMETAG TIMESTAMP NOT NULL,
    -- Just a few fact, all in the same HBase column
    -- Degenerate dimension: 'OPN' (open) or 'CLS' (closed)
    F.STATUS VARCHAR(3),
    -- Fact fields
    -- Number of slots in the station
    F.SLOTS UNSIGNED_LONG,
    -- Number of bikes available: should be 0 
    -- if F.STATUS is "CLS"
    F.AVAILABLE UNSIGNED_LONG,
    -- Number of bikes missing: F.SLOTS - number of bikes
    -- available independently of F.STATUS 
    F.MISSING UNSIGNED_LONG,
    -- Number of bikes lent for this station since 
    -- the previous update
    F.LENT UNSIGNED_LONG,
    -- Number of bikes returned to this station since 
    -- the previous update
    F.RETURNED UNSIGNED_LONG
    -- This gives a non monotonically increasing row key
    CONSTRAINT PK PRIMARY KEY (STATION, TIMETAG)
);
				 * */
				
				
				// Commit and close DB connection
				con.commit();
				con.close();

				// TODO: update BICING_DIM_TIME too
				 
				// TODO: check state is updated
				 
				return Optional.of(lastBikeCount);
			}	
		});
		
		stateUpdates.print(); // force evaluation 
		
//		stationStatePairs.print();
		
		// Note: trusting in Kafka ordering warranties
		
		/*
		 * HERE:
		 * - use updateStateByKey to generate a JavaPairDStream using the number of bikes in the last batch 
		 * as state. In case a more complex state is needed use an autovalue
		 * - in foreachRDD generate a pairRDD using the timestamp and use rdd.sortByKey() so station updates
		 * in this batch are treated in order. This together with Kafka order warranties should be enough
		 * 
		 * */
		
//		@AutoValue
//		public static abstract class EtlState implements Serializable {
//			public static EtlState create(int lastNumBikes)
//			
//			EtlState() {}
//			public abstract int lastNumBikes();
//		}
		/*
		 * 

@AutoValue
	public static abstract class Value implements Serializable {
		// generated by Eclipse
		private static final long serialVersionUID = 8581922474241143038L;
		
		Value() {}
		public static Value create(long updatetime, int id, double latitude,
				double longitude, String street, int height, int streetNumber,
				ArrayList<Integer> nearbyStationList, String status, int slots,
				int bikes) {
	        return new AutoValue_BicingStationDao_Value(updatetime, id, latitude,
					longitude, street, height, streetNumber, nearbyStationList, status, 
					slots, bikes);
	      }
		public abstract long updatetime();
		public abstract int id();
		public abstract double latitude();
		public abstract double longitude();
		public abstract String street();
		public abstract int height();
		public abstract int streetNumber();
		// NOTE: it's essential to use ArrayList instead of List, otherwise this class
		// won't be Serializable and thus NotSerializableException when used in Window operations
		public abstract ArrayList<Integer> nearbyStationList();
		public abstract String status();
		public abstract int slots();
		public abstract int bikes();
	}
*/
		
		// This is working writing to Phoenix
//		bicingStationStates.foreachRDD(new Function<JavaRDD<BicingStationDao.Value>, Void>() {
//			
//			private static final long serialVersionUID = 5414279565636816286L;
//
//			@Override
//			public Void call(JavaRDD<BicingStationDao.Value> stationsData) throws Exception {
//				stationsData.mapPartitions(new FlatMapFunction<Iterator<BicingStationDao.Value>, Void>() {
//					private static final long serialVersionUID = -5770319181680664525L;
//
//					@Override
//					public Iterable<Void> call(Iterator<BicingStationDao.Value> stationData) throws Exception {
//						LOGGER.info("writing to Phoenix!");
//						// Register JDBC driver
//						Class.forName(JDBC_DRIVER);
//						Connection con = DriverManager.getConnection(DB_URL);
//					    // FIXME harcoded phoenix host
////						Connection con = DriverManager.getConnection("jdbc:phoenix:[localhost]");
//						Statement stmt = con.createStatement();
//						stmt.executeUpdate("UPSERT INTO BICING_DIM_STATION VALUES (26, 2.18198100, 41.4070350, 28, 'Eixample', 'Sagrada Família', '08025', 'Dos Maig 230', 35678.34, 266874, 7.48)");
//						con.commit();
//						con.close();
//						return new ArrayList<Void>();
//					}
//				}).count();
//								
//				return null;
//			}
//		});
		
		
//		bicingStationStates.print();
		
		// Start Spark stream and await for termination
		jssc.start();
		jssc.awaitTermination();
		
	}

}
