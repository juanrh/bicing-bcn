package org.collprod.bicingbcn.etl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
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

	@SuppressWarnings("unchecked")
	private static final List<Pair<String, String>> CONFIGURATIONS = 
			Lists.newArrayList(new Pair<String, String>("main", "main.properties"));	

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
	
	/**
	Computing the new values for the fact table and the time dimension, and inserting in Phoenix. 
	Using a state per station, the state is just an integer with the number of bikes in the station for 
	the last batch, and its used to compute the number of bikes lent and returned 
	
	NOTE: trusting in Kafka ordering warranties, also ordering by timetag in each batch RDD		
	NOTE: due to Spark Streaming design there is some constraints in the size of the batches used for
		  checkpointed streams like those using updateStateByKey like this one, which implies a batch 
		  size of at least 10 seconds (http://spark.apache.org/docs/latest/streaming-programming-guide.html#persistence
		  "For DStreams that must be checkpointed (that is, DStreams created by updateStateByKey and 
		  reduceByKeyAndWindow with inverse function), the checkpoint interval of the DStream is by 
		  default set to a multiple of the DStream’s sliding interval such that its at least 10 seconds.")
	 */
	private static void updateBicingStar(final Broadcast<String> phoenixJdbcDriver, final Broadcast<String> phoenixDbUrl, 
								  final Broadcast<PhoenixWriter> phoenixWriter, JavaPairDStream<Integer, BicingStationDao.Value> stationStatePairs) {
		
		JavaPairDStream<Integer, Integer> stateUpdates = stationStatePairs.updateStateByKey(new Function2<List<BicingStationDao.Value>, Optional<Integer>, Optional<Integer>>() {

			private static final long serialVersionUID = 1869498697659731642L;

			/**
			 * @param lastBikeCountState bike count for the most recent station info in the last batch
			 * @param stationsInfo station info instances for the station corresponding to this key, for this batch
			 * 
			 * @return number of stations in the most recent station info of this batch
			 * 
			 * */
			@Override
			public Optional<Integer> call(List<BicingStationDao.Value> stationsInfo, Optional<Integer> lastBikeCountState) throws Exception {				
				// Prepare DB connection
					// Register JDBC driver
				try {
					Class.forName(phoenixJdbcDriver.getValue());
				} catch (ClassNotFoundException cne) {
					LOGGER.error("Error loading Phoenix driver {}", ExceptionUtils.getFullStackTrace(cne));
					// FIXME: use custom bicing exception class
					throw new RuntimeException(cne);
				}
				
				// get last state
				int lastBikeCount = lastBikeCountState.or(-1);
				
				// sort by time stamp and generate a batch of upserts
				List<BicingStationDao.Value> stationsInfoByUpdateTime = new Ordering<BicingStationDao.Value>() {

					@Override
					public int compare(@Nullable Value left, @Nullable Value right) {
						return Longs.compare(left.updatetime(), right.updatetime());
					}
				}.sortedCopy(stationsInfo);
				
				Connection dbConnection = null;
				PreparedStatement stmtBicingFact = null;
				PreparedStatement stmtBicingDimTime = null;
				// PreparedStatement stmtCheckExistsTimetagDimTime = null;
				
				try  {
					// http://docs.oracle.com/javase/tutorial/jdbc/basics/prepared.html
					dbConnection = DriverManager.getConnection(phoenixDbUrl.getValue());
					dbConnection.setAutoCommit(true);

					stmtBicingFact = phoenixWriter.getValue().buildBicingFactStatement(dbConnection); 
					stmtBicingDimTime = phoenixWriter.getValue().buildBicingDimTime(dbConnection);
					// stmtCheckExistsTimetagDimTime = phoenixWriter.getValue().buildCheckExistsTimetagDimTime(dbConnection);
								
					for (BicingStationDao.Value stationInfo : stationsInfoByUpdateTime) {				
						// generate and execute an upsert for BICING_FACT
						LOGGER.info("Updating table BICING_FACT");
						phoenixWriter.getValue().loadBicingFactStatement(stationInfo, lastBikeCount, stmtBicingFact);							
						stmtBicingFact.executeUpdate();
						
						// generate upsert for BICING_DIM_TIME
						// Note the same timetag will be present for several stations. The update is deterministic
						// so we perform it as many times as needed because we assume that the cost of checking
						// whether it was performed or not and only updating if not is higher, because several
						// nodes of the spark cluster will be processing different stationInfo values for the same 
						// timetag and different stations at the same time, as these will correspond to the 
						// same source bicing XML						
						LOGGER.info("Updating table BICING_DIM_TIME");
						phoenixWriter.getValue().loadBicingDimTimeStatement(stationInfo, stmtBicingDimTime);
						stmtBicingDimTime.executeUpdate();
						/*
						 * Alternative that only updates if needed
						 * 
						 * if (EtlStream.checkExistsTimetagDimTime(stationInfo, stmtCheckExistsTimetagDimTime)) {
						 
						LOGGER.info("Row for timetag {} in BICING_DIM_TIME already exists", stationInfo.updatetime());
						} else {
							EtlStream.loadBicingDimTimeStatement(stationInfo, stmtBicingDimTime);
							stmtBicingDimTime.executeUpdate();
						}*/
						
						// Commit to database both updates
						dbConnection.commit();
						
						// update state
						lastBikeCount = stationInfo.bikes();
					}
				} catch(SQLException se)  {
					LOGGER.error("Error updating tables BICING_FACT and BICING_DIM_TIME for stationInfo {}, {}", stationsInfo, ExceptionUtils.getStackTrace(se));
					// FIXME: use custom bicing exception class
					throw new RuntimeException(se);
				}
				
				finally {
					// Close DB resources
					if (stmtBicingFact != null) {
						stmtBicingFact.close();
					}
					/*
					if (stmtCheckExistsTimetagDimTime != null) {
						stmtCheckExistsTimetagDimTime.close();
					} */
					if (stmtBicingDimTime != null) {
						stmtBicingDimTime.close();
					}
					if (dbConnection != null) {
						dbConnection.close();
					}
				}
				 
				return Optional.of(lastBikeCount);
			}	
		});
		
		stateUpdates.print(); // force evaluation, this is essential for this code to have side effects!
	}
	
	/**
	Computing the new values for the fact table and the time dimension, and inserting in Phoenix. 
	Using a state per station, the state is just an integer with the number of bikes in the station for 
	the last batch, and its used to compute the number of bikes lent and returned 
	
	NOTE: trusting in Kafka ordering warranties, also ordering by timetag in each batch RDD		
	NOTE: due to Spark Streaming design there is some constraints in the size of the batches used for
		  checkpointed streams like those using updateStateByKey like this one, which implies a batch 
		  size of at least 10 seconds (http://spark.apache.org/docs/latest/streaming-programming-guide.html#persistence
		  "For DStreams that must be checkpointed (that is, DStreams created by updateStateByKey and 
		  reduceByKeyAndWindow with inverse function), the checkpoint interval of the DStream is by 
		  default set to a multiple of the DStream’s sliding interval such that its at least 10 seconds.")
	 */
	private static void updateBicingBigTable(final Broadcast<String> phoenixJdbcDriver, final Broadcast<String> phoenixDbUrl, 
								  final Broadcast<PhoenixWriter> phoenixWriter, JavaPairDStream<Integer, BicingStationDao.Value> stationStatePairs) {
		
		JavaPairDStream<Integer, Integer> stateUpdates = stationStatePairs.updateStateByKey(new Function2<List<BicingStationDao.Value>, Optional<Integer>, Optional<Integer>>() {

			private static final long serialVersionUID = 1869498697659731642L;

			/**
			 * @param lastBikeCountState bike count for the most recent station info in the last batch
			 * @param stationsInfo station info instances for the station corresponding to this key, for this batch
			 * 
			 * @return number of stations in the most recent station info of this batch
			 * 
			 * */
			@Override
			public Optional<Integer> call(List<BicingStationDao.Value> stationsInfo, Optional<Integer> lastBikeCountState) throws Exception {				
				// Prepare DB connection
					// Register JDBC driver
				try {
					Class.forName(phoenixJdbcDriver.getValue());
				} catch (ClassNotFoundException cne) {
					LOGGER.error("Error loading Phoenix driver {}", ExceptionUtils.getFullStackTrace(cne));
					// FIXME: use custom bicing exception class
					throw new RuntimeException(cne);
				}
				
				// get last state
				int lastBikeCount = lastBikeCountState.or(-1);
				
				// sort by time stamp and generate a batch of upserts
				List<BicingStationDao.Value> stationsInfoByUpdateTime = new Ordering<BicingStationDao.Value>() {

					@Override
					public int compare(@Nullable Value left, @Nullable Value right) {
						return Longs.compare(left.updatetime(), right.updatetime());
					}
				}.sortedCopy(stationsInfo);
				
				Connection dbConnection = null;
				PreparedStatement stmtGetStationInfo = null;
				PreparedStatement stmtUpsertBicingBigTableStatement= null;
		
				try  {
					// http://docs.oracle.com/javase/tutorial/jdbc/basics/prepared.html
					dbConnection = DriverManager.getConnection(phoenixDbUrl.getValue());
					dbConnection.setAutoCommit(true);
					
					stmtGetStationInfo = phoenixWriter.getValue().buildLookupStationStatement(dbConnection);
					stmtUpsertBicingBigTableStatement  = phoenixWriter.getValue().buildBicingBigTableStatement(dbConnection);
								
					for (BicingStationDao.Value stationInfo : stationsInfoByUpdateTime) {
						// generate and execute an upsert for BICING
						LOGGER.info("Updating table BICING");
						phoenixWriter.getValue().loadBicingBigTableStatement(stationInfo, lastBikeCount, stmtUpsertBicingBigTableStatement, stmtGetStationInfo);
						stmtUpsertBicingBigTableStatement.executeUpdate();
												
						// Commit update to database 
						dbConnection.commit();
						
						// update state
						lastBikeCount = stationInfo.bikes();
					}
				} catch(SQLException se)  {
					LOGGER.error("Error updating tables BICING_FACT and BICING_DIM_TIME for stationInfo {}, {}", stationsInfo, ExceptionUtils.getStackTrace(se));
					// FIXME: use custom bicing exception class
					throw new RuntimeException(se);
				}
				
				finally {
					// Close DB resources
					if (stmtGetStationInfo != null) {
						stmtGetStationInfo.close();
					}
					if (stmtUpsertBicingBigTableStatement != null) {
						stmtUpsertBicingBigTableStatement.close();
					}
					if (dbConnection != null) {
						dbConnection.close();
					}
				}
				 
				return Optional.of(lastBikeCount);
			}	
		});
		
		stateUpdates.print(); // force evaluation, this is essential for this code to have side effects!
	}
	
	public static void run(PropertiesConfiguration config) {
		// Connect to Spark cluster
		JavaStreamingContext jssc = new JavaStreamingContext(config.getString("spark.master"), 
				config.getString("spark.app_name"), 												
				new Duration(config.getLong("spark.batch_duration")));

		// Checkpointing is required when using updateStateByKey
		jssc.checkpoint(config.getString("spark.checkpoint_dir"));
		
		// Connect to Kafka
		Map<String,Integer> kafkaTopics = new HashMap<String, Integer>();
		// See https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/streaming/JavaKafkaWordCount.java
		// the second argument os the number of threads of the Kafka consumer for consuming this topic
		kafkaTopics.put(config.getString("kafka.bicing_topic"), config.getInt("kafka.bicing_topic_threads"));
		/* - First element is the Kafka partition key (see org.collprod.bicingbcn.ingestion.KafkaWriterBolt to
		 *  check that the partition key is datasource + timestamp.toString(), e.g. test_bicing_station_data1404244632)
		 * - Second element is the message itself
		 */
		JavaPairReceiverInputDStream<String,String> kafkaStream = KafkaUtils.createStream(jssc, 
				config.getString("kafka.zookeeper.quorum"),
				config.getString("kafka.groupid"),
				kafkaTopics);
			
		final Broadcast<BicingStationDao> bicingStationDao = jssc.sparkContext().broadcast(new BicingStationDao());
		final Broadcast<String> phoenixJdbcDriver = jssc.sparkContext().broadcast(config.getString("phoenix.jdbc_driver"));
		final Broadcast<String> phoenixDbUrl = jssc.sparkContext().broadcast(config.getString("phoenix.db_url"));
		// To avoid the single thread problem described at http://engineering.sharethrough.com/blog/2013/09/13/top-3-troubleshooting-tips-to-keep-you-sparking/
		final Broadcast<PhoenixWriter> phoenixWriter = jssc.sparkContext().broadcast(new PhoenixWriter());
		
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
		
		updateBicingBigTable(phoenixJdbcDriver, phoenixDbUrl, phoenixWriter, stationStatePairs);
		
		// Launch Spark stream and await for termination
		jssc.start();
		jssc.awaitTermination();
	}

	public static void main(String[] args) {
		// Load program configuration
		LOGGER.info("Loading program configuration");
		PropertiesConfiguration config = null;
		try {
			config = loadConfiguration();
		} catch (ConfigurationException ce) {
			LOGGER.error("Error loading program configuration {}, program will exit", ExceptionUtils.getFullStackTrace(ce));
			System.exit(1);
		}
		LOGGER.info("Done loading program configuration");
		
		LOGGER.info("Starting streamming computation");
		EtlStream.run(config);
	}
}
