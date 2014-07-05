package org.collprod.bicingbcn.heatmap;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.collprod.bicingbcn.BicingStationDao;
import org.collprod.bicingbcn.BicingStationDao.Value;

import scala.Tuple2;

// See https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/KafkaWordCount.scala
public class HeatmapStream {
	// TODO properties loading
	private static final String DEFAULT_INGESTION_PROPS="visuals.properties";

	public static void main(String[] args) {		
		// FIXME: local vs cluster mode, use properties
		JavaStreamingContext jssc = new JavaStreamingContext("local", "heatmap", new Duration(1000));
		
		// example of using an Spark broadcast variable from spark streaming 
		JavaSparkContext jsc = jssc.sparkContext();
			// each node uses its own copy
		final Broadcast<BicingStationDao> sharedBicingStationDao = jsc.broadcast(new  BicingStationDao());
		

		Map<String,Integer> kafkaTopics = new HashMap<String, Integer>();
		kafkaTopics.put("test_bicing_station_data", 1);
		// - First element is the Kafka partition key (see org.collprod.bicingbcn.ingestion.KafkaWriterBolt to
		// check that the partition key is datasource + timestamp.toString(), e.g. test_bicing_station_data1404244632)
		// - Second element is the message itself
		JavaPairReceiverInputDStream<String,String> kafkaStream = KafkaUtils.createStream(jssc, "localhost:2181", "0", kafkaTopics);
		/* JavaDStream<String> messages = kafkaStream.map(new Function<Tuple2<String, String>,String>() {
			@Override
			public String call(Tuple2<String, String> pair) throws Exception {
				// return the message, i.e., the bicing XML 
				return pair._2 ;
			}
		});
		*/
		JavaDStream<BicingStationDao.Value> bicingStationValues =  kafkaStream.flatMap(new FlatMapFunction<Tuple2<String, String>, BicingStationDao.Value>() {
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
				return sharedBicingStationDao.value().parse(kafkakeyMessage._2);
			}
		});
		
		bicingStationValues.print();
		
		// + " " + sharedInt.value().toString();
//		bicingStationDao
		
//		messages.print();
		
		jssc.start();
		jssc.awaitTermination();
	}
}
