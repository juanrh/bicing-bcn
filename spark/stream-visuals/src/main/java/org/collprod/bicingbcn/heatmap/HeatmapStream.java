package org.collprod.bicingbcn.heatmap;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

// See https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/KafkaWordCount.scala
public class HeatmapStream {
	// TODO properties loading
	private static final String DEFAULT_INGESTION_PROPS="visuals.properties";

	public static void main(String[] args) {		
		// FIXME: local vs cluster mode, use properties
		JavaStreamingContext jssc = new JavaStreamingContext("local", "heatmap", new Duration(1000));

		Map<String,Integer> kafkaTopics = new HashMap<String, Integer>();
		kafkaTopics.put("test_bicing_station_data", 1);
		// - First element is the Kafka partition key (see org.collprod.bicingbcn.ingestion.KafkaWriterBolt to
		// check that the partition key is datasource + timestamp.toString(), e.g. test_bicing_station_data1404244632)
		// - Second element is the message itself
		JavaPairReceiverInputDStream<String,String> kafkaStream = KafkaUtils.createStream(jssc, "localhost:2181", "0", kafkaTopics);
		JavaDStream<String> messages = kafkaStream.map(new Function<Tuple2<String, String>,String>() {
			@Override
			public String call(Tuple2<String, String> pair) throws Exception {
				return pair._2;
			}
		});
		messages.print();
		
		jssc.start();
		jssc.awaitTermination();
	}
}
