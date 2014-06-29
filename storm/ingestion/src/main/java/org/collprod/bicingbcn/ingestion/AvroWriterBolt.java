package org.collprod.bicingbcn.ingestion;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.google.auto.value.AutoValue;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
/**
 * Writes the data to the HDFS path specified in the configuration for this data source
 * Data is serialized in Avro with an Avro file per month, hence it is stored in the path
 * <data source HDFS path>/<month>.avro as a new Avro record with the name AvroWriterBolt.AVRO_RECORD_NAME, 
 * and with the fields:
 * - AVRO_TIMESTAMP_FIELD :: long
 * - AVRO_CONTENT_FIELD :: string
 * 
 * No additional timestamp check is performed, this bolt asssumes that all the data is new
 * */
public class AvroWriterBolt extends BaseRichBolt {
	
	// Generated by Eclipse 
	private static final long serialVersionUID = 117628243688887424L;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AvroWriterBolt.class);
	private static final SimpleDateFormat MONTH_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");
	private static final int FILEWRITER_SYNC_INTERVAL = 100;
	private static final int CACHE_EXPIRATION_TIME = 3;
	
	public static final String AVRO_RECORD_NAME = "data";
	public static final String AVRO_RECORD_NAMESPACE = "org.collprod";
	public static final String AVRO_TIMESTAMP_FIELD = "timestamp";
	public static final String AVRO_CONTENT_FIELD = "content";
	public static final Schema AVRO_SCHEMA;
	static {
		AVRO_SCHEMA = SchemaBuilder
				.record(AVRO_RECORD_NAME).namespace(AVRO_RECORD_NAMESPACE)
				.fields()
					.name(AVRO_TIMESTAMP_FIELD).type(Schema.create(Schema.Type.LONG)).noDefault()
					.name(AVRO_CONTENT_FIELD).type(Schema.create(Schema.Type.STRING)).noDefault()
				.endRecord();
	}
	/**
	 * Storm collector to emit tuples
	 * */
	private OutputCollector collector;

	// Configuration
	private Map<String, String> datasourcesDirectories;

	// HDFS stuff
	private org.apache.hadoop.conf.Configuration hadoopConf;
	private FileSystem hdfs;	
	private boolean debugMode; 

	// State
	/**
	 * As the order of the tuples is not preserved we might open a file for a new month and 
	 * then get a tuple for the previous month. We use this cache to keeps open the files 
	 * for several datasource-month pairs until not used for CACHE_EXPIRATION_TIME minutes 
	 * */
	private LoadingCache<DatasourceMonth, DataFileWriter<GenericRecord>> writersCache;

	@AutoValue
	static abstract class DatasourceMonth {
		DatasourceMonth() {}
		public static DatasourceMonth create(String datasource, String month) {
			return new AutoValue_AvroWriterBolt_DatasourceMonth(datasource, month);
		}
		public abstract String datasource();
		public abstract String month();
	}
	
	private Path buildTargetPath(DatasourceMonth datasourceMonth) {
		return new Path(this.datasourcesDirectories.get(datasourceMonth.datasource())
				+ "/"+  datasourceMonth.month() + ".avro");
	}
	/**
	 * Builds the target file path as <datasource directory>/<month>.avro.
	 * If the target file already exists, then it is open for appending, otherwise it is created
	 * */
	private DataFileWriter<GenericRecord> openHDFSFile(DatasourceMonth datasourceMonth) throws IOException {
		DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(
				new GenericDatumWriter<GenericRecord>(AVRO_SCHEMA));
		writer.setSyncInterval(FILEWRITER_SYNC_INTERVAL);
		// writer.setCodec(CodecFactory.snappyCodec()); // omit for now
		
		Path targetPath = buildTargetPath(datasourceMonth);
			// just for logging
		String fullTargetPath = this.hdfs.getWorkingDirectory() + "/" + targetPath;
		// Append to an existing file, or create a new file is file otherwise
		if (this.hdfs.exists(targetPath)) {
			// appending to an existing file
			// based on http://technicaltidbit.blogspot.com.es/2013/02/avro-can-append-in-hdfs-after-all.html
			if (debugMode) {
				this.hdfs.setReplication(targetPath, (short)1);
			}
			LOGGER.info("Appending to existing file {}", fullTargetPath);
			OutputStream outputStream = this.hdfs.append(targetPath);
			writer.appendTo(new FsInput(targetPath, this.hadoopConf), outputStream); 
		} else {
			// creating a new file
			LOGGER.info("Creating new file "  +  fullTargetPath + " for datasource {} and month {}", 
					datasourceMonth.datasource(), datasourceMonth.month());
			OutputStream outputStream = this.hdfs.create(targetPath);
			writer.create(AVRO_SCHEMA, outputStream);
		}
		
		return writer;
	}
	
	private LoadingCache<DatasourceMonth, DataFileWriter<GenericRecord>> createWritersCache() {
		CacheLoader<DatasourceMonth, DataFileWriter<GenericRecord>> loader = 
				new CacheLoader<DatasourceMonth, DataFileWriter<GenericRecord>> () {
			@Override
			public DataFileWriter<GenericRecord> load(DatasourceMonth datasourceMonth)
					throws Exception {
				return AvroWriterBolt.this.openHDFSFile(datasourceMonth);
			}
		};
		
		// A synchronous removal listener should be enough in principle 
		RemovalListener<DatasourceMonth, DataFileWriter<GenericRecord>> removalListener = 
				new RemovalListener<DatasourceMonth, DataFileWriter<GenericRecord>>() {
			@Override
			public void onRemoval(
					RemovalNotification<DatasourceMonth, DataFileWriter<GenericRecord>> removal) {
				try {
					LOGGER.info("Closing file for datasource {}", removal.getKey().datasource());
					removal.getValue().close();				
				} catch (IOException ioe) {
					LOGGER.error("Error closing file for datasource {}: {}", 
								removal.getKey().datasource(), ioe.getMessage());
					throw new RuntimeException(ioe);
				}
			}
		};

		return CacheBuilder.newBuilder()
				.expireAfterAccess(CACHE_EXPIRATION_TIME, TimeUnit.MINUTES)
				.removalListener(removalListener)
				.build(loader);
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
		this.debugMode = stormConf.get("debug").equals("true");
		if (this.debugMode) {
			LOGGER.info("Debug mode is on, will use replication factor of 1 for HDFS");
		} 
		
		// Load target datasource directories from Storm configuration 
		try {
			Map<String, Configuration> datasourcesConfigurations = 
					IngestionTopology.deserializeConfigurations((Map<String, String>) stormConf.get(IngestionTopology.DATASOURCE_CONF_KEY));
			this.datasourcesDirectories = new HashMap<String, String>();
			for (Map.Entry<String, Configuration> datasourceConfig : datasourcesConfigurations.entrySet()) {
				this.datasourcesDirectories.put(datasourceConfig.getKey(),
											    datasourceConfig.getValue().getString("hdfs_path"));
			}
		} catch (ConfigurationException ce) {
			LOGGER.error("Error parsing datasource configurations: " + ce.getMessage());
			throw new RuntimeException(ce);
		}
		
		// Create objects to interact with HDFS
			// This configuration reads from the default files
		this.hadoopConf = new org.apache.hadoop.conf.Configuration(true);
		hadoopConf.addResource(new Path(stormConf.get("hadoop.res.core-site").toString()));
		hadoopConf.addResource(new Path(stormConf.get("hadoop.res.hdfs-site").toString()));
		
		try {
			this.hdfs = FileSystem.get(this.hadoopConf);
		} catch (IOException ioe) {
			LOGGER.error("Error connecting to HDFS: " + ioe.getMessage());
			throw new RuntimeException(ioe);
		}
		
		// Initialize cache for DataFileWriter objects
		this.writersCache = createWritersCache();
	}
	
	private String timestampToMonth(long timestamp) {
		// convert from the seconds returned by a TimestampParser
		// to the milliseconds accepted by Date
		return MONTH_FORMATTER.format(new Date(timestamp * 1000));
	}

	@Override
	public void execute(Tuple inputTuple) {
		/* Processing tuples of the shape
		   (DATASOURCE_ID, TIMESTAMP_FIELD, CONTENT_FIELD) */
		
		// get datasource
		String datasource = inputTuple.getStringByField(RestIngestionSpout.DATASOURCE_ID);
		// compute month
		long timestamp = inputTuple.getLongByField(TimestampParserBolt.TIMESTAMP_FIELD);
			// this computation is completely stateless 
		String month = timestampToMonth(timestamp);
		
		// now get the DataFileWriter
		DataFileWriter<GenericRecord> writer = null;
		try {
			writer = 
					this.writersCache.get(DatasourceMonth.create(datasource, month));
		} catch (ExecutionException ee) {
			LOGGER.error("Error getting DataFileWriter for tuple for datasource " + datasource 
					+ " and timestamp " + timestamp + " : " + ee.getMessage());	
			this.collector.fail(inputTuple);
			return;
		}
		
		// create and write a new record
		GenericRecord newDataRecord = new GenericData.Record(AVRO_SCHEMA);
		newDataRecord.put(AVRO_TIMESTAMP_FIELD, new Long(timestamp));
		newDataRecord.put(AVRO_CONTENT_FIELD, inputTuple.getStringByField(RestIngestionSpout.CONTENT_FIELD));
		try {
			writer.append(newDataRecord);
		} catch (IOException ioe) {
			LOGGER.error("Error writing Avro record for datasource " + datasource 
					+ " and timestamp " + timestamp + " : " + ioe.getMessage());	
			this.collector.fail(inputTuple);
			return;
		}
		
		// ACK processing for this tupe as ok
		this.collector.ack(inputTuple);
	}
	
	/**
	 * This method is useless in cluster mode, as stated by Storms documentation
	 * 
	 * http://nathanmarz.github.io/storm/doc-0.8.1/backtype/storm/task/IBolt.html#cleanup()
	 * 
	 * "Called when an IBolt is going to be shutdown. There is no guarentee that cleanup will be called, 
	 * because the supervisor kill -9's worker processes on the cluster.
	 * The one context where cleanup is guaranteed to be called is when a topology is killed when running Storm in local mode."
	 * */
	@Override
	public void cleanup() {

		// Invalidate all the elements in the DataFileWriter cache. As a result the removal 
		// listeners will close the corresponding files
		this.writersCache.invalidateAll();
		try {
			this.hdfs.close();
			LOGGER.info("Closed connection to HDFS");
		} catch (IOException ioe) {
			LOGGER.error("Error closing connection to HDFS :" + ioe.getMessage() 
					+ ", " + ExceptionUtils.getStackTrace(ioe));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// this bolts emits no tuples, just stores in HDFS
	}

}
