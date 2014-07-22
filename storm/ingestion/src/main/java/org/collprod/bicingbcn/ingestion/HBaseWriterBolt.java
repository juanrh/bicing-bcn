package org.collprod.bicingbcn.ingestion;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * Accepts tuples (DATASOURCE_ID, TIMESTAMP_FIELD, KEY_FIELD, CONTENT_FIELD) which are sent to HBase
 * into a table named 'DATASOURCE_ID' using 'TIMESTAMP_FIELD' as row key, into the 
 * column "data:<KEY_FIELD>" with 'CONTENT_FIELD' as the value, and with a single version cell
 * 
 * TODO: this key schema generates RegionServer hot spotting on inserts, as it grows 
 * monotonically. Implement some salting mechanism like this http://blog.sematext.com/2012/04/09/hbasewd-avoid-regionserver-hotspotting-despite-writing-records-with-sequential-keys/ 
 * or the one in Phoenix to avoid this 
 */
public class HBaseWriterBolt extends BaseRichBolt {

	// generated by Eclipse
	private static final long serialVersionUID = -5483424185011411999L;
	
	private final static Logger LOGGER = LoggerFactory.getLogger(IngestionTopology.class);
	private final static String DATA_COLUMN_FAMILY = "data";
	private static final int CACHE_EXPIRATION_TIME = 10;

	/**
	 * Storm collector for acking or failing
	 * */
	private OutputCollector collector;
		
	private HBaseAdmin hBaseAdmin; 
//	private HConnection hbaseConnection; // this doesn't work in CHD4.4.0 with HBase 0.94.6-cdh4.4.0,
	private HTablePool hbasePool;
	
	/**
	 * Cache from data sources to connections to their 
	 * corresponding tables in HBase 
	 * */
	LoadingCache<String, HTableInterface> tableConnectionsCache;
	
	/**
	 * Ensures a table is created
	 * 
	 * @param hbaseConfigFile local full path to hbase-site.xml
	 * @param outputHBaseTable name of the table 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 * @throws IOException
	 * */
	public static void ensureHBaseTableExists(HBaseAdmin hBaseAdmin, String outputHBaseTable) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		if (! hBaseAdmin.tableExists(outputHBaseTable)) {
			// create table
			LOGGER.info("Target HBase table {} doesn't exist, creating table", outputHBaseTable);
			HTableDescriptor tableDescriptor = new HTableDescriptor(outputHBaseTable);
			HColumnDescriptor dataColumn = new HColumnDescriptor(DATA_COLUMN_FAMILY); 
			dataColumn.setMaxVersions(1); // just one version per row
			tableDescriptor.addFamily(dataColumn);
			hBaseAdmin.createTable(tableDescriptor);
			LOGGER.info("Done creating target HBase table {}", outputHBaseTable);
		} else {
			LOGGER.info("Target HBase table {} already exists", outputHBaseTable);
		}
	}
	
	private LoadingCache<String, HTableInterface> buildTableConnectionsCache() {
		CacheLoader<String, HTableInterface> loader = new CacheLoader<String, HTableInterface>(){
			@Override
			public HTableInterface load(String datasource) throws Exception {
				// Ensure the table exists 
				ensureHBaseTableExists(HBaseWriterBolt.this.hBaseAdmin, datasource);
				// Connect to the table
				HTableInterface table = HBaseWriterBolt.this.hbasePool.getTable(datasource);
				return table;
			}			
		};
		
		RemovalListener<String, HTableInterface> removalListener = new RemovalListener<String, HTableInterface>() {
			@Override
			public void onRemoval(
					RemovalNotification<String, HTableInterface> notification) {
				LOGGER.info("Closing HBase table for datasource {}", notification.getKey());
				try {
					notification.getValue().close();
				} catch (IOException ioe) {
					LOGGER.error("Error closing connection to HBase table {} : {}", notification.getKey(), ExceptionUtils.getStackTrace(ioe));
					throw new RuntimeException(ioe);
				}
			}
			
		}; 
		
		return CacheBuilder.newBuilder()
				.expireAfterAccess(CACHE_EXPIRATION_TIME, TimeUnit.MINUTES)
				.removalListener(removalListener)
				.build(loader);
	};

	
	@Override
	public void execute(Tuple inputTuple) {
		/* Processing tuples of the shape
		   (DATASOURCE_ID, TIMESTAMP_FIELD, CONTENT_FIELD) */
		// get data. No problem with receiving the same data twice, as there is just one versison
		// it will just overwrite itself
		String datasource = inputTuple.getStringByField(RestIngestionSpout.DATASOURCE_ID);
		Long timestamp = inputTuple.getLongByField(RestIngestionSpout.TIMESTAMP_FIELD);
		String key = inputTuple.getStringByField(RestIngestionSpout.KEY_FIELD);
		String content = inputTuple.getStringByField(RestIngestionSpout.CONTENT_FIELD);
		
		// create a new Put to insert into HBase into a table named 'DATASOURCE_ID' 
		// using 'TIMESTAMP_FIELD' as row key, into the column DATA_COLUMN_FAMILY:DATA_QUAL with 
		// 'CONTENT_FIELD' as the value, and with a single version cell
		HTableInterface targetTable = null;
		try {
			targetTable = this.tableConnectionsCache.get(datasource);
			Put put = new Put(Bytes.toBytes(timestamp));
			put.add(Bytes.toBytes(DATA_COLUMN_FAMILY), Bytes.toBytes(key), Bytes.toBytes(content));
			targetTable.put(put);
		} catch (ExecutionException ee) {
			LOGGER.error("Error inserting value for datasource {}: {}", datasource, ExceptionUtils.getStackTrace(ee));
			this.collector.fail(inputTuple);
			return;
		} catch (IOException ioe) {
			LOGGER.error("Error inserting value for datasource {}: {}", datasource, ExceptionUtils.getStackTrace(ioe));
			this.collector.fail(inputTuple);
			return;
		}
		
		// ack this tuple
		this.collector.ack(inputTuple);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes")  Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

		Configuration hBaseConfig = HBaseConfiguration.create();
		hBaseConfig.addResource(new Path(stormConf.get("hbase.res.hbase-site").toString()));
		try {
			this.hBaseAdmin = new HBaseAdmin(hBaseConfig);
			// this.hbaseConnection = HConnectionManager.createConnection(hBaseConfig); not for this version of HBase
			this.hbasePool = new HTablePool();
		} catch (MasterNotRunningException mnre) {
			LOGGER.error("Error closing connection to HBase {}", ExceptionUtils.getStackTrace(mnre));
			throw new RuntimeException(mnre);
		} catch (ZooKeeperConnectionException ze) {
			LOGGER.error("Error closing connection to HBase {}", ExceptionUtils.getStackTrace(ze));
			throw new RuntimeException(ze);
		}
		
		this.tableConnectionsCache = buildTableConnectionsCache();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// this bolts emits no tuples, just inserts them into HBase
	}
	
	@Override
	public void cleanup() {
		// Close connection to all tables
		this.tableConnectionsCache.invalidateAll();
		
		try {
			this.hBaseAdmin.close();
			// this.hbaseConnection.close(); // not for this version of HBase
			this.hbasePool.close();
		} catch (IOException ioe) {
			LOGGER.error("Error closing connection to HBase {}", ExceptionUtils.getStackTrace(ioe));
		}
	}

}
