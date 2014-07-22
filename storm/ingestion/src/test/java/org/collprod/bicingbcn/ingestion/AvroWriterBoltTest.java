package org.collprod.bicingbcn.ingestion;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import junit.framework.Assert;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.collprod.bicingbcn.ingestion.attic.AvroWriterBolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.Config;

public class AvroWriterBoltTest {
	private AvroWriterBolt avroWriterBolt;
	private Path targetPath;
	private FileSystem hdfs; 
	private AvroWriterBolt.DatasourceMonth datasourceMonth;
	private org.apache.hadoop.conf.Configuration hadoopConf;
	
	@Before
	public void setup() throws SecurityException, NoSuchMethodException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, IOException {
		avroWriterBolt = new AvroWriterBolt();
		
		Config conf = IngestionTopology.loadConfiguration();
		// enough for now, but not very nice
		avroWriterBolt.prepare(conf, null, null);
		Field hdfsField = avroWriterBolt.getClass().getDeclaredField("hdfs");
		hdfsField.setAccessible(true);
		hdfs = (FileSystem) hdfsField.get(avroWriterBolt);
		
		Field hadoopConfField = avroWriterBolt.getClass().getDeclaredField("hadoopConf");
		hadoopConfField.setAccessible(true);
		hadoopConf = (org.apache.hadoop.conf.Configuration) hadoopConfField.get(avroWriterBolt);
		
		// Delete target path
		Method buildTargetPath = avroWriterBolt.getClass()
				.getDeclaredMethod("buildTargetPath", AvroWriterBolt.DatasourceMonth.class);
		buildTargetPath.setAccessible(true);
		
		// Define target path
		// pass future month so the month is not present
		datasourceMonth = AvroWriterBolt.DatasourceMonth.create("bicing_station_data", "3014-06-22"); 
		targetPath = (Path) buildTargetPath.invoke(avroWriterBolt, datasourceMonth);
		if (hdfs.exists(targetPath)) {
			hdfs.delete(targetPath);	
		}
	}
	
	@After 
	public void tearDown() throws IOException {
		if (hdfs.exists(targetPath)) {
			hdfs.delete(targetPath);	
		}
	}
	
	@Test
	public void createMonthTest() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException  {
		// http://stackoverflow.com/questions/34571/whats-the-proper-way-to-test-a-class-with-private-methods-using-junit
		long timestamp = 1403452626L * 1000L;
		Method timestampToMonth = avroWriterBolt.getClass().getDeclaredMethod("timestampToMonth", long.class);
		timestampToMonth.setAccessible(true);
		Assert.assertEquals("2014-06-22", timestampToMonth.invoke(avroWriterBolt, timestamp));
	}
	
	
	private void writeNRecords(DataFileWriter<GenericRecord> writer, int numRecords, int startIndex) throws IOException {
		GenericRecord newDataRecord = new GenericData.Record(AvroWriterBolt.AVRO_SCHEMA);
		for (int i = startIndex; i < numRecords + startIndex; i++) {
			// FIXME: this code is repeated in checkReadNRecords
			newDataRecord.put(AvroWriterBolt.AVRO_TIMESTAMP_FIELD, new Long(10 + i));
			newDataRecord.put(AvroWriterBolt.AVRO_CONTENT_FIELD, "test content " + i);
			writer.append(newDataRecord);
			System.out.println("Wrote record " + newDataRecord);
		}
		writer.close();
	}
	
	private void checkReadNRecords(int numRecords) throws IOException {
		DataFileReader<GenericRecord> dataFileReader = 
				new DataFileReader<GenericRecord>(new FsInput(targetPath, hadoopConf),
						new GenericDatumReader<GenericRecord>(AvroWriterBolt.AVRO_SCHEMA));
		GenericRecord readDataRecord = null;
		int numRecordsRead = 0;
		while (dataFileReader.hasNext()) {
			readDataRecord = dataFileReader.next(readDataRecord);
			Assert.assertEquals(new Long(10 + numRecordsRead), readDataRecord.get(AvroWriterBolt.AVRO_TIMESTAMP_FIELD));
			Assert.assertEquals("test content "  + numRecordsRead, readDataRecord.get(AvroWriterBolt.AVRO_CONTENT_FIELD).toString());
			numRecordsRead++;
			System.out.println("Read record " + readDataRecord);
		}
		Assert.assertEquals(numRecords, numRecordsRead);
		dataFileReader.close();
	}
	
	@Test
	public void createHDFSFileTest() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, IOException, NoSuchFieldException {
		// Check create file
			// Create 
		Method openHDFSFile = avroWriterBolt.getClass()
									.getDeclaredMethod("openHDFSFile", AvroWriterBolt.DatasourceMonth.class);
		openHDFSFile.setAccessible(true);
		DataFileWriter<GenericRecord> writer = 
				(DataFileWriter<GenericRecord>) openHDFSFile.invoke(avroWriterBolt, datasourceMonth);
		
		writeNRecords(writer, 10, 0);
		checkReadNRecords(10);
		
		// Check append to file
			// The file should still exists
		Assert.assertTrue(hdfs.exists(targetPath));
			// this should be open in append mode
		writer = (DataFileWriter<GenericRecord>) openHDFSFile.invoke(avroWriterBolt, datasourceMonth);
			// hence there should be 20 records after writing 10 more
		writeNRecords(writer, 10, 10);
		checkReadNRecords(20);
	}
	
}
