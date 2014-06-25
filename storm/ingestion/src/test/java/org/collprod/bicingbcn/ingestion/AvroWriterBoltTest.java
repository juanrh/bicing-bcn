package org.collprod.bicingbcn.ingestion;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import junit.framework.Assert;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.Config;

public class AvroWriterBoltTest {
	AvroWriterBolt avroWriterBolt;
	
	@Before
	public void setup() {
		avroWriterBolt = new AvroWriterBolt();
	}
	
	@Test
	public void createMonthTest() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException  {
		// http://stackoverflow.com/questions/34571/whats-the-proper-way-to-test-a-class-with-private-methods-using-junit
		long timestamp = 1403452626L;
		Method timestampToMonth = avroWriterBolt.getClass().getDeclaredMethod("timestampToMonth", long.class);
		timestampToMonth.setAccessible(true);
		Assert.assertEquals("2014-06-22", timestampToMonth.invoke(avroWriterBolt, timestamp));
	}
	
	@Test
	public void createHDFSFileTest() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, IOException {
		// TODO: first delete the target file, test append too!!!
		
		// pass future month so the month is not present
		Config conf = IngestionTopology.loadConfiguration();
			// enough for now, but not very nice
		avroWriterBolt.prepare(conf, null, null);
		Method openHDFSFile = avroWriterBolt.getClass()
									.getDeclaredMethod("openHDFSFile", AvroWriterBolt.DatasourceMonth.class);
		openHDFSFile.setAccessible(true);
		DataFileWriter<GenericRecord> writer = 
				(DataFileWriter<GenericRecord>) openHDFSFile.invoke(avroWriterBolt, AvroWriterBolt.DatasourceMonth.create("bicing_station_data", "3014-06-22"));
		
		GenericRecord newDataRecord = new GenericData.Record(AvroWriterBolt.AVRO_SCHEMA);
		for (int i = 0; i < 10; i++) {
			newDataRecord.put(AvroWriterBolt.AVRO_TIMESTAMP_FIELD, new Long(10 + i));
			newDataRecord.put(AvroWriterBolt.AVRO_CONTENT_FIELD, "test content " + i);
			writer.append(newDataRecord);
		}
		
		writer.close();
	}
	
}
