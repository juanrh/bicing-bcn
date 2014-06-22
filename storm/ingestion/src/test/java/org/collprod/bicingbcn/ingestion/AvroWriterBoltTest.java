package org.collprod.bicingbcn.ingestion;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class AvroWriterBoltTest {
	AvroWriterBolt avroWriterBolt;
	
	@Before
	public void setup() {
		avroWriterBolt = new AvroWriterBolt();
	}
	
	@Test
	public void createMonth() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException  {
		// http://stackoverflow.com/questions/34571/whats-the-proper-way-to-test-a-class-with-private-methods-using-junit
		long timestamp = 1403452626L;
		Method timestampToMonth = avroWriterBolt.getClass().getDeclaredMethod("timestampToMonth", long.class);
		timestampToMonth.setAccessible(true);
		Assert.assertEquals("2014-06-22", timestampToMonth.invoke(avroWriterBolt, timestamp));
	}
}
