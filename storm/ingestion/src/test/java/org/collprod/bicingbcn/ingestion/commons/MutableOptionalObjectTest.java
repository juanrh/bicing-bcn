package org.collprod.bicingbcn.ingestion.commons;

import junit.framework.Assert;

import org.collprod.bicingbcn.ingestion.tsparser.BicingBCNTimeStampParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;

public class MutableOptionalObjectTest {
	MutableOptionalObject<String> mutableOptionalString;
	Optional<String> optionalString;
	
	@Before
	public void setUp() {
	}
	
	@After
	public void tearDown() {
		// helping garbage collector 
		mutableOptionalString = null;
		optionalString = null;
	}
	
	private void stressMutableOptionalString() {
		mutableOptionalString.set(Optional.of("hola"));
		Assert.assertTrue(mutableOptionalString.get().isPresent());
		Assert.assertEquals("hola", mutableOptionalString.get().get());
		
		mutableOptionalString.set(Optional.of("adios"));
		Assert.assertTrue(mutableOptionalString.get().isPresent());
		Assert.assertEquals("adios", mutableOptionalString.get().get());
		
		mutableOptionalString.setAbsent();
		Assert.assertFalse(mutableOptionalString.get().isPresent());
		
		mutableOptionalString.set(Optional.of("ey"));
		Assert.assertTrue(mutableOptionalString.get().isPresent());
		Assert.assertEquals("ey", mutableOptionalString.get().get());
	}
	
	@Test
	public void defaultConstructor() {
		mutableOptionalString = new MutableOptionalObject<String>();
		Assert.assertFalse(mutableOptionalString.get().isPresent());
		stressMutableOptionalString();
	}
	
	@Test
	public void directValueConstructor() {
		mutableOptionalString = new MutableOptionalObject<String>("hola");
		Assert.assertTrue(mutableOptionalString.get().isPresent());
		Assert.assertEquals("hola", mutableOptionalString.get().get());
		stressMutableOptionalString();
	}
	
	@Test
	public void indirectValueConstructor() {
		mutableOptionalString = new MutableOptionalObject<String>(Optional.of("hola"));
		Assert.assertTrue(mutableOptionalString.get().isPresent());
		Assert.assertEquals("hola", mutableOptionalString.get().get());
		stressMutableOptionalString();
	}
	
	@Test
	public void absentValueConstructor() {
		Optional<String> absentString = Optional.absent();
		mutableOptionalString = new MutableOptionalObject<String>(absentString);
		Assert.assertFalse(mutableOptionalString.get().isPresent());
		Assert.assertEquals(mutableOptionalString, new MutableOptionalObject<String>());
	}
	
	@Test
	public void equalsOk() {
		mutableOptionalString = new MutableOptionalObject<String>("hola");
		Assert.assertFalse(mutableOptionalString.equals(new MutableObject<String>("hola")));
	}
}
