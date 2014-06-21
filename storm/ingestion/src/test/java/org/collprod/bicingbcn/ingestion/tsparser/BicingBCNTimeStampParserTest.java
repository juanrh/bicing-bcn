package org.collprod.bicingbcn.ingestion.tsparser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.lang.SystemUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;


/**
 * Unit tests for class BicingBCNTimeStampParser
 * 
 * NOTE: use ${workspace_loc:storm-ingestion/src/test/resources} as working directory for running from Eclipse. Works ok 
 * with mvn test (as it follows http://maven.apache.org/guides/introduction/introduction-to-the-standard-directory-layout.html)
 * */

public class BicingBCNTimeStampParserTest {
	private BicingBCNTimeStampParser parser;
	
	private static final String TEST_FILENAME_SUCCESS_1 = "/bicing_2014-05-31_15.53.07_UTC.xml";
	private static final String TEST_FILENAME_FAIL_1 = "/bicing_2014-05-31_15.53.07_UTC-bad.xml";
	
	private String loadTestFile(String filePath) throws IOException {
		StringBuffer fileContents = new StringBuffer();
		String line;
		
		BufferedReader reader = new BufferedReader(new FileReader(filePath));
		while ((line = reader.readLine()) != null) {
			fileContents.append(line + SystemUtils.LINE_SEPARATOR);
		}
		reader.close();
		return fileContents.toString();
	}
	
	@Before
	public void setUp() {
		parser = new BicingBCNTimeStampParser();
	}
	
	@After
	public void tearDown() {
		// helping garbage collector 
		parser = null;
	}
	
	@Test
	public void sucessfulParse() throws IOException {
		String okFileContents = loadTestFile(this.getClass().getResource(TEST_FILENAME_SUCCESS_1).getFile());
		Optional<Long> timestamp = parser.apply(okFileContents);
		Assert.assertTrue(timestamp.isPresent());
		Assert.assertEquals(new Long(1401551587), timestamp.get());
	}
	
	@Test
	public void failingParse() throws IOException {
		String badFileContents = loadTestFile(this.getClass().getResource(TEST_FILENAME_FAIL_1).getFile());
		Optional<Long> timestamp = parser.apply(badFileContents);
		Assert.assertFalse(timestamp.isPresent());
	}
}
