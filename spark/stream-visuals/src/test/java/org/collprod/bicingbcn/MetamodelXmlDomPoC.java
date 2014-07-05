package org.collprod.bicingbcn;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.DataContextFactory;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.CompiledQuery;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Proof of concept on using Apache Metamodel for XML parsing, using its DOM variant 
 * 
 * TODO It would be nice studying how to optimize one pass XML parsing by using XmlSaxDataContext
 * */ 

public class MetamodelXmlDomPoC {
	private static final String MODEL_FILE_PATH = "/bicing_2014-05-31_15.53.07_UTC.xml";

	private DataContext bicingData;

	@Before
	public void setup() {
		String okFilePath = this.getClass().getResource(MODEL_FILE_PATH).getFile();
		// This is equivalent to using the factory
		// this.bicingData = new XmlDomDataContext(new File(okFilePath));
		this.bicingData = DataContextFactory.createXmlDataContext(new File(okFilePath), true);
	}
	
	/**
	 * In this query metadata is not used and query are performed using raw strings for the
	 * tables and columns
	 * */
	@Test
	public void rawQueryPoC() {
		Query updatetimeQuery = bicingData.query().from("updatetime").select("updatetime").toQuery();
		DataSet updatetimeResults = bicingData.executeQuery(updatetimeQuery);
		checkUpdatetimeResults(updatetimeResults);
		updatetimeResults.close();
		
		Query stationsQuery = bicingData.query().from("station").select("id").and("lat").and("long").toQuery();
		DataSet stationsResult = bicingData.executeQuery(stationsQuery);
		List<Object[]> stationsResulsArray = stationsResult.toObjectArrays();
		Assert.assertEquals(3, stationsResulsArray.size());
		Assert.assertArrayEquals(Arrays.asList(1, "41.3979520", "2.18004200").toArray(), stationsResulsArray.get(0));	
		Assert.assertArrayEquals(Arrays.asList(2, "41.3942720", "2.17516900").toArray(), stationsResulsArray.get(1));
		Assert.assertArrayEquals(Arrays.asList(3, "41.3936990", "2.18113700").toArray(), stationsResulsArray.get(2));
		stationsResult.close();
	}
	
	/**
	 * In this test we first get the meta representation for tables and columns, so we can fail 
	 * before querying in case this metadata is wrong 
	 * 
	 * For table "updatetime" a single row is obtained with a single column with the value is 1401551587
	 * */
	@Test
	public void updatetimeQueryPoC() {
		Table updatetimeTable = bicingData.getTableByQualifiedLabel("updatetime");
		Column updatetimeColumn = updatetimeTable.getColumnByName("updatetime"); 
		Query updatetimeQuery = bicingData.query().from(updatetimeTable).select(updatetimeColumn).toQuery();
		DataSet updatetimeResults = bicingData.executeQuery(updatetimeQuery);
		checkUpdatetimeResults(updatetimeResults, updatetimeColumn);
		updatetimeResults.close();
	}
	
	private void checkUpdatetimeResults(DataSet updatetimeResults) {
		// FIXME: duplicated code
		Table updatetimeTable = bicingData.getTableByQualifiedLabel("updatetime");
		Column updatetimeColumn = updatetimeTable.getColumnByName("updatetime");
		checkUpdatetimeResults(updatetimeResults, updatetimeColumn);
	}
	
	private void checkUpdatetimeResults(DataSet updatetimeResult, Column updatetimeColumn) {
		int nRows = 0;
		for (Row updatetimeRow : updatetimeResult) {
			Assert.assertEquals("1401551587", updatetimeRow.getValue(updatetimeColumn).toString());
			Assert.assertEquals(1, updatetimeRow.size());
			nRows ++;
		}		
		Assert.assertEquals(1, nRows);
	}
	
	/**
	 * Metadata is checked on query construction: in this case a non existing column throws and exception
	 * before executing the query
	 * */
	@Test(expected=IllegalArgumentException.class)
	public void updatetimeWrongSchemaPoC() {
		Table updatetimeTable = this.bicingData.getTableByQualifiedLabel("updatetime");
		Column wrongUpdatetimeColumn = updatetimeTable.getColumnByName("updatetime_wrong");
		// This throws and IllegalArgumentException because wrongUpdatetimeColumn doesn't exists
		Query updatetimeQuery = this.bicingData.query().from(updatetimeTable).select(wrongUpdatetimeColumn).toQuery();	
	}
	
	@Test
	public void stationQueryPoC() {
		Table stationsTable = bicingData.getTableByQualifiedLabel("station");
		Column idColumn = stationsTable.getColumnByName("id");
		Column latColumn = stationsTable.getColumnByName("lat");
		Column longColumn = stationsTable.getColumnByName("long");
		Column streetColumn = stationsTable.getColumnByName("street");
		Column heightColumn = stationsTable.getColumnByName("height");
		Column streetNumberColumn = stationsTable.getColumnByName("streetNumber");
		Column nearbyStationListColumn = stationsTable.getColumnByName("nearbyStationList");
		Column statusColumn = stationsTable.getColumnByName("status");
		Column slotsColumn = stationsTable.getColumnByName("slots");
		Column bikesColumn = stationsTable.getColumnByName("bikes");
		Query stationsQuery = bicingData.query().from(stationsTable).select(idColumn)
								.and(latColumn).and(longColumn).and(streetColumn).and(heightColumn)
								.and(streetNumberColumn).and(nearbyStationListColumn).and(statusColumn)
								.and(slotsColumn).and(bikesColumn).toQuery();
		CompiledQuery stationsCompiledQuery = bicingData.compileQuery(stationsQuery);
		
		DataSet stationsResult = bicingData.executeQuery(stationsCompiledQuery);
		List<Object[]> stationsResulsArray = stationsResult.toObjectArrays();
		// toObjectArrays consumes the results 
		Assert.assertNull(stationsResult.getRow());
		stationsResult.close();
		Assert.assertEquals(3, stationsResulsArray.size());		
		Assert.assertArrayEquals(Arrays.asList(1, "41.3979520", "2.18004200", "Gran Via Corts Catalanes",
					"21", "760", "24,369,387,426", "OPN", "18", "6").toArray(), stationsResulsArray.get(0));	
		Assert.assertArrayEquals(Arrays.asList(2, "41.3942720", "2.17516900", "Plaza Tetu&aacute;n",
				"21", "8", "360,368,387,414", "OPN", "23", "4").toArray(), stationsResulsArray.get(1));
		Assert.assertArrayEquals(Arrays.asList(3, "41.3936990", "2.18113700", "Ali Bei",
				"21", "44", "4,6,119,419", "OPN", "14", "11").toArray(), stationsResulsArray.get(2));
		
		stationsResult = bicingData.executeQuery(stationsCompiledQuery);
		// need to move to first element
		stationsResult.next();
		Row stationResultRow = stationsResult.getRow();
		Assert.assertEquals("6", stationResultRow.getValue(bikesColumn)); 
		
		// next row
		stationsResult.next();
		stationResultRow = stationsResult.getRow();
		Assert.assertEquals("OPN", stationResultRow.getValue(statusColumn)); 
		
		// next row
		stationsResult.next();
		stationResultRow = stationsResult.getRow();
		Assert.assertEquals("41.3936990", stationResultRow.getValue(latColumn)); 
		
		stationsResult.close();
	}
	
//	TODO test prepared statements
	
}
