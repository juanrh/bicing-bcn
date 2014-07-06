package org.collprod.bicingbcn;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import junit.framework.Assert;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Request;
import org.collprod.bicingbcn.heatmap.HeatmapStream;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

public class HeatmapStreamTest {
	private HeatmapStream heatmapStream;
	
	@Before
	public void setup() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, NoSuchFieldException {
		heatmapStream = new HeatmapStream();
		
		Method loadConfiguration = heatmapStream.getClass().getDeclaredMethod("loadConfiguration");
		loadConfiguration.setAccessible(true);
		loadConfiguration.invoke(heatmapStream);
	}
	
	private void executeRequestAndCheck200(Request request) throws ClientProtocolException, IOException {
		int responseStatusCode = request.execute().returnResponse().getStatusLine().getStatusCode();
		Assert.assertEquals(200, responseStatusCode);
	}
	
	// Ignore as it runs against the actual service, and also requires the credentials, which are not in the repo
	@Ignore
	@Test
	public void testCartoDBStationsStateTableInsert() throws ClientProtocolException, IOException, URISyntaxException {
		BicingStationDao.Value stationValue = BicingStationDao.Value.create(1400940246, 1, 
				41.3979520, 2.18004200, "Gran Via Corts Catalanes", 21, 760, 
				Lists.newArrayList(24,369,387,426), "OPN", 13, 9);
		Request cartoDBInsertStateTableRequest = HeatmapStream
				.insertValueToCartoDBStationsStateTable(stationValue);
		System.out.println("Will send request " + cartoDBInsertStateTableRequest);
		executeRequestAndCheck200(cartoDBInsertStateTableRequest);
	}
	
	// Ignore as it runs against the actual service, and also requires the credentials, which are not in the repo
	@Ignore
	@Test
	public void testClearCartoDBStationsStateTable() throws URISyntaxException, ClientProtocolException, IOException {
		Request cartoDBResetStateTableRequest = HeatmapStream.clearCartoDBStationsStateTable();
		System.out.println("Will send request " + cartoDBResetStateTableRequest);
		executeRequestAndCheck200(cartoDBResetStateTableRequest);	
	}
	
	@Test
	public void testUpdateCartoDBStationsStateTable() throws URISyntaxException, ClientProtocolException, IOException {
		List<BicingStationDao.Value> values = new ArrayList<BicingStationDao.Value>();
		values.add(BicingStationDao.Value.create(1400940246, 1, 
				41.3979520, 2.18004200, "Gran Via Corts Catalanes", 21, 760, 
				Lists.newArrayList(24,369,387,426), "OPN", 13, 3));
		values.add(BicingStationDao.Value.create(1400940246, 1, 
				41.3942720, 2.17516900, "Plaza Tetu&aacute;n", 21, 760, 
				Lists.newArrayList(24,369,387,426), "CLS", 13, 2));
		
		HeatmapStream.executeupdateCartoDBStationsStateTable(values);
	}
	
	
	
}
