package org.collprod.bicingbcn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import junit.framework.Assert;

import org.collprod.bicingbcn.etl.PhoenixWriter;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

public class EtlStreamTest {
	// FIXME hardcoded, load from config and broadcast
	// JDBC driver name and database URL
	final String JDBC_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
	final String DB_URL = "jdbc:phoenix:localhost";
	
	private Connection con = null;
	private BicingStationDao.Value stationInfo;
	private PhoenixWriter phoenixWriter; 
	
	@Before
	public void setup() {
		stationInfo = BicingStationDao.Value.create(1400940246L * 1000L, 1, 41.3979520, 2.18004200, "Gran Via Corts Catalanes", 21,
				760, Lists.newArrayList(24,369,387,426), "OPN", 13, 9);
		phoenixWriter = new PhoenixWriter();
	}
	
	/**
	 * Ignored as it implies a side effect in the database
	 * */
	@Ignore
	@Test
	public void testUpsertBicingTime() throws ClassNotFoundException, SQLException  {
		PreparedStatement stmtBicingDimTime = null;
		try {
			// Prepare DB connection
			// Register JDBC driver
			Class.forName(JDBC_DRIVER);
			con = DriverManager.getConnection(DB_URL);
			
			// Create, load and execute update statement
			stmtBicingDimTime = phoenixWriter.buildBicingDimTime(con); 
			phoenixWriter.loadBicingDimTimeStatement(stationInfo, stmtBicingDimTime);
			stmtBicingDimTime.executeUpdate();
			
			// Commit to database
			con.commit();
			
		} catch (SQLException e) {
			throw e;
		} finally {
			// Close DB resources
			if (stmtBicingDimTime != null) {
				stmtBicingDimTime.close();
			}
			if (con != null) {
				con.close();
			}
		}
	}
	
	/**
	 * Ignored as it assumes a particular state of the database
	 * */
	@Ignore
	@Test
	public void testLookupBicingTime() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		con = DriverManager.getConnection(DB_URL);
		PreparedStatement stmtCheckExistsTimetagDimTime = con.prepareStatement("SELECT TIMETAG from BICING_DIM_TIME WHERE TIMETAG = ?");
			// in milliseconds 
		stmtCheckExistsTimetagDimTime.setTimestamp(1, new Timestamp(stationInfo.updatetime()));
		ResultSet queryResults = stmtCheckExistsTimetagDimTime.executeQuery();
		boolean exists =  queryResults.next();
		Assert.assertTrue(exists);
		stmtCheckExistsTimetagDimTime.close();
		con.close();
	}
	

	/**
	 * Ignored as it assumes a particular state of the database
	 * */
	@Ignore
	@Test
	public void testLookupStationState() throws ClassNotFoundException, SQLException {
		PreparedStatement stmtGetStationInfo = null;
		Long stationId = 510L;
		try  {
			Class.forName(JDBC_DRIVER);
			con = DriverManager.getConnection(DB_URL);
			stmtGetStationInfo = phoenixWriter.buildLookupStationStatement(con);
			PhoenixWriter.DimStationRecord stationInfo = phoenixWriter.lookupStationRecord(stmtGetStationInfo, stationId);
			System.out.println(stationInfo);
		
		} catch (SQLException e) {
			throw e;
		} finally {
			// Close DB resources
			if (stmtGetStationInfo != null) {
				stmtGetStationInfo.close();
			}
			if (con != null) {
				con.close();
			}
		}
	}
	
	@Test
	public void testBicingBigTableUpsert() throws ClassNotFoundException, SQLException {
		PreparedStatement stmtGetStationInfo = null;
		PreparedStatement stmtUpsertBicingBigTableStatement= null;
		
		try  {
			Class.forName(JDBC_DRIVER);
			con = DriverManager.getConnection(DB_URL);
			stmtGetStationInfo = phoenixWriter.buildLookupStationStatement(con);
			
			stmtUpsertBicingBigTableStatement = phoenixWriter.buildBicingBigTableStatement(con);
			stmtGetStationInfo = phoenixWriter.buildLookupStationStatement(con);
			phoenixWriter.loadBicingBigTableStatement(stationInfo, 0, stmtUpsertBicingBigTableStatement, stmtGetStationInfo);
			stmtUpsertBicingBigTableStatement.executeUpdate();
			
			con.commit();
		} catch (SQLException e) {
			throw e;
		} finally {
			// Close DB resources
			if (stmtGetStationInfo != null) {
				stmtGetStationInfo.close();
			}
			if (stmtUpsertBicingBigTableStatement != null) {
				stmtUpsertBicingBigTableStatement.close();
			}
			if (con != null) {
				con.close();
			}
		}
	}
	
}
