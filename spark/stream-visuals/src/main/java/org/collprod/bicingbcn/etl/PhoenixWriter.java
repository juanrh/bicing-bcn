package org.collprod.bicingbcn.etl;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.collprod.bicingbcn.BicingStationDao;
import org.joda.time.DateTime;

public class PhoenixWriter implements Serializable {

	private static final long serialVersionUID = -8620289406311041119L;

	/**
	 * Uses con to build a PreparedStatement object that can be used to update the table BICING_FACT
	 * @throws SQLException 
	 * */
	public PreparedStatement buildBicingFactStatement(Connection con) throws SQLException {
		String upsertBicingFactString = "UPSERT INTO BICING_FACT VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
		return con.prepareStatement(upsertBicingFactString);
	}
	
	/**
	 * Sets the fields of stmtBicingFact according to the values of stationInfo and lastBikeCount
	 * so it can be used to update the table BICING_FACT
	 * 
	 * NOTE: The field slots is not used for computing the number of bikes lent and returned, as it changes with 
	 * the station health, and not necessarily because of bike lent transactions
	 * 
	 * CREATE TABLE IF NOT EXISTS BICING_FACT (
				    -- station to which the data for this row applies
				    -- FK for BICING_DIM_STATION
			1.		STATION UNSIGNED_LONG NOT NULL,
				    -- NOTE: remember bicing ingests UNIX time in seconds
				    -- FK for BICING_DIM_TIME
			2.	    TIMETAG TIMESTAMP NOT NULL,
				    -- Fact fields
				    -- As there are just a few fact we put all in the same HBase column
				    -- Degenerate dimension: 'OPN' (open) or 'CLS' (closed)
			3.	    F.STATUS VARCHAR(3),
				    -- Number of parking slots available: should be 0 
				    -- if F.STATUS is "CLS"
			4.	    F.SLOTS UNSIGNED_LONG,
				    -- Number of bikes available: should be 0 
				    -- if F.STATUS is "CLS"
			5.	    F.AVAILABLE UNSIGNED_LONG,
				    -- Total capacity of the station as (parking slots
				    -- + bikes) * (status == OPN)
			6.	    F.CAPACITY UNSIGNED_LONG,
				    -- Number of bikes lent for this station since 
				    -- the previous update
			7.	    F.LENT UNSIGNED_LONG,
				    -- Number of bikes returned to this station since 
				    -- the previous update
			8.	    F.RETURNED UNSIGNED_LONG
				    -- This gives a non monotonically increasing row key
				    CONSTRAINT PK PRIMARY KEY (STATION, TIMETAG)
				);
	 *	
	 * @throws SQLException 
	 */
	public void loadBicingFactStatement(BicingStationDao.Value stationInfo, int lastBikeCount, PreparedStatement stmtBicingFact) throws SQLException {
		boolean isStationOpen = stationInfo.status().equals("OPN");
		boolean notValidState = lastBikeCount < 0;
		
		// STATION
		stmtBicingFact.setInt(1, stationInfo.id());
		// TIMETAG
		stmtBicingFact.setTimestamp(2, new Timestamp(stationInfo.updatetime() * 1000)); // TODO ensure correct translation to milliseconds
		// F.STATUS
		stmtBicingFact.setString(3,  stationInfo.status());
		// F.SLOTS
		stmtBicingFact.setInt(4, isStationOpen ? stationInfo.slots() : 0);
		// F.AVAILABLE
		// Number of bikes available, 0 if station is not open
		stmtBicingFact.setInt(5, isStationOpen ? stationInfo.bikes() : 0);
		// F.CAPACITY 
		//  Total capacity of the station as (parking slots + bikes) * (status == OPN)
		stmtBicingFact.setInt(6, isStationOpen ? stationInfo.slots() + stationInfo.bikes() : 0); 
		// F.LENT
		// Number of bikes lent for this station since the previous update
		//  - if lastBikeCount is negative then we don't have info and we return as nothing
		// 	- if we have more bikes now than in the previous update we assume no bike has been lent,  
		//    this implies an error if bikes are returned and taken between updates
		stmtBicingFact.setInt(7, notValidState ? 0 : Math.max(lastBikeCount - stationInfo.bikes(), 0));
		// F.RETURNED
		// Number of bikes returned to this station since the previous update
		// same compromises as the previous value
		stmtBicingFact.setInt(8, notValidState ? 0 : Math.max(stationInfo.bikes() - lastBikeCount, 0));
	}
	
	/**
	 * Uses con to build a PreparedStatement object that can be used to update the table BICING_FACT
	 * @throws SQLException 
	 * */
	public PreparedStatement buildBicingDimTime(Connection con) throws SQLException {
		
		String upsertBicingTimeString = "UPSERT INTO BICING_DIM_TIME VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		return con.prepareStatement(upsertBicingTimeString);
	}
	
	/**
	 * Maps date to date sixths according to 
	 *  -- VALUES: '[04:00 - 08:00)', '[08:00 - 12:00)', 
	 *  --         '[12:00 - 16:00)', '[16:00 - 20:00)', 
	 *  --         '[20:00 - 00:00)', '[00:00 - 04:00)'
	 *  
	 *  FIXME: all these constants should be moved into a configuration file
	 * */
	private static String dateToDaySixth(DateTime date) {
		
		int hour = date.getHourOfDay();
		
		if (hour < 4) {
			return "[00:00 - 04:00)";
		}
		
		if (hour >= 4 && hour < 8) {
			return "[04:00 - 08:00)";
		}
		
		if (hour >= 8 && hour < 12) {
			return "[08:00 - 12:00)";
		}
		
		if (hour >= 12 && hour < 16) {
			return "[12:00 - 16:00)";
		}
		
		if (hour >= 16 && hour < 20) {
			return "[16:00 - 20:00)";
		}
		
		if (hour >= 20) {
			return "[20:00 - 00:00)";
		}
	
		throw new RuntimeException("Date " + date + "has an hour value outside the interval [0, 23]");
	}
	
	/**
	 * Maps date to date parts according to day parts (http://en.wikipedia.org/wiki/Rush_hour) 
	 * adapted to the Spanish cycles
	 * VALUE: 'GO-WORK' (rush hour going to work: [06:00 - 10:00)), 
	 * -- 'MORNING' ([10:00 - 13:00)), 'LUNCH' (spanish lunch [13:00, 15:00)),
	 * -- 'AFTERNOON' (afternoon, [15:00 - 17:00), 'BACK-HOME' (spanish rush hour for 
	 * -- going back home: [17:00 - 19:00), 'NIGHT' [19:00, 06:00)
	 * 
	 * FIXME: all these constants should be moved into a configuration file
	 * */
	private static String dateToDayParts(DateTime date) {
		int hour = date.getHourOfDay();
		
		if (hour >= 6 && hour < 10) {
			return "GO-WORK";
		}
		
		if (hour >= 10 && hour < 13) {
			return "MORNING";
		}

		if (hour >= 13 && hour < 15) {
			return "LUNCH";
		}

		if (hour >= 15 && hour < 17) {
			return "AFTERNOON";
		}

		if (hour >= 17 && hour < 19) {
			return "BACK-HOME";
		}
		
		if (hour >= 19 || hour < 6) {
			return "NIGHT";
		}
		
		throw new RuntimeException("Date " + date + "has an hour value outside the interval [0, 23]");
	}
	
	public void loadBicingDimTimeStatement(BicingStationDao.Value stationInfo, PreparedStatement stmtBicingDimTime) throws SQLException {
		DateTime stationTimetagDatetime = new DateTime(new Date(stationInfo.updatetime() * 1000));
		// Joda time doesn't support week of month
		SimpleDateFormat weekOfMonthFormat = new SimpleDateFormat("W");

		// 1.	    TIMETAG TIMESTAMP NOT NULL,
		stmtBicingDimTime.setTimestamp(1, new Timestamp(stationInfo.updatetime() * 1000));
		// -- small values, all in the same column
		// -- 0 to 59
		// 2.	    D.MINUTE UNSIGNED_TINYINT,
		stmtBicingDimTime.setInt(2, stationTimetagDatetime.getMinuteOfHour());
		// 	    -- 0 to 23
		// 3.	    D.HOUR UNSIGNED_TINYINT,
		stmtBicingDimTime.setInt(3, stationTimetagDatetime.getHourOfDay());
		// 	    -- VALUES: '[04:00 - 08:00)', '[08:00 - 12:00)', 
		// 	    --         '[12:00 - 16:00)', '[16:00 - 20:00)', 
		// 	    --         '[20:00 - 00:00)', '[00:00 - 04:00)'
		// 4.	    D.DAYSIXTH VARCHAR(5),
		stmtBicingDimTime.setString(4, dateToDaySixth(stationTimetagDatetime));
		// 	    -- Day parts: http://en.wikipedia.org/wiki/Rush_hour
		// 	    -- VALUE: 'GO-WORK' (rush hour going to work: [06:00 - 10:00)), 
		// 	    -- 'MORNING' ([10:00 - 13:00)), 'LUNCH' (spanish lunch [13:00, 15:00)),
		// 	    -- 'AFTERNOON' (afternoon, [15:00 - 17:00), 'BACK-HOME' (spanish rush hour for 
		// 	    -- going back home: [17:00 - 19:00), 'NIGHT' [19:00, 06:00)
		// 5.	    D.PART VARCHAR(9),
		stmtBicingDimTime.setString(5, dateToDayParts(stationTimetagDatetime));
		// 	    -- 1 to 30
		// 6.	    D.MONTH_DAY UNSIGNED_TINYINT, 
		stmtBicingDimTime.setInt(6, stationTimetagDatetime.getDayOfMonth());
		// 	    -- 1 to 365
		// 7.	    D.YEAR_DAY UNSIGNED_SMALLINT, 
		stmtBicingDimTime.setInt(7, stationTimetagDatetime.getDayOfYear());
		// 	    -- 1 to 5
		// 8.	    D.MONTH_WEEK UNSIGNED_TINYINT, 
		stmtBicingDimTime.setInt(8, Integer.parseInt(weekOfMonthFormat.format(stationTimetagDatetime.toDate())));
		// 	    -- 1 to 53
		// 9.	    D.YEAR_WEEK UNSIGNED_TINYINT, 
		stmtBicingDimTime.setInt(9, stationTimetagDatetime.getWeekOfWeekyear());
		// 	    -- 1 to 12
		// 10.	    D.MONTH UNSIGNED_TINYINT,
		stmtBicingDimTime.setInt(10, stationTimetagDatetime.getMonthOfYear());
		// 	    -- 1 to 4
		// 11.	    D.TRIMESTER UNSIGNED_TINYINT,
		stmtBicingDimTime.setInt(11, (int) Math.floor((stationTimetagDatetime.getMonthOfYear() - 1) / 3) + 1);
		// 	    -- e.g. 2014
		// 12.	    D.YEAR UNSIGNED_SMALLINT
		stmtBicingDimTime.setInt(12, stationTimetagDatetime.getYear());

	}
	
	public PreparedStatement buildCheckExistsTimetagDimTime(Connection con) throws SQLException {
		String stmtCheckExistsTimetagDimTimeStr = "SELECT TIMETAG from BICING_DIM_TIME WHERE TIMETAG = ?";
		return con.prepareStatement(stmtCheckExistsTimetagDimTimeStr);
	}
	
	/**
	 * Returns true iff exists some row in BICING_DIM_TIME with the same timetag as stationInfo, 
	 * otherwise returns false
	 * */
	public boolean checkExistsTimetagDimTime(BicingStationDao.Value stationInfo, PreparedStatement stmtCheckExistsTimetagDimTime) throws SQLException {
		stmtCheckExistsTimetagDimTime.setTimestamp(1, new Timestamp(stationInfo.updatetime() * 1000));
		ResultSet queryResults = stmtCheckExistsTimetagDimTime.executeQuery();
		return queryResults.next();
	}

}
