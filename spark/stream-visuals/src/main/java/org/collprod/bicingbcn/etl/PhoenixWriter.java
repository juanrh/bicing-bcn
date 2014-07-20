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

import com.google.auto.value.AutoValue;
import com.google.common.base.Optional;

/**
 * JDBI (http://www.jdbi.org/) could have been a good option for this
 * */
public class PhoenixWriter implements Serializable {

	private static final long serialVersionUID = -8620289406311041119L;
	
	
	/**
	 * POJO for a record of the table BICING_DIM_STATION
	 * 
	 * 
		CREATE TABLE IF NOT EXISTS BICING_DIM_STATION (
		    ID UNSIGNED_LONG NOT NULL,
		    -- geo info
		    GEO.LONGITUDE UNSIGNED_DOUBLE,
		    GEO.LATITUDE UNSIGNED_DOUBLE,
		    GEO.HEIGH UNSIGNED_LONG,
		    -- human readable location 
		    LOC.DISTRICT VARCHAR,
		    LOC.NEIGHBORHOOD VARCHAR,
		    LOC.POSTAL_CODE VARCHAR,
		    LOC.ADDRESS VARCHAR,
		    -- district info
		    DISTRICT.POP_DENSITY UNSIGNED_DOUBLE,
		    DISTRICT.POPULATION UNSIGNED_LONG,
		    DISTRICT.SIZE UNSIGNED_DOUBLE
		    -- 
		    CONSTRAINT PK PRIMARY KEY (ID)
		);
	 * */
	@AutoValue
	public static abstract class DimStationRecord implements Serializable {
		private static final long serialVersionUID = 2645638526992521100L;
		
		DimStationRecord() {}
		
		public static DimStationRecord create (long id, 
				Optional<Double> longitude, Optional<Double> latitude, Optional<Long> heigh,
				Optional<String> district, Optional<String> neighborhood, Optional<String> postalCode, Optional<String> address,
				Optional<Double> popDensity, Optional<Long> population, Optional<Double> size) {
			return new AutoValue_PhoenixWriter_DimStationRecord(id, longitude, latitude, heigh,
					 									district, neighborhood, postalCode, address,
					 									popDensity, population, size);
			}
		public abstract long id();
		public abstract Optional<Double> longitude();
		public abstract Optional<Double> latitude();
		public abstract Optional<Long> heigh();
		public abstract Optional<String> district();
		public abstract Optional<String> neighborhood();
		public abstract Optional<String> postalCode();
		public abstract Optional<String> address();
		public abstract Optional<Double> popDensity();
		public abstract Optional<Long> population();
		public abstract Optional<Double> size();
	}
	
	private DimStationRecord resultsetToStationRecord(ResultSet stationInfoResults) throws SQLException {
		// will take just first result
		// move cursor to first result
		stationInfoResults.next();

		Double longitude = stationInfoResults.getDouble(2);
		longitude = stationInfoResults.wasNull() ? null : longitude;

		Double latitude = stationInfoResults.getDouble(3);
		latitude = stationInfoResults.wasNull() ? null : latitude;

		Long heigh = stationInfoResults.getLong(4);
		heigh = stationInfoResults.wasNull() ? null : heigh;

		String district = stationInfoResults.getString(5); 
		district = (stationInfoResults.wasNull() || district.equals("NULL")) ? null : district;

		String neighborhood = stationInfoResults.getString(6); 
		neighborhood = (stationInfoResults.wasNull() || neighborhood.equals("NULL")) ? null : neighborhood;

		String postalCode = stationInfoResults.getString(7); 
		postalCode = (stationInfoResults.wasNull() || postalCode.equals("NULL")) ? null : postalCode;

		String address = stationInfoResults.getString(8); 
		address = (stationInfoResults.wasNull() || address.equals("NULL")) ? null : address;

		Double popDensity = stationInfoResults.getDouble(9); 
		popDensity = stationInfoResults.wasNull() ? null : popDensity;

		Long population = stationInfoResults.getLong(10); 
		population = stationInfoResults.wasNull() ? null : population;

		Double size = stationInfoResults.getDouble(11);
		size = stationInfoResults.wasNull() ? null : size;

		PhoenixWriter.DimStationRecord stationInfo = PhoenixWriter.DimStationRecord
				.create(stationInfoResults.getLong(1), 
						Optional.fromNullable(longitude), Optional.fromNullable(latitude), Optional.fromNullable(heigh), 
						Optional.fromNullable(district), Optional.fromNullable(neighborhood), Optional.fromNullable(postalCode), Optional.fromNullable(address), 
						Optional.fromNullable(popDensity), Optional.fromNullable(population), Optional.fromNullable(size));
		return stationInfo;
	}
	
	/**
	 * Is not responsible from closing con
	 * */
	public PreparedStatement buildLookupStationStatement(Connection con) throws SQLException {
		String stmtGetStationInfoStr = "SELECT * from BICING_DIM_STATION WHERE ID = ?";
		PreparedStatement stmtGetStationInfo = con.prepareStatement(stmtGetStationInfoStr);
		return stmtGetStationInfo;
	}
	
	
	/**
	 * Is not responsible from closing stmtGetStationInfo
	 * */
	public DimStationRecord lookupStationRecord(PreparedStatement stmtGetStationInfo, Long stationId) throws SQLException {
			// Execute the query for the station id specified
			stmtGetStationInfo.setLong(1, stationId);
			ResultSet stationInfoResults = stmtGetStationInfo.executeQuery();
			
			return resultsetToStationRecord(stationInfoResults);
	}
	

	/**
	 * Uses con to build a PreparedStatement object that can be used to update the table BICING_FACT
	 * @throws SQLException 
	 * */
	public PreparedStatement buildBicingBigTableStatement(Connection con) throws SQLException {
		String upsertBicingBigTableString = "UPSERT INTO BICING VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		return con.prepareStatement(upsertBicingBigTableString);
	}
	
	
	 /**
	 * Sets the fields of stmtBicingBigTable according to the values of stationInfo and lastBikeCount
	 * so it can be used to update the table BICING
	 * 
	 * @param stmtGetStationInfo is used to query BICING_DIM_STATION for the data for the station in stationInfo. This
	 * method fills the parameters of stmtGetStationInfo according to stationInfo
	 * 
	 * This method is not responsible for closing any statement
	 * 
	 * NOTE: The field slots is not used for computing the number of bikes lent and
			CREATE TABLE IF NOT EXISTS BICING (
			    -- Keys
			    ---
			    -- station to which the data for this row applies
		1.	    STATION UNSIGNED_LONG NOT NULL,
			    -- native apache phoenix meaning: The format is yyyy-MM-dd hh:mm:ss[.nnnnnnnnn]
			    -- Mapped to java.sql.Timestamp with an internal representation 
			    -- of the number of nanos from the epoch
		2.	    TIMETAG TIMESTAMP NOT NULL,
			    --
			    -- Fact fields
			    --
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
		8.	    F.RETURNED UNSIGNED_LONG,
			    --
			    -- Station dimension fields
			    --
			    -- geo info
		9.	    S.LONGITUDE UNSIGNED_DOUBLE,
		10.	    S.LATITUDE UNSIGNED_DOUBLE,
		11.	    S.HEIGH UNSIGNED_LONG,
			    -- human readable location 
		12.	    S.DISTRICT VARCHAR,
		13.	    S.NEIGHBORHOOD VARCHAR,
		14.	    S.POSTAL_CODE VARCHAR,
		15.	    S.ADDRESS VARCHAR,
			    -- district info
		16.	    S.POP_DENSITY UNSIGNED_DOUBLE,
		17.	    S.POPULATION UNSIGNED_LONG,
		18.	    S.SIZE UNSIGNED_DOUBLE,
			    --
			    -- Time dimension fields
			    -- small values, all in the same column
			    -- 0 to 59
		19.	    T.MINUTE UNSIGNED_TINYINT, 
			    -- 0 to 23
		20.	    T.HOUR UNSIGNED_TINYINT, 
			    -- VALUES: '[04:00 - 08:00)', '[08:00 - 12:00)', 
			    --         '[12:00 - 16:00)', '[16:00 - 20:00)', 
			    --         '[20:00 - 00:00)', '[00:00 - 04:00)'
		21.	    T.DAYSIXTH VARCHAR(15),
			    -- Day parts: http://en.wikipedia.org/wiki/Rush_hour
			    -- VALUE: 'GO-WORK' (rush hour going to work: [06:00 - 10:00)), 
			    -- 'MORNING' ([10:00 - 13:00)), 'LUNCH' (spanish lunch [13:00, 15:00)),
			    -- 'AFTERNOON' (afternoon, [15:00 - 17:00), 'BACK-HOME' (spanish rush hour for 
			    -- going back home: [17:00 - 19:00), 'NIGHT' [19:00, 06:00)
		22.	    T.PART VARCHAR(9),
			    -- 1 to 30
		23.	    T.MONTH_DAY UNSIGNED_TINYINT, 
			    -- 1 to 365
		24.	    T.YEAR_DAY UNSIGNED_SMALLINT, 
			    -- 1 to 5
		25.	    T.MONTH_WEEK UNSIGNED_TINYINT, 
			    -- 1 to 53
		26.	    T.YEAR_WEEK UNSIGNED_TINYINT, 
			    -- 1 to 12
		27.	    T.MONTH UNSIGNED_TINYINT, 
			    -- 1 to 4
		28.	    T.TRIMESTER UNSIGNED_TINYINT, 
			    -- e.g. 2014
		29.	    T.YEAR UNSIGNED_SMALLINT
			
			    -- This gives a non monotonically increasing row key
			    CONSTRAINT PK PRIMARY KEY (STATION, TIMETAG)
);
	 * */
	public void loadBicingBigTableStatement(BicingStationDao.Value stationInfo, int lastBikeCount, PreparedStatement stmtBicingBigTable, PreparedStatement stmtGetStationInfo) throws SQLException {
		boolean isStationOpen = stationInfo.status().equals("OPN");
		boolean notValidState = lastBikeCount < 0;
		
	    // -- station to which the data for this row applies
		// 1.	    STATION UNSIGNED_LONG NOT NULL,
		stmtBicingBigTable.setInt(1, stationInfo.id());
		
		// 2.  TIMETAG TIMESTAMP NOT NULL,
		stmtBicingBigTable.setTimestamp(2, new Timestamp(stationInfo.updatetime() * 1000)); // TODO ensure correct translation to milliseconds
		
		// -- Degenerate dimension: 'OPN' (open) or 'CLS' (closed)
		// 3.	    F.STATUS VARCHAR(3),
		stmtBicingBigTable.setString(3,  stationInfo.status());
		
		// -- Number of parking slots available: should be 0 
	    // -- if F.STATUS is "CLS"
		// 4.	    F.SLOTS UNSIGNED_LONG,
		stmtBicingBigTable.setInt(4, isStationOpen ? stationInfo.slots() : 0);
		
	    // -- Number of bikes available: should be 0 
	    // -- if F.STATUS is "CLS"
	    // 5.	    F.AVAILABLE UNSIGNED_LONG,
		stmtBicingBigTable.setInt(5, isStationOpen ? stationInfo.bikes() : 0);
		
		// 6.	    F.CAPACITY UNSIGNED_LONG,
		// -- Number of bikes lent for this station since 
		// -- the previous update
		stmtBicingBigTable.setInt(6, isStationOpen ? stationInfo.slots() + stationInfo.bikes() : 0);
		
		// 7.	    F.LENT UNSIGNED_LONG,
		// -- Number of bikes returned to this station since 
		// -- the previous update		
		//  - if lastBikeCount is negative then we don't have info and we return as nothing
		// 	- if we have more bikes now than in the previous update we assume no bike has been lent,  
		//    this implies an error if bikes are returned and taken between updates
		stmtBicingBigTable.setInt(7, notValidState ? 0 : Math.max(lastBikeCount - stationInfo.bikes(), 0));
		
		
		// -- Number of bikes returned to this station since 
		// -- the previous update
		// 8.	    F.RETURNED UNSIGNED_LONG,
		// same compromises as the previous value
		stmtBicingBigTable.setInt(8, notValidState ? 0 : Math.max(stationInfo.bikes() - lastBikeCount, 0));
				
		//	    --
		//	    -- Station dimension fields
		//	    --
		// Get station info: FIXME: BicingStationDao.Value uses int for station ids while the
		// table BICING_DIM_STATION uses UNSIGNED_LONG
		DimStationRecord stationDimStationInfo = lookupStationRecord(stmtGetStationInfo, (long) stationInfo.id());
		
		//	    -- geo info
		
		//9.	    S.LONGITUDE UNSIGNED_DOUBLE,
		if (stationDimStationInfo.longitude().isPresent()) {
			stmtBicingBigTable.setDouble(9, stationDimStationInfo.longitude().get());
		} else {
			stmtBicingBigTable.setNull(9, java.sql.Types.DOUBLE);
		}
			
		//10.	    S.LATITUDE UNSIGNED_DOUBLE,
		if (stationDimStationInfo.latitude().isPresent()) {
			stmtBicingBigTable.setDouble(10, stationDimStationInfo.latitude().get());
		} else {
			stmtBicingBigTable.setNull(10, java.sql.Types.DOUBLE);
		}
		
		//11.	    S.HEIGH UNSIGNED_LONG,
		if (stationDimStationInfo.heigh().isPresent()) {
			stmtBicingBigTable.setLong(11, stationDimStationInfo.heigh().get());
		} else {
			stmtBicingBigTable.setNull(11, java.sql.Types.INTEGER);
		}
		
		//	    -- human readable location 
		
		//12.	    S.DISTRICT VARCHAR,
		if (stationDimStationInfo.district().isPresent()) {
			stmtBicingBigTable.setString(12, stationDimStationInfo.district().get());
		} else {
			stmtBicingBigTable.setNull(12, java.sql.Types.VARCHAR);
		}
		
		//13.	    S.NEIGHBORHOOD VARCHAR,
		if (stationDimStationInfo.neighborhood().isPresent()) {
			stmtBicingBigTable.setString(13, stationDimStationInfo.neighborhood().get());
		} else {
			stmtBicingBigTable.setNull(13, java.sql.Types.VARCHAR);
		}
		
		//14.	    S.POSTAL_CODE VARCHAR,
		if (stationDimStationInfo.postalCode().isPresent()) {
			stmtBicingBigTable.setString(14, stationDimStationInfo.postalCode().get());
		} else {
			stmtBicingBigTable.setNull(14, java.sql.Types.VARCHAR);
		}
		
		//15.	    S.ADDRESS VARCHAR,
		if (stationDimStationInfo.address().isPresent()) {
			stmtBicingBigTable.setString(15, stationDimStationInfo.address().get());
		} else {
			stmtBicingBigTable.setNull(15, java.sql.Types.VARCHAR);
		}
		
		//	    -- district info
		
		//16.	    S.POP_DENSITY UNSIGNED_DOUBLE,
		if (stationDimStationInfo.popDensity().isPresent()) {
			stmtBicingBigTable.setDouble(16, stationDimStationInfo.popDensity().get());
		} else {
			stmtBicingBigTable.setNull(16, java.sql.Types.DOUBLE);
		}
		
		//17.	    S.POPULATION UNSIGNED_LONG,
		if (stationDimStationInfo.population().isPresent()) {
			stmtBicingBigTable.setLong(17, stationDimStationInfo.population().get());
		} else {
			stmtBicingBigTable.setNull(17, java.sql.Types.INTEGER);
		}
		
		//18.	    S.SIZE UNSIGNED_DOUBLE,
		if (stationDimStationInfo.size().isPresent()) {
			stmtBicingBigTable.setDouble(18, stationDimStationInfo.size().get());
		} else {
			stmtBicingBigTable.setNull(18, java.sql.Types.DOUBLE);
		}
		
		//	    -- Time dimension fields
		DateTime stationTimetagDatetime = new DateTime(new Date(stationInfo.updatetime() * 1000));
		// Joda time doesn't support week of month
		SimpleDateFormat weekOfMonthFormat = new SimpleDateFormat("W");
		
		//	    -- 0 to 59
		//19.	    T.MINUTE UNSIGNED_TINYINT,
		stmtBicingBigTable.setInt(19, stationTimetagDatetime.getMinuteOfHour());
		
		//	    -- 0 to 23
		//20.	    T.HOUR UNSIGNED_TINYINT, 
		stmtBicingBigTable.setInt(20, stationTimetagDatetime.getHourOfDay());
		
		//	    -- VALUES: '[04:00 - 08:00)', '[08:00 - 12:00)', 
		//	    --         '[12:00 - 16:00)', '[16:00 - 20:00)', 
		//	    --         '[20:00 - 00:00)', '[00:00 - 04:00)'
		//21.	    T.DAYSIXTH VARCHAR(15),
		stmtBicingBigTable.setString(21, dateToDaySixth(stationTimetagDatetime));
		
		//	    -- Day parts: http://en.wikipedia.org/wiki/Rush_hour
		//	    -- VALUE: 'GO-WORK' (rush hour going to work: [06:00 - 10:00)), 
		//	    -- 'MORNING' ([10:00 - 13:00)), 'LUNCH' (spanish lunch [13:00, 15:00)),
		//	    -- 'AFTERNOON' (afternoon, [15:00 - 17:00), 'BACK-HOME' (spanish rush hour for 
		//	    -- going back home: [17:00 - 19:00), 'NIGHT' [19:00, 06:00)
		//22.	    T.PART VARCHAR(9),
		stmtBicingBigTable.setString(22, dateToDayParts(stationTimetagDatetime));
		
		//	    -- 1 to 30
		//23.	    T.MONTH_DAY UNSIGNED_TINYINT,
		stmtBicingBigTable.setInt(23, stationTimetagDatetime.getDayOfMonth());
		
		//	    -- 1 to 365
		//24.	    T.YEAR_DAY UNSIGNED_SMALLINT,
		stmtBicingBigTable.setInt(24, stationTimetagDatetime.getDayOfYear());
		
		//	    -- 1 to 5
		//25.	    T.MONTH_WEEK UNSIGNED_TINYINT,
		stmtBicingBigTable.setInt(25, Integer.parseInt(weekOfMonthFormat.format(stationTimetagDatetime.toDate())));
		
		//	    -- 1 to 53
		//26.	    T.YEAR_WEEK UNSIGNED_TINYINT,
		stmtBicingBigTable.setInt(26, stationTimetagDatetime.getWeekOfWeekyear());
		
		//	    -- 1 to 12
		//27.	    T.MONTH UNSIGNED_TINYINT,
		stmtBicingBigTable.setInt(27, stationTimetagDatetime.getMonthOfYear());
		
		//	    -- 1 to 4
		//28.	    T.TRIMESTER UNSIGNED_TINYINT,
		stmtBicingBigTable.setInt(28, (int) Math.floor((stationTimetagDatetime.getMonthOfYear() - 1) / 3) + 1);
		
		//	    -- e.g. 2014
		//29.	    T.YEAR UNSIGNED_SMALLINT
		stmtBicingBigTable.setInt(29, stationTimetagDatetime.getYear());
	}
	
	
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
