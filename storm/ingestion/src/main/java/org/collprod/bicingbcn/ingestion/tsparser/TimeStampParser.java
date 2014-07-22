package org.collprod.bicingbcn.ingestion.tsparser;

import java.io.Serializable;

import com.google.common.base.Function;
import com.google.common.base.Optional;

/**
 * Expected behaviour for the inherited apply() method 
 * 
 * @param data String corresponding to the data
 * @return a timestamp corresponding to the data as a long POSIX timestamp in UTC 
 * (number of milliseconds that have elapsed since 00:00:00 Coordinated Universal Time (UTC), 
 *  Thursday, 1 January 1970,[1][note 1] not counting leap seconds, see http://en.wikipedia.org/wiki/Unix_time).
 *  
 *  If no timestamp is found at data then the method should return an Optional value such that isPresent() 
 *  returns false, to mark the parsing error 
 * */
public interface TimeStampParser extends Function<String, Optional<Long>>, Serializable {
	/**
	 * @param data String corresponding to the data
	 * @return a String with a unique key for this data. This key will be used in case
	 * two data records for the same data source are ingested at the same time, which might 
	 * happen quite often in parallel ingestions 
	 * */
	Optional<String> getKey(String data);
}
