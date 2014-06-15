package org.collprod.bicingbcn.ingestion.tsparser;

import com.google.common.base.Function;

/**
 * Expected behaviour for the inherited apply() method 
 * 
 * @param data String corresponding to the data
 * @return a timestamp corresponding to the data as a long POSIX timestamp in UTC 
 * (number of seconds that have elapsed since 00:00:00 Coordinated Universal Time (UTC), 
 *  Thursday, 1 January 1970,[1][note 1] not counting leap seconds, see http://en.wikipedia.org/wiki/Unix_time).
 *  If no timestamp is found at data then the method should return null to mark this error 
 * */
public interface TimeStampParser extends Function<String, Long>{
}
