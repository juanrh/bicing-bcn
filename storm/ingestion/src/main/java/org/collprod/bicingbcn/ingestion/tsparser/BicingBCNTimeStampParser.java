package org.collprod.bicingbcn.ingestion.tsparser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BicingBCNTimeStampParser implements TimeStampParser {

	private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("<updatetime><!\\[CDATA\\[(\\d+)\\]\\]></updatetime>");
	// nice resource: http://www.regexplanet.com/advanced/java/index.html
	@Override
	public Long apply(String data) {
		/*
		 * Just match the <updatetime> tag with a regexp
		 * 
		 * */
		Matcher matcher = TIMESTAMP_PATTERN.matcher(data);
		if (matcher.find()) {
			try {
				return Long.parseLong(matcher.group(1));
			} catch (IllegalStateException ise) {
				System.err.println("Cannot parse timestamp for data " + data +  "\n:" + ise.getMessage());
			} catch (IndexOutOfBoundsException iobe) {
				System.err.println("Cannot parse timestamp for data " + data +  "\n:" + iobe.getMessage());
			}
		}
		
		// No match
		return null;
	}

}
