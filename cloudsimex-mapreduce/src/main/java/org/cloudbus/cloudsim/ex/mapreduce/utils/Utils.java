package org.cloudbus.cloudsim.ex.mapreduce.utils;

public class Utils {
	
	public static boolean isNull(Object test) {
		if (test instanceof String) 
			return isNull((String)test);
		
		return test == null;
	}
	
	public static boolean isNull(String test) {
		return test == null || test.length() == 0 || test.equalsIgnoreCase("null") || test.trim().length() == 0;
	}

}
