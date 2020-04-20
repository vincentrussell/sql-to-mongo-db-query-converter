package com.github.vincentrussell.query.mongodb.sql.converter.processor;

import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import java.util.HashMap;

public final class FunctionProcessor {
	
	private static HashMap<String,String> functionMapper;
	
	// Instantiating the static map 
    static
    { 
    	functionMapper = new HashMap<>(); 
    	functionMapper.put("OID", "toObjectId");
    	functionMapper.put("TIMESTAMP", "date"); 
    }
	
	
	public static String transcriptFunctionName(String functionName) throws ParseException {
		String transfunction = functionMapper.get(functionName);
		if(transfunction != null) {
			return transfunction;
		}
		else {
			return functionName;
		}
		
	}

}
