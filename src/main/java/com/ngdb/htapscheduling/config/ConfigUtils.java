package com.ngdb.htapscheduling.config;

import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ConfigUtils {
	
	public static JSONObject getClusterConfig(String fileName) {
	    return getJsonFromFile("configs/cluster/" + fileName + ".json");
	}
	
	public static JSONObject getSchedulerConfig(String fileName) {
	    return getJsonFromFile("configs/scheduler/" + fileName + ".json");
	}
	
	public static JSONObject getMemoryMgmtConfig(String fileName) {
	    return getJsonFromFile("configs/mem_mgmt/" + fileName + ".json");
	}
	
	public static JSONObject getWorkloadConfig(String fileName) {
	    return getJsonFromFile("configs/workload/" + fileName + ".json");
	}
	
	
	public static String getAttributeValue(JSONObject object, String attribute) {
		if (object.get(attribute) instanceof Long) {
			return String.valueOf(object.get(attribute));
		} else { 
			return object.get(attribute).toString();
		}
	}
	
	public static JSONObject getJsonValue(JSONObject object, String attribute) {
		return (JSONObject) object.get(attribute);
	}
	
	private static JSONObject getJsonFromFile(String fileName) {
		JSONParser parser = new JSONParser();
		try {
			return (JSONObject) parser.parse(new FileReader(fileName));
		} catch (IOException | ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
}
