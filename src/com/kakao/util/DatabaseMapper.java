package com.kakao.util;

import java.util.HashMap;
import java.util.Map;

public class DatabaseMapper {
	public static final String MAP_SEP = ",";
	public static final char MAP_KEY_VALUE_SEP = '=';
	public static final String MAP_IPADDR_DB_SEP = "/";
	
	boolean useDatabaseMap = false;
	private Map<String, String> databaseMap = new HashMap<String, String>();
	
	public void parseDatabaseMapping(String option) throws Exception{
		// option format is
		//      "ipaddress1/db=new_db1,ipaddress2/db=new_db2"
		
		if(option==null) return;
		option = option.trim();
		if(option.length()<=0) return;
		
		String[] mappings = option.split(MAP_SEP);
		for(int idx=0; idx<mappings.length; idx++){
			int pos = mappings[idx].indexOf(MAP_KEY_VALUE_SEP);
			if(pos<0){
				throw new Exception("Database mapping option has syntax error '"+mappings[idx]+"', Not found key_value_separator("+MAP_KEY_VALUE_SEP+")");
			}
			String ipAddressAndDatabase = mappings[idx].substring(0, pos).trim();
			String newDatabase = mappings[idx].substring(pos+1).trim();
			
			if(ipAddressAndDatabase.length()<=0 || newDatabase.length()<=0){
				throw new Exception("Database mapping option has syntax error '"+mappings[idx]+"', key or value is empty");
			}
			
			databaseMap.put(ipAddressAndDatabase.toLowerCase(), newDatabase.toLowerCase());
			useDatabaseMap = true;
		}
	}
	
	public String getNewDatabase(String sourceIp, String oldDatabase){
		if(sourceIp==null || oldDatabase==null || sourceIp.length()<=0 || oldDatabase.length()<=0)
			return null;
		
		if(useDatabaseMap){
			String key = sourceIp + MAP_IPADDR_DB_SEP + oldDatabase;
			return databaseMap.get(key.toLowerCase());
		}
		
		return null;
	}
}
