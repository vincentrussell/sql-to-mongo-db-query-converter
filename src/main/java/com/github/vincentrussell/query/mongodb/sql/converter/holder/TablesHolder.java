package com.github.vincentrussell.query.mongodb.sql.converter.holder;

import java.util.HashMap;
import java.util.Map;

public class TablesHolder {
	private String baseTable;
	private String baseAlias;
    private Map<String, String> aliasToTable = new HashMap<String,String>();
	private Map<String, String> tableToAlias = new HashMap<String,String>();
	
	private void addBaseTable(String table) {
		baseTable = table;
	}
	
	private void addBaseTable(String table, String alias) {
		baseTable = table;
		if(alias != null) {
			baseAlias = alias;
			aliasToTable.put(alias, table);
		}
		tableToAlias.put(table, alias);
	}
	
	public void addTable(String table) {
		if(baseTable != null) {
			tableToAlias.put(table, null);
		}
		else {
			addBaseTable(table);
		}
	}
	
	public void addTable(String table, String alias) {
		if(baseTable != null) {
			if(alias != null) {
				aliasToTable.put(alias, table);
			}
			tableToAlias.put(table, alias);
		}
		else {
			addBaseTable(table, alias);
		}
	}
    
    public String getBaseTable() {
    	return baseTable;
    }
    
    public String getBaseAliasTable() {
    	return baseAlias;
    }
    
    /**
     * Return null if not alias, otherwise original table  
     * **/
    
    public String isAlias(String posibleAlias) {
    	return (aliasToTable.containsKey(posibleAlias)?aliasToTable.get(posibleAlias):null);
    }
    
    /**
     * Return alias of table, otherwise null  
     * **/
    
    public String getAlias(String table) {
    	return (tableToAlias.containsKey(table)?tableToAlias.get(table):null);
    }
    
}
