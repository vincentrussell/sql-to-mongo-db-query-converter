package com.github.vincentrussell.query.mongodb.sql.converter.holder.from;

public class SQLTableInfoHolder implements SQLInfoHolder{
    private String baseTable;
    
    public SQLTableInfoHolder(String baseTable) {
    	this.baseTable = baseTable;
    }
    
    @Override
    public String getBaseTableName() {
    	return baseTable;
    }
	
}
