package com.github.vincentrussell.query.mongodb.sql.converter.holder.from;

import net.sf.jsqlparser.parser.ParseException;

public interface SQLInfoHolder{
    
	public String getBaseTableName() throws ParseException;
	
}
