package com.github.vincentrussell.query.mongodb.sql.converter.holder.from;

import java.util.HashMap;
import java.util.Map;

import com.github.vincentrussell.query.mongodb.sql.converter.FieldType;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;

public class FromHolder {
	private FieldType defaultFieldType;
	private Map<String, FieldType> fieldNameToFieldTypeMapping; 
	
	private FromItem baseFrom;
	private String baseAlias;
    private Map<String, FromItem> aliasToTable = new HashMap<String,FromItem>();
	private Map<FromItem, String> tableToAlias = new HashMap<FromItem,String>();
	private Map<FromItem, SQLInfoHolder> fromToSQLHolder = new HashMap<FromItem,SQLInfoHolder>();
	
	public FromHolder(FieldType defaultFieldType, Map<String, FieldType> fieldNameToFieldTypeMapping) {
		this.defaultFieldType = defaultFieldType;
		this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping;
	}
	
	private void addToSQLHolderMap(FromItem from) throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
		if (from instanceof Table) {
			Table table = (Table)from;
			fromToSQLHolder.put(table, new SQLTableInfoHolder(table.getName()));
    	}
    	else if(from instanceof SubSelect){
    		SubSelect subselect = (SubSelect) from; 
    		fromToSQLHolder.put(from, SQLCommandInfoHolder.Builder
                    .create(defaultFieldType, fieldNameToFieldTypeMapping)
                    .setPlainSelect((PlainSelect)subselect.getSelectBody())
                    .build());
    	}
    	else {//Not happen SubJoin, not supported previously
    		return;
    	}
	}
	
	private void addBaseFrom(FromItem from) throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
		baseFrom = from;
		addToSQLHolderMap(from);
	}
	
	private void addBaseFrom(FromItem from, String alias) throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
		addBaseFrom(from);
		if(alias != null) {
			baseAlias = alias;
			aliasToTable.put(alias, from);
		}
		tableToAlias.put(from, alias);
	}
	
	public void addFrom(FromItem from) throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
		if(baseFrom != null) {
			tableToAlias.put(from, null);
			addToSQLHolderMap(from);
		}
		else {
			addBaseFrom(from);
		}
	}
	
	public void addFrom(FromItem from, String alias) throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
		if(baseFrom != null) {
			if(alias != null) {
				aliasToTable.put(alias, from);
			}
			tableToAlias.put(from, alias);
			addToSQLHolderMap(from);
		}
		else {
			addBaseFrom(from, alias);
		}
	}
    
    public String getBaseFromTableName() throws ParseException {
    	return fromToSQLHolder.get(baseFrom).getBaseTableName();
    }
    
    public FromItem getBaseFrom() throws ParseException {
    	return baseFrom;
    }
    
    public SQLInfoHolder getBaseSQLHolder() throws ParseException {
    	return fromToSQLHolder.get(baseFrom);
    }
    
    public SQLInfoHolder getSQLHolder(FromItem fitem) throws ParseException {
    	return fromToSQLHolder.get(fitem);
    }
    
    public String getBaseAliasTable() {
    	return baseAlias;
    }
    
    /**
     * Return null if not alias, otherwise original table  
     * **/
    
    /*public String isAlias(String posibleAlias) {
    	return (aliasToTable.containsKey(posibleAlias)?aliasToTable.get(posibleAlias):null);
    }*/
    
    /**
     * Return alias of table, otherwise null  
     * **/
    
    public String getAlias(String table) {
    	return (tableToAlias.containsKey(table)?tableToAlias.get(table):null);
    }
    
}
