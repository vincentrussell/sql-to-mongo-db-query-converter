package com.github.vincentrussell.query.mongodb.sql.converter.holder;

import java.util.HashMap;

import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;

public class AliasHolder {
	private HashMap<String,String> aliasFromFieldHash;
    private HashMap<String,String> fieldFromAliasHash;
	
	public AliasHolder(HashMap<String,String> aliasFromFieldHash, HashMap<String,String> fieldFromAliasHash) {
		this.aliasFromFieldHash = aliasFromFieldHash;
		this.fieldFromAliasHash = fieldFromAliasHash;
	}

	public String getFieldExpFromAlias(String alias) {
		return fieldFromAliasHash.get(alias);
	}	
	
	public String getAliasFromFieldExp(String field) {
		return aliasFromFieldHash.get(field);
	}
	
	public String getFieldFromAliasOrField(String fieldOrAlias) throws ParseException {
		if(!isAmbiguous(fieldOrAlias)) {
			String field = fieldFromAliasHash.get(fieldOrAlias);
			if(field==null) {
				return fieldOrAlias;
			}
			else {
				return field;
			}
		}
		else {
			throw new ParseException("Ambiguous field: " + fieldOrAlias);
		}
	}
	
	public boolean isEmpty() {
		return aliasFromFieldHash.isEmpty();
	}
	
	private boolean isAmbiguous(String fieldOrAlias) {
		String aliasFromField = aliasFromFieldHash.get(fieldOrAlias);
		String fieldFromAlias = fieldFromAliasHash.get(fieldOrAlias);
		return aliasFromField != null && fieldFromAlias != null && !aliasFromField.equals(fieldFromAlias); 
	}
    
}
