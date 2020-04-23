package com.github.vincentrussell.query.mongodb.sql.converter.processor;

import com.github.vincentrussell.query.mongodb.sql.converter.FieldType;
import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.AliasHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import net.sf.jsqlparser.expression.*;
import org.bson.Document;

import java.util.Map;

public class HavingCauseProcessor extends WhereCauseProcessor{

	AliasHolder aliasHolder;
	
	public HavingCauseProcessor(FieldType defaultFieldType, Map<String, FieldType> fieldNameToFieldTypeMapping) {
        super(defaultFieldType,fieldNameToFieldTypeMapping);
    }
	
	public void setAliasHolder(AliasHolder aliasHolder) {
		this.aliasHolder = aliasHolder;
	}

    @Override //for use alias because it's after group expression so alias is already applied
    protected Object recurseFunctions(Document query, Object object, FieldType defaultFieldType, Map<String, FieldType> fieldNameToFieldTypeMapping) throws ParseException {
    	if (Function.class.isInstance(object)) {
            Function function = (Function)object;
            String strFunction = function.toString();
            if(SqlUtils.isAggregateExp(strFunction)) {
            	String alias = aliasHolder.getAliasFromFieldExp(function.toString());
        		return "$" + SqlUtils.generateAggField(function, alias);
            }
    	}
    	return super.recurseFunctions(query, object, defaultFieldType, fieldNameToFieldTypeMapping);
    }

}
