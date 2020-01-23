package com.github.vincentrussell.query.mongodb.sql.converter.visitor;

import org.bson.Document;

import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;

//Generate lookup lets from on clause. All fields without table fields 
public class OnVisitorMatchLookupBuilder extends ExpressionVisitorAdapter{
	private String joinAliasTable;
	
	public OnVisitorMatchLookupBuilder(String joinAliasTable) {
		this.joinAliasTable = joinAliasTable;
	}
	
	@Override
    public void visit(Column column) {
		String name = column.getTable().getName();
		String columnName = column.getColumnName();
		column.setTable(null);
		if(!name.equals(joinAliasTable)) {
			column.setColumnName("$$" + columnName);
		}
		else {
			column.setColumnName("$" + columnName);
		}
    }
}
