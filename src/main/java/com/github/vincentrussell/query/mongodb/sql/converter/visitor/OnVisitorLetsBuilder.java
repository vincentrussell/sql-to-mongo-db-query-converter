package com.github.vincentrussell.query.mongodb.sql.converter.visitor;

import org.bson.Document;

import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;

//Generate lookup lets from on clause. All fields without table fields 
public class OnVisitorLetsBuilder extends ExpressionVisitorAdapter{
	private Document onDocument;
	private String joinAliasTable;
	
	public OnVisitorLetsBuilder(Document onDocument, String joinAliasTable) {
		this.onDocument = onDocument;
		this.joinAliasTable = joinAliasTable;
	}
	
	@Override
    public void visit(Column column) {
		if(!column.getTable().getName().equals(joinAliasTable)) {
			onDocument.put(column.getColumnName(), "$" + column.getColumnName());
		}
    }
}
