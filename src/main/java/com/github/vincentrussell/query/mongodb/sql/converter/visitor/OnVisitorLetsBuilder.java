package com.github.vincentrussell.query.mongodb.sql.converter.visitor;

import org.bson.Document;

import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;

import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;

//Generate lookup lets from on clause. All fields without table fields 
public class OnVisitorLetsBuilder extends ExpressionVisitorAdapter{
	private Document onDocument;
	private String joinAliasTable;
	private String baseAliasTable;
	
	public OnVisitorLetsBuilder(Document onDocument, String joinAliasTable, String baseAliasTable) {
		this.onDocument = onDocument;
		this.joinAliasTable = joinAliasTable;
		this.baseAliasTable = baseAliasTable;
	}
	
	@Override
    public void visit(Column column) {
		if(!SqlUtils.isTableAliasOfColumn(column, joinAliasTable)) {
			String columnName;
			if(SqlUtils.isTableAliasOfColumn(column, baseAliasTable)) {
				columnName = SqlUtils.getColumnNameFromColumn(column);
			}
			else {
				columnName = column.getName(false);
			}
			onDocument.put(columnName.replace(".", "_").toLowerCase(), "$" + columnName);
		}
    }
}
