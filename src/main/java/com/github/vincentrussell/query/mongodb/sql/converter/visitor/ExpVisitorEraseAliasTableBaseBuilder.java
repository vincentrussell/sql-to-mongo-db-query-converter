package com.github.vincentrussell.query.mongodb.sql.converter.visitor;

import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;

import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

//Generate lookup lets from on clause. All fields without table fields 
public class ExpVisitorEraseAliasTableBaseBuilder extends ExpressionVisitorAdapter{
	private String baseAliasTable;
	
	public ExpVisitorEraseAliasTableBaseBuilder(String baseAliasTable) {
		this.baseAliasTable = baseAliasTable;
	}
	
	@Override
    public void visit(Column column) {
		SqlUtils.removeAliasFromColumn(column, baseAliasTable);
    }
}
