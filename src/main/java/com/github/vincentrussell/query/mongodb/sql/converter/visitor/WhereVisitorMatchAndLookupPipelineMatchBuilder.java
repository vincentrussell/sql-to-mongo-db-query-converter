package com.github.vincentrussell.query.mongodb.sql.converter.visitor;

import org.apache.commons.lang.mutable.MutableBoolean;

import com.github.vincentrussell.query.mongodb.sql.converter.holder.ExpressionHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.ValueListExpression;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.ExpressionListItem;
import net.sf.jsqlparser.statement.select.FunctionItem;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.Pivot;
import net.sf.jsqlparser.statement.select.PivotXml;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;

//Generate lookup match from where. For optimization, this must combine with "on" part of joined collection
//TODO: For optimization with multiple joins this visitor could return some map with table asociated where expression 
public class WhereVisitorMatchAndLookupPipelineMatchBuilder extends ExpressionVisitorAdapter{
	private String baseAliasTable;
	private ExpressionHolder outputMatch = null;//This expression will have the where part of baseAliasTable
	private MutableBoolean haveOrExpression = new MutableBoolean();//This flag will be true is there is some "or" expression. It that case match expression go in the main pipeline after lookup. TODO: Better optimization to get the baseTablePart even with "ors" in where 
	private boolean isBaseAliasOrValue;
	
	public WhereVisitorMatchAndLookupPipelineMatchBuilder(String baseAliasTable, ExpressionHolder outputMatch, MutableBoolean haveOrExpression) {
		this.baseAliasTable = baseAliasTable;
		this.outputMatch = outputMatch;
		this.haveOrExpression = haveOrExpression;
	}
	
	private ExpressionHolder setOrAndExpression(ExpressionHolder baseExp, Expression newExp) {
		Expression exp;
		if(baseExp.getExpression() != null) {
			exp = new AndExpression(baseExp.getExpression(), newExp);
		}
		else {
			exp = newExp;
		}
		baseExp.setExpression(exp);
		return baseExp;
	}
	
	@Override
    public void visit(Column column) {
		if(SqlUtils.isColumn(column)){
			this.isBaseAliasOrValue = SqlUtils.isTableAliasOfColumn(column, this.baseAliasTable);
		}
    }
	
	@Override
    public void visit(OrExpression expr) {
		this.haveOrExpression.setValue(true);
    }
	
	//Default  with expresion copy
    protected void visitBinaryExpression(BinaryExpression expr) {
    	this.isBaseAliasOrValue = true;
    	expr.getLeftExpression().accept(this);
    	if(!this.isBaseAliasOrValue) {
    		expr.getRightExpression().accept(this);
		}
    	else {
    		expr.getRightExpression().accept(this);
            if(this.isBaseAliasOrValue) {
    			this.setOrAndExpression(outputMatch,expr);
    		}
    	}
    }
    	
}
