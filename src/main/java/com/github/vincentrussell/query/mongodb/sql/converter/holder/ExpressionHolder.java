package com.github.vincentrussell.query.mongodb.sql.converter.holder;

import net.sf.jsqlparser.expression.Expression;

//Only for input/output visitor purposes
public class ExpressionHolder {
	private Expression expression;
	
	public ExpressionHolder(Expression expression) {
		this.expression = expression;
	}

	public Expression getExpression() {
		return expression;
	}

	public void setExpression(Expression expression) {
		this.expression = expression;
	}
    
}
