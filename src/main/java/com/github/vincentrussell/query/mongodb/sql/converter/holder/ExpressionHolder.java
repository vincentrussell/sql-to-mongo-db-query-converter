package com.github.vincentrussell.query.mongodb.sql.converter.holder;

import net.sf.jsqlparser.expression.Expression;

/**
 * Only for input/output visitor purposes.
 */
public class ExpressionHolder {
    private Expression expression;

    /**
     * Default constructor.
     * @param expression the expression
     */
    public ExpressionHolder(final Expression expression) {
        this.expression = expression;
    }

    /**
     * get the expression.
     * @return the expression.
     */
    public Expression getExpression() {
        return expression;
    }

    /**
     * set the expression.
     * @param expression the expression
     */
    public void setExpression(final Expression expression) {
        this.expression = expression;
    }

}
