package com.github.vincentrussell.query.mongodb.sql.converter.visitor;

import com.github.vincentrussell.query.mongodb.sql.converter.holder.ExpressionHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.schema.Column;
import org.apache.commons.lang.mutable.MutableBoolean;

/**
 * Generate lookup match from where. For optimization, this must combine with "on" part of joined collection.
 */
public class WhereVisitorMatchAndLookupPipelineMatchBuilder extends ExpressionVisitorAdapter {
    private String baseAliasTable;
    /**
     * This expression will have the where part of baseAliasTable.
     */
    private ExpressionHolder outputMatch = null;
    /**
     * This flag will be true is there is some "or" expression.
     * It that case match expression go in the main pipeline after lookup.
     */
    private MutableBoolean haveOrExpression = new MutableBoolean();
    private boolean isBaseAliasOrValue;

    /**
     * Default constructor.
     * @param baseAliasTable
     * @param outputMatch
     * @param haveOrExpression
     */
    public WhereVisitorMatchAndLookupPipelineMatchBuilder(final String baseAliasTable,
                                                          final ExpressionHolder outputMatch,
                                                          final MutableBoolean haveOrExpression) {
        this.baseAliasTable = baseAliasTable;
        this.outputMatch = outputMatch;
        this.haveOrExpression = haveOrExpression;
    }

    private ExpressionHolder setOrAndExpression(final ExpressionHolder baseExp, final Expression newExp) {
        Expression exp;
        if (baseExp.getExpression() != null) {
            exp = new AndExpression(baseExp.getExpression(), newExp);
        } else {
            exp = newExp;
        }
        baseExp.setExpression(exp);
        return baseExp;
    }

    /**
     * {@inheritDoc}
     * @param column
     */
    @Override
    public void visit(final Column column) {
        if (SqlUtils.isColumn(column)) {
            this.isBaseAliasOrValue = SqlUtils.isTableAliasOfColumn(column, this.baseAliasTable);
        }
    }

    /**
     * {@inheritDoc}
     * @param expr
     */
    @Override
    public void visit(final OrExpression expr) {
        this.haveOrExpression.setValue(true);
    }

    /**
     * {@inheritDoc}
     * @param expr
     */
    @Override
    public void visit(final IsNullExpression expr) {
        if (this.isBaseAliasOrValue) {
            this.setOrAndExpression(outputMatch, expr);
        }
    }

    /**
     * {@inheritDoc}
     * @param expr
     */
    @Override
    protected void visitBinaryExpression(final BinaryExpression expr) {
        this.isBaseAliasOrValue = true;
        expr.getLeftExpression().accept(this);
        if (!this.isBaseAliasOrValue) {
            expr.getRightExpression().accept(this);
        } else {
            expr.getRightExpression().accept(this);
            if (this.isBaseAliasOrValue && !(expr instanceof AndExpression || expr instanceof OrExpression)) {
                this.setOrAndExpression(outputMatch, expr);
            }
        }
    }
}
