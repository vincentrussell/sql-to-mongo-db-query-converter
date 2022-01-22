package com.github.vincentrussell.query.mongodb.sql.converter.visitor;

import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

/**
 * Generate lookup lets from on clause. All fields without table fields.
 */
public class ExpVisitorEraseAliasTableBaseBuilder extends ExpressionVisitorAdapter {
    private String baseAliasTable;

    /**
     * Default constructor.
     * @param baseAliasTable the alias for the base table
     */
    public ExpVisitorEraseAliasTableBaseBuilder(final String baseAliasTable) {
        this.baseAliasTable = baseAliasTable;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visit(final Column column) {
        SqlUtils.removeAliasFromColumn(column, baseAliasTable);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visit(final SelectExpressionItem selectExpressionItem) {
        SqlUtils.removeAliasFromSelectExpressionItem(selectExpressionItem, baseAliasTable);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void visit(final AllColumns allColumns) {
        //noop.... needed to avoid StackOverflowException
    }

}
