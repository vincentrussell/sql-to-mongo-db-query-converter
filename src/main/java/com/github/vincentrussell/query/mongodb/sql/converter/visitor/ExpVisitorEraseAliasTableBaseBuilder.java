package com.github.vincentrussell.query.mongodb.sql.converter.visitor;

import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;

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
     * @param column
     */
    @Override
    public void visit(final Column column) {
        SqlUtils.removeAliasFromColumn(column, baseAliasTable);
    }
}
