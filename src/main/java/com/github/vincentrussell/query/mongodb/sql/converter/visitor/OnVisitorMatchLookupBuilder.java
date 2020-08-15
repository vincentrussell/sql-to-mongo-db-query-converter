package com.github.vincentrussell.query.mongodb.sql.converter.visitor;

import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;

/**
 * Generate lookup subpipeline match step from on clause. For optimization,
 * this must combine with where part of joined collection
 */
public class OnVisitorMatchLookupBuilder extends ExpressionVisitorAdapter {
    private String joinAliasTable;
    private String baseAliasTable;

    /**
     * Default constructor.
     * @param joinAliasTable the alias for the join table.
     * @param baseAliasTable the alias for the base table.
     */
    public OnVisitorMatchLookupBuilder(final String joinAliasTable, final String baseAliasTable) {
        this.joinAliasTable = joinAliasTable;
        this.baseAliasTable = baseAliasTable;
    }

    /**
     * {@inheritDoc}
     * @param column
     */
    @Override
    public void visit(final Column column) {
        if (SqlUtils.isColumn(column)) {
            String columnName;
            if (column.getTable() != null) {
                columnName = SqlUtils.getColumnNameFromColumn(column);
            } else {
                columnName = column.getColumnName();
            }
            if (!SqlUtils.isTableAliasOfColumn(column, joinAliasTable)) {
                if (column.getTable() == null || SqlUtils.isTableAliasOfColumn(column, baseAliasTable)) {
                    //we know let var don't have table inside
                    column.setColumnName("$$" + columnName.replace(".", "_").toLowerCase());
                } else {
                    column.setColumnName("$$"
                            + column.getName(false).replace(".", "_").toLowerCase());
                }
                column.setTable(null);
            } else {
                column.setTable(null);
                column.setColumnName("$" + columnName);
            }
        }
    }
}
