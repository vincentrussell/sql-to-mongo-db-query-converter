package com.github.vincentrussell.query.mongodb.sql.converter.visitor;

import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;
import org.bson.Document;

/**
 * Generate lookup lets from on clause. All fields without table fields.
 */
public class OnVisitorLetsBuilder extends ExpressionVisitorAdapter {
    private Document onDocument;
    private String joinAliasTable;
    private String baseAliasTable;

    /**
     * Default constructor.
     * @param onDocument the new document.
     * @param joinAliasTable the alias for the join table
     * @param baseAliasTable the alias for the base table
     */
    public OnVisitorLetsBuilder(final Document onDocument, final String joinAliasTable, final String baseAliasTable) {
        this.onDocument = onDocument;
        this.joinAliasTable = joinAliasTable;
        this.baseAliasTable = baseAliasTable;
    }

    /**
     * {@inheritDoc}
     * @param column
     */
    @Override
    public void visit(final Column column) {
        if (!SqlUtils.isTableAliasOfColumn(column, joinAliasTable)) {
            String columnName;
            if (SqlUtils.isTableAliasOfColumn(column, baseAliasTable)) {
                columnName = SqlUtils.getColumnNameFromColumn(column);
            } else {
                columnName = column.getName(false);
            }
            onDocument.put(columnName.replace(".", "_").toLowerCase(), "$" + columnName);
        }
    }
}
