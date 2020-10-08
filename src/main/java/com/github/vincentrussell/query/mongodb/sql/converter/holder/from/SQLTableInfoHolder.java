package com.github.vincentrussell.query.mongodb.sql.converter.holder.from;

/**
 * Implementation of {@link SQLInfoHolder} meant to hold.
 */
public class SQLTableInfoHolder implements SQLInfoHolder {
    private String baseTable;

    /**
     * Default constructor.
     * @param baseTable the base table
     */
    public SQLTableInfoHolder(final String baseTable) {
        this.baseTable = baseTable;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getBaseTableName() {
        return baseTable;
    }

}
