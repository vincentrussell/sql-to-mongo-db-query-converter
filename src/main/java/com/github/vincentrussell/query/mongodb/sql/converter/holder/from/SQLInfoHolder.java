package com.github.vincentrussell.query.mongodb.sql.converter.holder.from;

import net.sf.jsqlparser.parser.ParseException;

/**
 * Interface for classes that hold information.
 */
public interface SQLInfoHolder {

    /**
     * get the base table name.
     * @return the base table name
     * @throws ParseException
     */
    String getBaseTableName() throws ParseException;

}
