package com.github.vincentrussell.query.mongodb.sql.converter.processor;

import net.sf.jsqlparser.expression.Expression;
import org.bson.Document;

/**
 * Class used to help ClauseProcessor abstract.
 *
 * @author maxid
 * @since 2024/8/21 09:11
 */
public interface ClauseProcessor {
    /**
     * parse expression.
     * @param query query
     * @param incomingExpression incoming expression
     * @param otherSide other expression
     * @return the converted mongo structure.
     * @throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException if there is an issue parsing
     * the incomingExpression
     */
    @SuppressWarnings("checkstyle:methodlength")
    Object parseExpression(Document query,
                           Expression incomingExpression, Expression otherSide)
            throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
}
