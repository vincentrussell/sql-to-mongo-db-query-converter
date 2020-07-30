package com.github.vincentrussell.query.mongodb.sql.converter.util;

import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;

/**
 * Object that serves as a wrapper around a date.
 */
public class DateFunction {
    private final Date date;
    private final String column;
    private String comparisonExpression = "$eq";

    /**
     * Default Constructor.
     * @param format
     * @param value
     * @param column
     * @param comparisonOperator
     * @throws ParseException
     */
    public DateFunction(final String format, final String value,
                        final String column, final ComparisonOperator comparisonOperator) throws ParseException {
        if ("natural".equals(format)) {
            this.date = SqlUtils.parseNaturalLanguageDate(value);
        } else {
            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(format).withZoneUTC();
            this.date = dateTimeFormatter.parseDateTime(value).toDate();
        }
        this.column = column;
        setComparisonFunction(comparisonOperator);
    }

    /**
     * get the date object.
     * @return the date
     */
    public Date getDate() {
        return new Date(date.getTime());
    }

    /**
     * get the column that is a date.
     * @return the column
     */
    public String getColumn() {
        return column;
    }

    private void setComparisonFunction(final ComparisonOperator comparisonFunction) throws ParseException {
        if (GreaterThanEquals.class.isInstance(comparisonFunction)) {
            this.comparisonExpression = "$gte";
        } else if (GreaterThan.class.isInstance(comparisonFunction)) {
            this.comparisonExpression = "$gt";
        } else if (MinorThanEquals.class.isInstance(comparisonFunction)) {
            this.comparisonExpression = "$lte";
        } else if (MinorThan.class.isInstance(comparisonFunction)) {
            this.comparisonExpression = "$lt";
        } else {
            throw new ParseException("could not parseNaturalLanguageDate string expression: "
                    + comparisonFunction.getStringExpression());
        }
    }

    /**
     * get comparison expression for this Date function.
     * @return the comparison expression
     */
    public String getComparisonExpression() {
        return comparisonExpression;
    }
}
