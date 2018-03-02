package com.github.vincentrussell.query.mongodb.sql.converter.util;

import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import net.sf.jsqlparser.expression.operators.relational.*;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;

public class DateFunction {
    private final Date date;
    private final String column;
    private String comparisonExpression = "$eq";

    public DateFunction(String format,String value, String column) {
        if ("natural".equals(format)) {
            this.date = SqlUtils.parseNaturalLanguageDate(value);
        } else {
            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(format).withZoneUTC();
            this.date = dateTimeFormatter.parseDateTime(value).toDate();
        }
        this.column = column;
    }


    public Date getDate() {
        return date;
    }

    public String getColumn() {
        return column;
    }

    public void setComparisonFunction(ComparisonOperator comparisonFunction) throws ParseException {
        if (GreaterThanEquals.class.isInstance(comparisonFunction)) {
            this.comparisonExpression = "$gte";
        } else if (GreaterThan.class.isInstance(comparisonFunction)) {
            this.comparisonExpression = "$gt";
        } else if (MinorThanEquals.class.isInstance(comparisonFunction)) {
            this.comparisonExpression = "$lte";
        } else if (MinorThan.class.isInstance(comparisonFunction)) {
            this.comparisonExpression = "$lt";
        } else {
            throw new ParseException("could not parseNaturalLanguageDate string expression: " + comparisonFunction.getStringExpression());
        }
    }

    public String getComparisonExpression() {
        return comparisonExpression;
    }
}