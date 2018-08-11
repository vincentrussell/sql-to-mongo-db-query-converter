package com.github.vincentrussell.query.mongodb.sql.converter.util;

import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.List;

public class ObjectIdFunction {
    private final Object value;
    private final String column;
    private final Expression comparisonExpression;

    public ObjectIdFunction(String column, Object value, Expression expression)
        throws ParseException {
        this.column = column;
        this.value = value;
        this.comparisonExpression = expression;
    }

    public String getColumn() {
        return column;
    }

    public Object toDocument() throws ParseException {
        if (EqualsTo.class.isInstance(comparisonExpression)) {
            return new ObjectId(value.toString());
        } else if (NotEqualsTo.class.isInstance(comparisonExpression)) {
            return new Document("$ne", new ObjectId(value.toString()));
        } else if (InExpression.class.isInstance(comparisonExpression)) {
            InExpression inExpression = (InExpression) comparisonExpression;
            List<String> stringList = (List<String>) value;
            return new Document(inExpression.isNot() ? "$nin" : "$in", Lists.transform(stringList, new Function<String, ObjectId>() {
                @Override public ObjectId apply(String s) {
                    return new ObjectId(s);
                }
            }));
        }
        throw new ParseException("Count not convert ObjectId function into document");
    }
}
