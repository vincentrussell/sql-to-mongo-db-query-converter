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

/**
 * Wrapper around mongo ObjectId.
 */
public class ObjectIdFunction {
    private final Object value;
    private final String column;
    private final Expression comparisonExpression;

    /**
     * Default constructor.
     * @param column the column that is an objectId
     * @param value the value of the objectId
     * @param expression {@link Expression}
     */
    public ObjectIdFunction(final String column, final Object value, final Expression expression) {
        this.column = column;
        this.value = value;
        this.comparisonExpression = expression;
    }

    /**
     * get the column that is an ObjectId.
     * @return the column
     */
    public String getColumn() {
        return column;
    }

    /**
     * convert this ObjectId into a mongo document.
     * @return the mongo document
     * @throws ParseException when the objectId could not be converted into a document.
     */
    public Object toDocument() throws ParseException {
        if (EqualsTo.class.isInstance(comparisonExpression)) {
            return new ObjectId(value.toString());
        } else if (NotEqualsTo.class.isInstance(comparisonExpression)) {
            return new Document("$ne", new ObjectId(value.toString()));
        } else if (InExpression.class.isInstance(comparisonExpression)) {
            InExpression inExpression = (InExpression) comparisonExpression;
            List<String> stringList = (List<String>) value;
            return new Document(inExpression.isNot() ? "$nin" : "$in", Lists.transform(stringList,
                    new Function<String, ObjectId>() {
                        @Override
                        public ObjectId apply(final String s) {
                            return new ObjectId(s);
                        }
                    }));
        }
        throw new ParseException("could not convert ObjectId function into document");
    }
}
