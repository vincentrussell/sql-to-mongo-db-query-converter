package com.github.vincentrussell.query.mongodb.sql.converter;

import com.github.vincentrussell.query.mongodb.sql.converter.util.DateFunction;
import com.github.vincentrussell.query.mongodb.sql.converter.util.RegexFunction;
import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WhereCauseProcessor {

    private final FieldType defaultFieldType;
    private final Map<String, FieldType> fieldNameToFieldTypeMapping;

    public WhereCauseProcessor(FieldType defaultFieldType, Map<String, FieldType> fieldNameToFieldTypeMapping) {
        this.defaultFieldType = defaultFieldType;
        this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping;
    }

    public Object parseExpression(Document query, Expression incomingExpression, Expression otherSide) throws ParseException {
        if (ComparisonOperator.class.isInstance(incomingExpression)) {
            RegexFunction regexFunction = SqlUtils.isRegexFunction(incomingExpression);
            DateFunction dateFunction = SqlUtils.isDateFunction(incomingExpression);
            if (regexFunction != null) {
                Document regexDocument = new Document("$regex", regexFunction.getRegex());
                if (regexFunction.getOptions() != null) {
                    regexDocument.append("$options", regexFunction.getOptions());
                }
                query.put(regexFunction.getColumn(), regexDocument);
            } else if (dateFunction!=null) {
                query.put(dateFunction.getColumn(),new Document(dateFunction.getComparisonExpression(),dateFunction.getDate()));
            } else if (EqualsTo.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((EqualsTo) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((EqualsTo) incomingExpression).getRightExpression();
                query.put(parseExpression(new Document(), leftExpression, rightExpression).toString(),parseExpression(new Document(), rightExpression, leftExpression));
            } else if (NotEqualsTo.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((NotEqualsTo) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((NotEqualsTo) incomingExpression).getRightExpression();
                query.put(SqlUtils.getStringValue(leftExpression), new Document("$ne", parseExpression(new Document(), rightExpression, leftExpression)));
            } else if (GreaterThan.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((GreaterThan) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((GreaterThan) incomingExpression).getRightExpression();
                query.put(leftExpression.toString(),new Document("$gt",parseExpression(new Document(), rightExpression, leftExpression)));
            } else if (MinorThan.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((MinorThan) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((MinorThan) incomingExpression).getRightExpression();
                query.put(leftExpression.toString(),new Document("$lt", parseExpression(new Document(), rightExpression, leftExpression)));
            } else if (GreaterThanEquals.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((GreaterThanEquals) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((GreaterThanEquals) incomingExpression).getRightExpression();
                query.put(leftExpression.toString(),new Document("$gte",parseExpression(new Document(), rightExpression, leftExpression)));
            } else if (MinorThanEquals.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((MinorThanEquals) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((MinorThanEquals) incomingExpression).getRightExpression();
                query.put(leftExpression.toString(),new Document("$lte", parseExpression(new Document(), rightExpression, leftExpression)));
            }
        } else if(LikeExpression.class.isInstance(incomingExpression)
                && Column.class.isInstance(((LikeExpression)incomingExpression).getLeftExpression())
                && (StringValue.class.isInstance(((LikeExpression)incomingExpression).getRightExpression()) ||
                Column.class.isInstance(((LikeExpression)incomingExpression).getRightExpression()))) {
            LikeExpression likeExpression = (LikeExpression)incomingExpression;
            String stringValueLeftSide = SqlUtils.getStringValue(likeExpression.getLeftExpression());
            String stringValueRightSide = SqlUtils.getStringValue(likeExpression.getRightExpression());
            Document document = new Document("$regex", "^" + SqlUtils.replaceRegexCharacters(stringValueRightSide) + "$");
            if (likeExpression.isNot()) {
                document = new Document("$not",new Document(stringValueLeftSide,document));
                throw new ParseException("NOT LIKE queries not supported");
            } else {
                document = new Document(stringValueLeftSide,document);
            }
            query.putAll(document);
        } else if(IsNullExpression.class.isInstance(incomingExpression)) {
            IsNullExpression isNullExpression = (IsNullExpression) incomingExpression;
            query.put(isNullExpression.getLeftExpression().toString(),new Document("$exists",isNullExpression.isNot()));
        } else if(InExpression.class.isInstance(incomingExpression)) {
            final InExpression inExpression = (InExpression) incomingExpression;
            final Expression leftExpression = ((InExpression) incomingExpression).getLeftExpression();
            final String leftExpressionAsString = SqlUtils.getStringValue(leftExpression);
            query.put(leftExpressionAsString,new Document(inExpression.isNot() ? "$nin" : "$in", Lists.transform(((ExpressionList)inExpression.getRightItemsList()).getExpressions(), new com.google.common.base.Function<Expression, Object>() {
                @Override
                public Object apply(Expression expression) {
                    try {
                        return parseExpression(new Document(),expression, leftExpression);
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                }
            })));
        } else if(AndExpression.class.isInstance(incomingExpression)) {
            handleAndOr("$and", (BinaryExpression)incomingExpression, query);
        } else if(OrExpression.class.isInstance(incomingExpression)) {
            handleAndOr("$or", (BinaryExpression)incomingExpression, query);
        } else if(Parenthesis.class.isInstance(incomingExpression)) {
            Parenthesis parenthesis = (Parenthesis) incomingExpression;
            Object expression = parseExpression(new Document(), parenthesis.getExpression(), null);
            if (parenthesis.isNot()) {
                return new Document("$nor", Arrays.asList(expression));
            }
            return expression;
        } else if (NotExpression.class.isInstance(incomingExpression) && otherSide == null) {
            Expression expression = ((NotExpression)incomingExpression).getExpression();
            return new Document(SqlUtils.getStringValue(expression), new Document("$ne", true));
        } else if (Function.class.isInstance(incomingExpression) && !SqlUtils.isSpecialtyFunction(incomingExpression)) {
            Function function = ((Function)incomingExpression);
            query.put("$"+ function.getName(), SqlUtils.parseFunctionArguments(function.getParameters(),
                    defaultFieldType, fieldNameToFieldTypeMapping));
        } else if (otherSide == null) {
            return new Document(SqlUtils.getStringValue(incomingExpression), true);
        } else {
            return SqlUtils.getValue(incomingExpression,otherSide, defaultFieldType, fieldNameToFieldTypeMapping);
        }
        return query;
    }

    private void handleAndOr(String key, BinaryExpression incomingExpression, Document query) throws ParseException {
        final Expression leftExpression = incomingExpression.getLeftExpression();
        final Expression rightExpression = incomingExpression.getRightExpression();

        List result = flattenOrsOrAnds(new ArrayList(), leftExpression, leftExpression, rightExpression);

        if (result != null) {
            query.put(key, Lists.reverse(result));
        } else {
            query.put(key, Arrays.asList(parseExpression(new Document(), leftExpression, rightExpression),
                    parseExpression(new Document(), rightExpression, leftExpression)));
        }
    }

    private List flattenOrsOrAnds(List arrayList, Expression firstExpression, Expression leftExpression, Expression rightExpression) throws ParseException {
            if (firstExpression.getClass().isInstance(leftExpression) &&
                    isOrAndExpression(leftExpression) && !isOrAndExpression(rightExpression)) {
                Expression left = ((BinaryExpression)leftExpression).getLeftExpression();
                Expression right = ((BinaryExpression)leftExpression).getRightExpression();
                arrayList.add(parseExpression(new Document(), rightExpression, null));
                List result = flattenOrsOrAnds(arrayList, firstExpression, left, right);
                if (result != null) {
                    return arrayList;
                }
            } else if (isOrAndExpression(firstExpression) && !isOrAndExpression(leftExpression) && !isOrAndExpression(rightExpression)) {
                arrayList.add(parseExpression(new Document(), rightExpression, null));
                arrayList.add(parseExpression(new Document(), leftExpression, null));
                return arrayList;
            } else {
                return null;
            }
            return null;
    }

    private boolean isOrAndExpression(Expression expression) {
        return OrExpression.class.isInstance(expression) || AndExpression.class.isInstance(expression);
    }

}
