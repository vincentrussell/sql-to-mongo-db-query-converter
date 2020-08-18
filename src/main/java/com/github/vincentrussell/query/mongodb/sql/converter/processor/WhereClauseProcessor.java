package com.github.vincentrussell.query.mongodb.sql.converter.processor;

import com.github.vincentrussell.query.mongodb.sql.converter.FieldType;
import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.github.vincentrussell.query.mongodb.sql.converter.util.DateFunction;
import com.github.vincentrussell.query.mongodb.sql.converter.util.ObjectIdFunction;
import com.github.vincentrussell.query.mongodb.sql.converter.util.RegexFunction;
import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Class responsible for parsing where clause in sql structure.
 */
public class WhereClauseProcessor {

    private final FieldType defaultFieldType;
    private final Map<String, FieldType> fieldNameToFieldTypeMapping;
    private final boolean requiresMultistepAggregation;

    /**
     * Default constructor.
     * @param defaultFieldType the default {@link FieldType}
     * @param fieldNameToFieldTypeMapping the field name to {@link FieldType} mapping
     * @param requiresMultistepAggregation if aggregation is detected for the sql query
     */
    public WhereClauseProcessor(final FieldType defaultFieldType,
                                final Map<String, FieldType> fieldNameToFieldTypeMapping,
                                final boolean requiresMultistepAggregation) {
        this.defaultFieldType = defaultFieldType;
        this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping;
        this.requiresMultistepAggregation = requiresMultistepAggregation;
    }

    /**
     * Construtor to use when no value for requiresAggregation.
     * @param defaultFieldType the default {@link FieldType}
     * @param fieldNameToFieldTypeMapping the field name to {@link FieldType} map
     */
    public WhereClauseProcessor(final FieldType defaultFieldType,
                                final Map<String, FieldType> fieldNameToFieldTypeMapping) {
        this(defaultFieldType, fieldNameToFieldTypeMapping, false);
    }

    //Parse comparative expression != = < > => <= into mongo expr
    private void parseComparativeExpr(final Document query, final Expression leftExpression,
                                      final Expression rightExpression, final String comparatorType)
            throws ParseException {
        String operator = "$" + comparatorType;
        if (Function.class.isInstance(leftExpression)) {
            Document doc = new Document();
            Object leftParse = parseExpression(new Document(), leftExpression, rightExpression);
            Object rightParse = parseExpression(new Document(), rightExpression, leftExpression);
            doc.put(operator, Arrays.asList(leftParse, (SqlUtils.isColumn(rightExpression)
                    && !rightExpression.toString().startsWith("$") ? "$" + rightParse : rightParse)));
            if (requiresMultistepAggregation) {
                query.put("$expr", doc);
            } else {
                query.putAll(doc);
            }
        } else if (SqlUtils.isColumn(leftExpression) && SqlUtils.isColumn(rightExpression)) {
            if (requiresMultistepAggregation) {
                Document doc = new Document();
                String leftName = ((Column) leftExpression).getName(false);
                String rightName = ((Column) rightExpression).getName(false);
                doc.put(operator,
                        Arrays.asList((leftName.startsWith("$") ? leftName : "$" + leftName),
                                (rightName.startsWith("$") ? rightName : "$" + rightName)));
                query.put("$expr", doc);
            } else {
                query.put(parseExpression(new Document(), leftExpression, rightExpression).toString(),
                        parseExpression(new Document(), rightExpression, leftExpression));
            }
        } else if (Function.class.isInstance(rightExpression)) {
            Document doc = new Document();
            Object leftParse = parseExpression(new Document(), rightExpression, leftExpression);
            Object rightParse = parseExpression(new Document(), leftExpression, rightExpression);
            doc.put(operator, Arrays.asList(leftParse, (SqlUtils.isColumn(leftExpression)
                    && !leftExpression.toString().startsWith("$") ? "$" + rightParse : rightParse)));
            if (requiresMultistepAggregation) {
                query.put("$expr", doc);
            } else {
                query.putAll(doc);
            }
        } else if (SqlUtils.isColumn(leftExpression)) {
            Document subdocument = new Document();
            if (operator.equals("$eq")) {
                query.put(parseExpression(new Document(), leftExpression, rightExpression).toString(),
                        parseExpression(new Document(), rightExpression, leftExpression));
            } else {
                subdocument.put(operator, parseExpression(new Document(), rightExpression, leftExpression));
                query.put(parseExpression(new Document(), leftExpression, rightExpression).toString(),
                        subdocument);
            }
        } else {
            Document doc = new Document();
            Object leftParse = parseExpression(new Document(), leftExpression, rightExpression);
            doc.put(operator, Arrays.asList(leftParse, SqlUtils.nonFunctionToNode(
                    rightExpression, requiresMultistepAggregation)));
            if (requiresMultistepAggregation) {
                query.put("$expr", doc);
            } else {
                query.putAll(doc);
            }
        }
    }

    /**
     * Recursive function responsible for stepping through the sql structure and converting it into a mongo structure.
     * @param query the query in {@link Document} format
     * @param incomingExpression the incoming {@link Expression}
     * @param otherSide the {@link Expression} on the other side
     * @return the converted mongo structure.
     * @throws ParseException if there is an issue parsing the incomingExpression
     */
    public Object parseExpression(final Document query,
                                  final Expression incomingExpression, final Expression otherSide)
            throws ParseException {
        if (ComparisonOperator.class.isInstance(incomingExpression)) {
            RegexFunction regexFunction = SqlUtils.isRegexFunction(incomingExpression);
            DateFunction dateFunction = SqlUtils.getDateFunction(incomingExpression);
            ObjectIdFunction objectIdFunction = SqlUtils.isObjectIdFunction(this,
                    incomingExpression);
            if (regexFunction != null) {
                Document regexDocument = new Document("$regex", regexFunction.getRegex());
                if (regexFunction.getOptions() != null) {
                    regexDocument.append("$options", regexFunction.getOptions());
                }
                query.put(regexFunction.getColumn(), wrapIfIsNot(regexDocument, regexFunction));
            } else if (dateFunction != null) {
                query.put(dateFunction.getColumn(),
                        new Document(dateFunction.getComparisonExpression(), dateFunction.getDate()));
            } else if (objectIdFunction != null) {
                query.put(objectIdFunction.getColumn(), objectIdFunction.toDocument());
            } else if (EqualsTo.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((EqualsTo) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((EqualsTo) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "eq");
            } else if (NotEqualsTo.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((NotEqualsTo) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((NotEqualsTo) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "ne");
            } else if (GreaterThan.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((GreaterThan) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((GreaterThan) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "gt");
            } else if (MinorThan.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((MinorThan) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((MinorThan) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "lt");
            } else if (GreaterThanEquals.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((GreaterThanEquals) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((GreaterThanEquals) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "gte");
            } else if (MinorThanEquals.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((MinorThanEquals) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((MinorThanEquals) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "lte");
            }
        } else if (LikeExpression.class.isInstance(incomingExpression)
                && Column.class.isInstance(((LikeExpression) incomingExpression).getLeftExpression())
                && (StringValue.class.isInstance(((LikeExpression) incomingExpression).getRightExpression())
                || Column.class.isInstance(((LikeExpression) incomingExpression).getRightExpression()))) {
            LikeExpression likeExpression = (LikeExpression) incomingExpression;
            String stringValueLeftSide = SqlUtils.getStringValue(likeExpression.getLeftExpression());
            String stringValueRightSide = SqlUtils.getStringValue(likeExpression.getRightExpression());
            String convertedRegexString = "^" + SqlUtils.replaceRegexCharacters(stringValueRightSide) + "$";
            Document document = new Document("$regex", convertedRegexString);
            if (likeExpression.isNot()) {
                document = new Document(stringValueLeftSide, new Document("$not", Pattern.compile(
                        convertedRegexString)));
            } else {
                document = new Document(stringValueLeftSide, document);
            }
            query.putAll(document);
        } else if (IsNullExpression.class.isInstance(incomingExpression)) {
            IsNullExpression isNullExpression = (IsNullExpression) incomingExpression;
            query.put(SqlUtils.getStringValue(isNullExpression.getLeftExpression()), new Document("$exists",
                    isNullExpression.isNot()));
        } else if (InExpression.class.isInstance(incomingExpression)) {
            final InExpression inExpression = (InExpression) incomingExpression;
            final Expression leftExpression = ((InExpression) incomingExpression).getLeftExpression();
            final String leftExpressionAsString = SqlUtils.getStringValue(leftExpression);
            ObjectIdFunction objectIdFunction = SqlUtils.isObjectIdFunction(this, incomingExpression);

            if (objectIdFunction != null) {
                query.put(objectIdFunction.getColumn(), objectIdFunction.toDocument());
            } else {
                List<Object> objectList = Lists
                        .transform(((ExpressionList) inExpression.getRightItemsList()).getExpressions(),
                                new com.google.common.base.Function<Expression, Object>() {
                                    @Override
                                    public Object apply(final Expression expression) {
                                        try {
                                            return parseExpression(new Document(), expression,
                                                    leftExpression);
                                        } catch (ParseException e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                });

                if (Function.class.isInstance(leftExpression)) {
                    String mongoInFunction = inExpression.isNot() ? "$fnin" : "$fin";
                    query.put(mongoInFunction, new Document("function", parseExpression(new Document(),
                            leftExpression, otherSide)).append("list", objectList));
                } else {
                    String mongoInFunction = inExpression.isNot() ? "$nin" : "$in";
                    Document doc = new Document();
                    if (requiresMultistepAggregation) {
                        List<Object> lobj = Arrays.asList(
                                SqlUtils.nonFunctionToNode(leftExpression, requiresMultistepAggregation), objectList);
                        doc.put(mongoInFunction, lobj);
                        query.put("$expr", doc);
                    } else {
                        doc.put(leftExpressionAsString, new Document().append(mongoInFunction, objectList));
                        query.putAll(doc);
                    }

                }
            }
        } else if (AndExpression.class.isInstance(incomingExpression)) {
            handleAndOr("$and", (BinaryExpression) incomingExpression, query);
        } else if (OrExpression.class.isInstance(incomingExpression)) {
            handleAndOr("$or", (BinaryExpression) incomingExpression, query);
        } else if (Parenthesis.class.isInstance(incomingExpression)) {
            Parenthesis parenthesis = (Parenthesis) incomingExpression;
            Object expression = parseExpression(new Document(), parenthesis.getExpression(), null);
            return expression;
        } else if (NotExpression.class.isInstance(incomingExpression)) {
            Expression expression = ((NotExpression) incomingExpression).getExpression();

            if (Parenthesis.class.isInstance(expression)) {
                return new Document("$nor", Arrays.asList(parseExpression(query, expression, otherSide)));
            }

            return new Document(SqlUtils.getStringValue(expression), new Document("$ne", true));
        } else if (Function.class.isInstance(incomingExpression)) {
            Function function = ((Function) incomingExpression);
            RegexFunction regexFunction = SqlUtils.isRegexFunction(incomingExpression);
            ObjectIdFunction objectIdFunction = SqlUtils.isObjectIdFunction(this, incomingExpression);
            if (regexFunction != null) {
                Document regexDocument = new Document("$regex", regexFunction.getRegex());
                if (regexFunction.getOptions() != null) {
                    regexDocument.append("$options", regexFunction.getOptions());
                }
                query.put(regexFunction.getColumn(), wrapIfIsNot(regexDocument, regexFunction));
            } else if (objectIdFunction != null) {
                return objectIdFunction.toDocument();
            } else {
                return recurseFunctions(query, function, defaultFieldType, fieldNameToFieldTypeMapping);
            }
        } else if (otherSide == null) {
            return new Document(SqlUtils.getStringValue(incomingExpression), true);
        } else {
            return SqlUtils.getNormalizedValue(incomingExpression, otherSide,
                    defaultFieldType, fieldNameToFieldTypeMapping, null);
        }
        return query;
    }

    private Object wrapIfIsNot(final Document regexDocument, final RegexFunction regexFunction) {
        if (regexFunction.isNot()) {
            if (regexFunction.getOptions() != null) {
                throw new IllegalArgumentException("$not regex not supported with options");
            }
            return new Document("$not", Pattern.compile(regexFunction.getRegex()));
        }
        return regexDocument;
    }

    /**
     * Recurse through functions in the sql structure to generate mongo query structure.
     * @param query the query in {@link Document} format
     * @param object the value
     * @param defaultFieldType the default {@link FieldType}
     * @param fieldNameToFieldTypeMapping the field name to{@link FieldType} map
     * @return the mongo structure
     * @throws ParseException if the value of the object param could not be parsed
     */
    protected Object recurseFunctions(final Document query, final Object object,
                                      final FieldType defaultFieldType,
                                      final Map<String, FieldType> fieldNameToFieldTypeMapping) throws ParseException {
        if (Function.class.isInstance(object)) {
            Function function = (Function) object;
            query.put("$" + SqlUtils.translateFunctionName(function.getName()),
                    recurseFunctions(new Document(), function.getParameters(),
                            defaultFieldType, fieldNameToFieldTypeMapping));
        } else if (ExpressionList.class.isInstance(object)) {
            ExpressionList expressionList = (ExpressionList) object;
            List<Object> objectList = new ArrayList<>();
            for (Expression expression : expressionList.getExpressions()) {
                objectList.add(recurseFunctions(new Document(), expression,
                        defaultFieldType, fieldNameToFieldTypeMapping));
            }
            return objectList.size() == 1 ? objectList.get(0) : objectList;
        } else if (Expression.class.isInstance(object)) {
            return SqlUtils.getNormalizedValue((Expression) object, null,
                    defaultFieldType, fieldNameToFieldTypeMapping, null);
        }

        return query.isEmpty() ? null : query;
    }

    private void handleAndOr(final String key, final BinaryExpression incomingExpression,
                             final Document query) throws ParseException {
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

    private List flattenOrsOrAnds(final List arrayList, final Expression firstExpression,
                                  final Expression leftExpression,
                                  final Expression rightExpression) throws ParseException {
        if (firstExpression.getClass().isInstance(leftExpression)
                && isOrAndExpression(leftExpression) && !isOrAndExpression(rightExpression)) {
            Expression left = ((BinaryExpression) leftExpression).getLeftExpression();
            Expression right = ((BinaryExpression) leftExpression).getRightExpression();
            arrayList.add(parseExpression(new Document(), rightExpression, null));
            List result = flattenOrsOrAnds(arrayList, firstExpression, left, right);
            if (result != null) {
                return arrayList;
            }
        } else if (isOrAndExpression(firstExpression)
                && !isOrAndExpression(leftExpression) && !isOrAndExpression(rightExpression)) {
            arrayList.add(parseExpression(new Document(), rightExpression, null));
            arrayList.add(parseExpression(new Document(), leftExpression, null));
            return arrayList;
        } else {
            return null;
        }
        return null;
    }

    private boolean isOrAndExpression(final Expression expression) {
        return OrExpression.class.isInstance(expression) || AndExpression.class.isInstance(expression);
    }

}
