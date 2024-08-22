package com.github.vincentrussell.query.mongodb.sql.converter.processor;

import com.github.vincentrussell.query.mongodb.sql.converter.FieldType;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.AliasHolder;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * case when then else end => $cond support.
 *
 * @author maxid
 * @since 2024/8/20 18:00
 */
public class CaseClauseProcessor implements ClauseProcessor {
    private final FieldType              defaultFieldType;
    private final Map<String, FieldType> fieldNameToFieldTypeMapping;
    private final boolean                requiresMultistepAggregation;
    @SuppressWarnings("checkstyle:VisibilityModifier")
    protected     AliasHolder            aliasHolder;

    /**
     * Default constructor.
     *
     * @param defaultFieldType             the default {@link FieldType}
     * @param fieldNameToFieldTypeMapping  the field name to {@link FieldType} mapping
     * @param requiresMultistepAggregation if aggregation is detected for the sql query
     * @param aliasHolder                  the {@link AliasHolder}
     */
    public CaseClauseProcessor(final FieldType defaultFieldType,
                               final Map<String, FieldType> fieldNameToFieldTypeMapping,
                               final boolean requiresMultistepAggregation, final AliasHolder aliasHolder) {
        this.defaultFieldType = defaultFieldType;
        this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping;
        this.requiresMultistepAggregation = requiresMultistepAggregation;
        this.aliasHolder = aliasHolder;
    }

    /**
     * Constructor without AliasHolder.
     *
     * @param defaultFieldType             the default {@link FieldType}
     * @param fieldNameToFieldTypeMapping  the field name to type mapping
     * @param requiresMultistepAggregation if multistep aggregation is required.
     */
    public CaseClauseProcessor(final FieldType defaultFieldType,
                               final Map<String, FieldType> fieldNameToFieldTypeMapping,
                               final boolean requiresMultistepAggregation) {
        this(defaultFieldType, fieldNameToFieldTypeMapping, requiresMultistepAggregation, new AliasHolder());
    }

    /**
     * constructor to use when no value for requiresAggregation.
     *
     * @param defaultFieldType            the default {@link FieldType}
     * @param fieldNameToFieldTypeMapping the field name to {@link FieldType} map
     */
    public CaseClauseProcessor(final FieldType defaultFieldType,
                               final Map<String, FieldType> fieldNameToFieldTypeMapping) {
        this(defaultFieldType, fieldNameToFieldTypeMapping, false);
    }

    /**
     * Recursive function responsible for stepping through the sql structure and converting it into a mongo structure.
     *
     * @param query              the query in {@link Document} format
     * @param incomingExpression the incoming {@link Expression}
     * @param otherSide          the {@link Expression} on the other side
     * @return the converted mongo structure.
     * @throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException if there is an issue
     * parsing the incomingExpression
     */
    @Override
    @SuppressWarnings("checkstyle:methodlength")
    public Object parseExpression(final Document query,
                                  final Expression incomingExpression, final Expression otherSide)
            throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException {
        if (incomingExpression instanceof ComparisonOperator) {
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
                query.put(dateFunction.getColumn(), new Document(dateFunction.getComparisonExpression(),
                        dateFunction.getDate()));
            } else if (objectIdFunction != null) {
                query.put(objectIdFunction.getColumn(), objectIdFunction.toDocument());
            } else if (incomingExpression instanceof EqualsTo) { // =
                final Expression leftExpression = ((EqualsTo) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((EqualsTo) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "eq");
            } else if (incomingExpression instanceof NotEqualsTo) { // !=
                final Expression leftExpression = ((NotEqualsTo) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((NotEqualsTo) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "ne");
            } else if (incomingExpression instanceof GreaterThan) { // >
                final Expression leftExpression = ((GreaterThan) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((GreaterThan) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "gt");
            } else if (incomingExpression instanceof MinorThan) { // <
                final Expression leftExpression = ((MinorThan) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((MinorThan) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "lt");
            } else if (incomingExpression instanceof GreaterThanEquals) { // >=
                final Expression leftExpression = ((GreaterThanEquals) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((GreaterThanEquals) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "gte");
            } else if (incomingExpression instanceof MinorThanEquals) { // <=
                final Expression leftExpression = ((MinorThanEquals) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((MinorThanEquals) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "lte");
            }
        } else if (incomingExpression instanceof LikeExpression
                && ((LikeExpression) incomingExpression).getLeftExpression() instanceof Column
                && (((LikeExpression) incomingExpression).getRightExpression() instanceof StringValue
                || ((LikeExpression) incomingExpression).getRightExpression() instanceof Column)) { // like || not like
            LikeExpression likeExpression = (LikeExpression) incomingExpression;
            String stringValueLeftSide = SqlUtils.getStringValue(likeExpression.getLeftExpression());
            String stringValueRightSide = SqlUtils.getStringValue(likeExpression.getRightExpression());
            String convertedRegexString = "^" + SqlUtils.replaceRegexCharacters(stringValueRightSide) + "$";
            String operator = "$regexMatch";
            String left = stringValueLeftSide.startsWith("$") ? stringValueLeftSide : "$" + stringValueLeftSide;
            Document document = new Document("input", left)
                    .append("regex", convertedRegexString);
            if (likeExpression.isNot()) {
                String operatorNot = "$not";
                document = new Document(operatorNot, new Document(operator, document));
            } else {
                document = new Document(operator, document);
            }
            query.putAll(document);
        } else if (incomingExpression instanceof IsNullExpression) {
            IsNullExpression isNullExpression = (IsNullExpression) incomingExpression;
            if (isNullExpression.getLeftExpression() instanceof Function) {
                Document result = ((Document) recurseFunctions(new Document(), isNullExpression.getLeftExpression(),
                        defaultFieldType, fieldNameToFieldTypeMapping))
                        .append("$exists", isNullExpression.isNot());
                query.putAll(result);
            } else {
                query.put(SqlUtils.getStringValue(isNullExpression.getLeftExpression()), new Document("$exists",
                        isNullExpression.isNot()));
            }
        } else if (incomingExpression instanceof InExpression) { // in(?,?,?,..), v3.2 not support
            final InExpression inExpression = (InExpression) incomingExpression;
            final Expression leftExpression = ((InExpression) incomingExpression).getLeftExpression();
            ObjectIdFunction objectIdFunction = SqlUtils.isObjectIdFunction(this,
                    incomingExpression);

            if (objectIdFunction != null) {
                query.put(objectIdFunction.getColumn(), objectIdFunction.toDocument());
            } else {
                List<Object> objectList = Lists.transform(((ExpressionList) inExpression.getRightItemsList())
                        .getExpressions(), expression -> {
                    try {
                        return parseExpression(new Document(), expression, leftExpression);
                    } catch (com.github.vincentrussell.query.mongodb.sql.converter.ParseException e) {
                        throw new RuntimeException(e);
                    }
                });

                if (leftExpression instanceof Function) {
                    String mongoInFunction = inExpression.isNot() ? "$fnin" : "$fin";
                    query.put(mongoInFunction, new Document("function", parseExpression(new Document(), leftExpression,
                            otherSide)).append("list", objectList));
                } else {
                    String mongoInFunction = inExpression.isNot() ? "$nin" : "$in";
                    Document doc = new Document();
                    if (requiresMultistepAggregation) {
                        List<Object> lobj = Arrays.asList(SqlUtils.nonFunctionToNode(leftExpression,
                                requiresMultistepAggregation), objectList);
                        doc.put(mongoInFunction, lobj);
                        query.put("$expr", doc);
                    } else {
                        // v3.2 not support, 4.2+ pass
                        List<Object> lobj = Arrays.asList(SqlUtils.nonFunctionToNode(leftExpression,
                                true), objectList);
                        doc.put(mongoInFunction, lobj);
                        query.putAll(doc);
                    }
                }
            }
        } else if (incomingExpression instanceof AndExpression) {
            handleAndOr("$and", (BinaryExpression) incomingExpression, query);
        } else if (incomingExpression instanceof OrExpression) {
            handleAndOr("$or", (BinaryExpression) incomingExpression, query);
        } else if (incomingExpression instanceof Parenthesis) {
            Parenthesis parenthesis = (Parenthesis) incomingExpression;
            return parseExpression(new Document(), parenthesis.getExpression(), null);
        } else if (incomingExpression instanceof NotExpression) {
            NotExpression notExpression = (NotExpression) incomingExpression;
            Expression expression = notExpression.getExpression();
            if (expression instanceof Parenthesis) {
                return new Document("$nor", Collections.singletonList(parseExpression(query, expression, otherSide)));
            } else if (expression instanceof Column) {
                return new Document(SqlUtils.getStringValue(expression), new Document("$ne", true));
            } else if (expression instanceof ComparisonOperator) {
                Document parsedDocument = (Document) parseExpression(query, expression, otherSide);
                String column = parsedDocument.keySet().iterator().next();
                Document value = parsedDocument.get(column, Document.class);
                return new Document(column, new Document("$not", value));
            }
        } else if (incomingExpression instanceof Function) {
            Function function = ((Function) incomingExpression);
            RegexFunction regexFunction = SqlUtils.isRegexFunction(incomingExpression);
            ObjectIdFunction objectIdFunction = SqlUtils.isObjectIdFunction(this,
                    incomingExpression);
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
                    defaultFieldType, fieldNameToFieldTypeMapping, aliasHolder, null);
        }
        return query;
    }

    /**
     * Recurse through functions in the sql structure to generate mongo query structure.
     *
     * @param query                       the query in {@link Document} format
     * @param object                      the value
     * @param defaultFieldType            the default {@link FieldType}
     * @param fieldNameToFieldTypeMapping the field name to{@link FieldType} map
     * @return the mongo structure
     * @throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException if the value of the object
     * param could not be parsed
     */
    protected Object recurseFunctions(final Document query, final Object object,
                                      final FieldType defaultFieldType,
                                      final Map<String, FieldType> fieldNameToFieldTypeMapping)
            throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException {
        if (object instanceof Function) {
            Function function = (Function) object;
            query.put("$" + SqlUtils.translateFunctionName(function.getName()),
                    recurseFunctions(new Document(), function.getParameters(),
                            defaultFieldType, fieldNameToFieldTypeMapping));
        } else if (object instanceof ExpressionList) {
            ExpressionList expressionList = (ExpressionList) object;
            List<Object> objectList = new ArrayList<>();
            for (Expression expression : expressionList.getExpressions()) {
                objectList.add(recurseFunctions(new Document(), expression, defaultFieldType,
                        fieldNameToFieldTypeMapping));
            }
            return objectList.size() == 1 ? objectList.get(0) : objectList;
        } else if (object instanceof Expression) {
            return SqlUtils.getNormalizedValue((Expression) object, null, defaultFieldType,
                    fieldNameToFieldTypeMapping, null);
        }

        return query.isEmpty() ? null : query;
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

    // Parse comparative expression != = < > => <= into mongo expr
    private void parseComparativeExpr(final Document query, final Expression leftExpression,
                                      final Expression rightExpression, final String comparatorType)
            throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException {
        String operator = "$" + comparatorType;
        if (leftExpression instanceof Function) {
            Document doc = new Document();
            Object leftParse = parseExpression(new Document(), leftExpression, rightExpression);
            Object rightParse = parseExpression(new Document(), rightExpression, leftExpression);
            doc.put(operator, Arrays.asList(leftParse, ((SqlUtils.isColumn(rightExpression)
                    && !rightExpression.toString().startsWith("$")
                    && !(leftParse instanceof Document)) ? "$" + rightParse : rightParse)));
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
                doc.put(operator, Arrays.asList((leftName.startsWith("$") ? leftName : "$" + leftName),
                        (rightName.startsWith("$") ? rightName : "$" + rightName)));
                query.put("$expr", doc);
            } else {
                query.put(parseExpression(new Document(), leftExpression, rightExpression).toString(),
                        parseExpression(new Document(), rightExpression, leftExpression));
            }
        } else if (rightExpression instanceof Function) {
            Document doc = new Document();
            Object leftParse = parseExpression(new Document(), leftExpression, rightExpression);
            Object rightParse = parseExpression(new Document(), rightExpression, leftExpression);
            doc.put(operator, Arrays.asList(leftParse, ((SqlUtils.isColumn(leftExpression)
                    && !leftExpression.toString().startsWith("$")
                    && !(rightParse instanceof Document)) ? "$" + rightParse : rightParse)));
            if (requiresMultistepAggregation) {
                query.put("$expr", doc);
            } else {
                query.putAll(doc);
            }
        } else if (SqlUtils.isColumn(leftExpression)) {
            String leftParse = "$" + parseExpression(new Document(), leftExpression, rightExpression).toString();
            Object rightParse = parseExpression(new Document(), rightExpression, leftExpression);
            query.put(operator, Lists.newArrayList(leftParse, rightParse));
        } else {
            Object leftParse = parseExpression(new Document(), leftExpression, rightExpression);
            Object rightParse = "$" + parseExpression(new Document(), rightExpression, leftExpression).toString();
            query.put(operator, Lists.newArrayList(leftParse, rightParse));
        }
    }

    private void handleAndOr(final String key, final BinaryExpression incomingExpression,
                             final Document query)
            throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException {
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
                                  final Expression rightExpression)
            throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException {
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
        return expression instanceof OrExpression || expression instanceof AndExpression;
    }
}
