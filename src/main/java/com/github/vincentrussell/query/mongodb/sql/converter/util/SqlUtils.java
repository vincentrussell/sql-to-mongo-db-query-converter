package com.github.vincentrussell.query.mongodb.sql.converter.util;

import com.github.vincentrussell.query.mongodb.sql.converter.FieldType;
import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.github.vincentrussell.query.mongodb.sql.converter.processor.WhereClauseProcessor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.bson.Document;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.google.common.base.MoreObjects.firstNonNull;

public final class SqlUtils {
    private static final Pattern SURROUNDED_IN_QUOTES = Pattern.compile("^\"(.+)*\"$");
    private static final Pattern LIKE_RANGE_REGEX = Pattern.compile("(\\[.+?\\])");
    private static final String REGEXMATCH_FUNCTION = "regexMatch";
    private static final String NOT_REGEXMATCH_FUNCTION = "notRegexMatch";


    private static final DateTimeFormatter YY_MM_DDFORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final DateTimeFormatter YYMMDDFORMATTER = DateTimeFormat.forPattern("yyyyMMdd");

    private static final Map<String, String> FUNCTION_MAPPER = new ImmutableMap.Builder<String, String>()
            .put("OID", "toObjectId")
            .put("TIMESTAMP", "toDate")
            .build();

    private static final Collection<DateTimeFormatter> FORMATTERS = Collections.unmodifiableList(Arrays.asList(
            ISODateTimeFormat.dateTime(),
            YY_MM_DDFORMATTER,
            YYMMDDFORMATTER));

    private SqlUtils() {

    }

    /**
     * Will get the string value for an expression and remove double double quotes if they are present like,
     * i.e: ""45 days ago"".
     * @param expression
     * @return the string value for an expression and remove double double quotes if they are present
     */
    public static String getStringValue(final Expression expression) {
        if (StringValue.class.isInstance(expression)) {
            return ((StringValue) expression).getValue();
        } else if (Column.class.isInstance(expression)) {
            String columnName = expression.toString();
            Matcher matcher = SURROUNDED_IN_QUOTES.matcher(columnName);
            if (matcher.matches()) {
                return matcher.group(1);
            }
            return columnName;
        }
        return expression.toString();
    }

    /**
     * Take an {@link Expression} and normalize it.
     * @param incomingExpression
     * @param otherSide
     * @param defaultFieldType
     * @param fieldNameToFieldTypeMapping
     * @return the normalized value
     * @throws ParseException
     */
    public static Object getNormalizedValue(final Expression incomingExpression, final Expression otherSide,
                                            final FieldType defaultFieldType,
                                            final Map<String, FieldType> fieldNameToFieldTypeMapping)
            throws ParseException {
        FieldType fieldType = otherSide != null ? firstNonNull(
                fieldNameToFieldTypeMapping.get(getStringValue(otherSide)),
                defaultFieldType) : FieldType.UNKNOWN;
        if (LongValue.class.isInstance(incomingExpression)) {
            return getNormalizedValue((((LongValue) incomingExpression).getValue()), fieldType);
        } else if (DoubleValue.class.isInstance(incomingExpression)) {
            return getNormalizedValue((((DoubleValue) incomingExpression).getValue()), fieldType);
        } else if (SignedExpression.class.isInstance(incomingExpression)) {
            return getNormalizedValue((((SignedExpression) incomingExpression).toString()), fieldType);
        } else if (StringValue.class.isInstance(incomingExpression)) {
            return getNormalizedValue((((StringValue) incomingExpression).getValue()), fieldType);
        } else if (Column.class.isInstance(incomingExpression)) {
            return getNormalizedValue(getStringValue(incomingExpression), fieldType);
        } else if (TimestampValue.class.isInstance(incomingExpression)) {
            return getNormalizedValue(
                    new Date((((TimestampValue) incomingExpression).getValue().getTime())), fieldType);
        } else if (DateValue.class.isInstance(incomingExpression)) {
            return getNormalizedValue((((DateValue) incomingExpression).getValue()), fieldType);
        } else {
            throw new ParseException("can not parseNaturalLanguageDate: " + incomingExpression.toString());
        }
    }

    /**
     * Take an {@link Object} and normalize it.
     * @param value
     * @param fieldType
     * @return the normalized value
     * @throws ParseException
     */
    public static Object getNormalizedValue(final Object value, final FieldType fieldType) throws ParseException {
        if (fieldType == null || FieldType.UNKNOWN.equals(fieldType)) {
            Object bool = getObjectAsBoolean(value);
            return (bool != null) ? bool : value;
        } else {
            if (FieldType.STRING.equals(fieldType)) {
                return fixDoubleSingleQuotes(forceString(value));
            }
            if (FieldType.NUMBER.equals(fieldType)) {
                return getObjectAsNumber(value);
            }
            if (FieldType.DATE.equals(fieldType)) {
                return getObjectAsDate(value);
            }
            if (FieldType.BOOLEAN.equals(fieldType)) {
                return Boolean.valueOf(value.toString());
            }
        }
        throw new ParseException("could not normalize value:" + value);
    }

    private static long getLongFromStringIfInteger(final String stringValue) throws ParseException {
        BigInteger bigInt = new BigInteger(stringValue);
        isFalse(bigInt.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0,
                stringValue + ": value is too large");
        return bigInt.longValue();
    }

    /**
     * get limit as long.
     * @param limit
     * @return the limit
     * @throws ParseException
     */
    public static long getLimitAsLong(final Limit limit) throws ParseException {
        if (limit != null) {
            return getLongFromStringIfInteger(SqlUtils.getStringValue(limit.getRowCount()));
        }
        return -1;
    }

    /**
     * get offset as long.
     * @param offset
     * @return the offset
     */
    public static long getOffsetAsLong(final Offset offset) {
        if (offset != null) {
            return offset.getOffset();
        }
        return -1;
    }

    /**
     * Will replace double single quotes in regex with a single single quote,
     * i.e: "^[ae"don''tgaf]+$" -> "^[ae"don'tgaf]+$".
     * @param regex
     * @return the regex without double single quotes
     */
    public static String fixDoubleSingleQuotes(final String regex) {
        return regex.replaceAll("''", "'");
    }

    /**
     * Will return tue if the query is select *.
     * @param selectItems
     * @return true if select *
     */
    public static boolean isSelectAll(final List<SelectItem> selectItems) {
        if (selectItems != null && selectItems.size() == 1) {
            SelectItem firstItem = selectItems.get(0);
            return AllColumns.class.isInstance(firstItem);
        } else {
            return false;
        }
    }

    /**
     * Will return true if query is doing a count(*).
     * @param selectItems
     * @return true if query is doing a count(*)
     */
    public static boolean isCountAll(final List<SelectItem> selectItems) {
        if (selectItems != null && selectItems.size() == 1) {
            SelectItem firstItem = selectItems.get(0);
            if ((SelectExpressionItem.class.isInstance(firstItem))
                    && Function.class.isInstance(((SelectExpressionItem) firstItem).getExpression())) {
                Function function = (Function) ((SelectExpressionItem) firstItem).getExpression();

                if ("count(*)".equals(function.toString())) {
                    return true;
                }

            }
        }
        return false;
    }

    /**
     * Convert object to boolean.
     * @param value
     * @return boolean value of object
     */
    public static Object getObjectAsBoolean(final Object value) {
        if (value.toString().equalsIgnoreCase("true")
                || value.toString().equalsIgnoreCase("false")) {
            return Boolean.valueOf(value.toString());
        }
        return null;
    }

    /**
     * Convert object to {@link Date} object.
     * @param value
     * @return date object
     * @throws ParseException
     */
    public static Object getObjectAsDate(final Object value) throws ParseException {
        if (String.class.isInstance(value)) {
            for (DateTimeFormatter formatter : FORMATTERS) {
                try {
                    DateTime dt = formatter.parseDateTime((String) value);
                    return dt.toDate();
                } catch (Exception e) {
                    //noop
                }
            }
            try {
                return parseNaturalLanguageDate((String) value);
            } catch (Exception e) {
                //noop
            }

        }
        throw new ParseException("could not convert " + value + " to a date");
    }

    /**
     * Parse natural language to Date object.
     * @param text
     * @return parsed date
     */
    public static Date parseNaturalLanguageDate(final String text) {
        Parser parser = new Parser();
        List<DateGroup> groups = parser.parse(text);
        for (DateGroup group : groups) {
            List<Date> dates = group.getDates();
            if (dates.size() > 0) {
                return dates.get(0);
            }
        }
        throw new IllegalArgumentException("could not natural language date: " + text);
    }

    /**
     * Get object as number.
     * @param value
     * @return number
     * @throws ParseException
     */
    public static Object getObjectAsNumber(final Object value) throws ParseException {
        if (String.class.isInstance(value)) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e1) {
                try {
                    return Double.parseDouble((String) value);
                } catch (NumberFormatException e2) {
                    try {
                        return Float.parseFloat((String) value);
                    } catch (NumberFormatException e3) {
                        throw new ParseException("could not convert " + value + " to number", e3);
                    }
                }
            }
        } else {
            return value;
        }
    }

    /**
     * Force an object to being a string.
     * @param value
     * @return the object converted to a string
     */
    public static String forceString(final Object value) {
        if (String.class.isInstance(value)) {
            return (String) value;
        } else {
            return "" + value + "";
        }
    }

    /**
     * convert {@link net.sf.jsqlparser.parser.ParseException} to {@link ParseException}.
     * @param incomingException
     * @return the {@link ParseException}
     */
    public static ParseException convertParseException(
            final net.sf.jsqlparser.parser.ParseException incomingException) {
            return new ParseException(incomingException);
    }


    /**
     * Will replace a LIKE sql query with the format for a regex.
     * @param value
     * @return a regex that represents the LIKE format.
     */
    public static String replaceRegexCharacters(final String value) {
        String newValue = value.replaceAll("%", ".*")
                .replaceAll("_", ".{1}");

        Matcher m = LIKE_RANGE_REGEX.matcher(newValue);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb, m.group(1) + "{1}");
        }
        m.appendTail(sb);

        return sb.toString();
    }

    /**
     * Get the columns used for the group by from the {@link PlainSelect}.
     * @param plainSelect
     * @return the columns used for the group by from the {@link PlainSelect}.
     */
    public static List<String> getGroupByColumnReferences(final PlainSelect plainSelect) {
        if (plainSelect.getGroupBy() == null) {
            return Collections.emptyList();
        }

        return Lists.transform(plainSelect.getGroupBy().getGroupByExpressions(),
                new com.google.common.base.Function<Expression, String>() {
                    @Override
                    public String apply(@Nonnull final Expression expression) {
                        return SqlUtils.getStringValue(expression);
                    }
                });
    }

    /**
     * Will take the expression and create an {@link ObjectIdFunction} if the expression is an ObjectId function.
     * Otherwise it will return null.
     * @param whereClauseProcessor
     * @param incomingExpression
     * @return the {@link ObjectIdFunction}
     * @throws ParseException
     */
    public static ObjectIdFunction isObjectIdFunction(final WhereClauseProcessor whereClauseProcessor,
                                                      final Expression incomingExpression) throws ParseException {
        if (ComparisonOperator.class.isInstance(incomingExpression)) {
            ComparisonOperator comparisonOperator = (ComparisonOperator) incomingExpression;
            String rightExpression = getStringValue(comparisonOperator.getRightExpression());
            if (Function.class.isInstance(comparisonOperator.getLeftExpression())) {
                Function function = ((Function) comparisonOperator.getLeftExpression());
                if ("objectid".equals(function.getName().toLowerCase())
                        && (function.getParameters().getExpressions().size() == 1)
                        && StringValue.class.isInstance(function.getParameters().getExpressions().get(0))) {
                    String column = getStringValue(function.getParameters().getExpressions().get(0));
                    return new ObjectIdFunction(column, rightExpression, comparisonOperator);
                }
            }
        } else if (InExpression.class.isInstance(incomingExpression)) {
            InExpression inExpression = (InExpression) incomingExpression;
            final Expression leftExpression = ((InExpression) incomingExpression).getLeftExpression();

            if (Function.class.isInstance(inExpression.getLeftExpression())) {
                Function function = ((Function) inExpression.getLeftExpression());
                if ("objectid".equals(function.getName().toLowerCase())
                        && (function.getParameters().getExpressions().size() == 1)
                        && StringValue.class.isInstance(function.getParameters().getExpressions().get(0))) {
                    String column = getStringValue(function.getParameters().getExpressions().get(0));
                    List<Object> rightExpression = Lists.transform(((ExpressionList)
                                    inExpression.getRightItemsList()).getExpressions(),
                            new com.google.common.base.Function<Expression, Object>() {
                                @Override
                                public Object apply(final Expression expression) {
                                    try {
                                        return whereClauseProcessor.parseExpression(
                                                new Document(), expression, leftExpression);
                                    } catch (ParseException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            });
                    return new ObjectIdFunction(column, rightExpression, inExpression);
                }
            }
        }
        return null;
    }

    /**
     * return a {@link DateFunction} if this {@link Expression} is a date.
     * @param incomingExpression
     * @return the {@link DateFunction} or null if not a date.
     * @throws ParseException
     */
    public static DateFunction getDateFunction(final Expression incomingExpression) throws ParseException {
        if (ComparisonOperator.class.isInstance(incomingExpression)) {
            ComparisonOperator comparisonOperator = (ComparisonOperator) incomingExpression;
            String rightExpression = getStringValue(comparisonOperator.getRightExpression());
            if (Function.class.isInstance(comparisonOperator.getLeftExpression())) {
                Function function = ((Function) comparisonOperator.getLeftExpression());
                if ("date".equals(function.getName().toLowerCase())
                        && (function.getParameters().getExpressions().size() == 2)
                        && StringValue.class.isInstance(function.getParameters().getExpressions().get(1))) {
                    String column = getStringValue(function.getParameters().getExpressions().get(0));
                    try {
                        return new DateFunction(
                                ((StringValue) (function.getParameters().getExpressions().get(1))).getValue(),
                                rightExpression, column, comparisonOperator);
                    } catch (IllegalArgumentException e) {
                        throw new ParseException(e);
                    }
                }

            }
        }
        return null;
    }

    /**
     * return a {@link RegexFunction} if this {@link Expression} is a regex.
     * @param incomingExpression
     * @return the {@link RegexFunction} or null if not a regex.
     * @throws ParseException
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static RegexFunction isRegexFunction(final Expression incomingExpression) throws ParseException {
        if (EqualsTo.class.isInstance(incomingExpression)) {
            EqualsTo equalsTo = (EqualsTo) incomingExpression;
            String rightExpression = equalsTo.getRightExpression().toString();
            if (Function.class.isInstance(equalsTo.getLeftExpression())) {
                Function function = ((Function) equalsTo.getLeftExpression());
                if ((REGEXMATCH_FUNCTION.equalsIgnoreCase(function.getName())
                        || NOT_REGEXMATCH_FUNCTION.equalsIgnoreCase(function.getName()))
                        && (function.getParameters().getExpressions().size() == 2
                        || function.getParameters().getExpressions().size() == 3)
                        && StringValue.class.isInstance(function.getParameters().getExpressions().get(1))) {

                    final Boolean rightExpressionValue = Boolean.valueOf(rightExpression);

                    isTrue(rightExpressionValue, "false is not allowed for regexMatch function");

                    RegexFunction regexFunction = getRegexFunction(function,
                            NOT_REGEXMATCH_FUNCTION.equalsIgnoreCase(function.getName()));
                    return regexFunction;
                }

            }
        } else if (Function.class.isInstance(incomingExpression)) {
            Function function = ((Function) incomingExpression);
            if ((REGEXMATCH_FUNCTION.equalsIgnoreCase(function.getName())
                    || NOT_REGEXMATCH_FUNCTION.equalsIgnoreCase(function.getName()))
                    && (function.getParameters().getExpressions().size() == 2
                    || function.getParameters().getExpressions().size() == 3)
                    && StringValue.class.isInstance(function.getParameters().getExpressions().get(1))) {

                RegexFunction regexFunction = getRegexFunction(function,
                        NOT_REGEXMATCH_FUNCTION.equalsIgnoreCase(function.getName()));
                return regexFunction;
            }
        }
        return null;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private static RegexFunction getRegexFunction(final Function function,
                                                  final boolean isNot) throws ParseException {
        final String column = getStringValue(function.getParameters().getExpressions().get(0));
        final String regex = fixDoubleSingleQuotes(
                ((StringValue) (function.getParameters().getExpressions().get(1))).getValue());
        try {
            Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new ParseException(e);
        }
        RegexFunction regexFunction = new RegexFunction(column, regex, isNot);

        if (function.getParameters().getExpressions().size() == 3 && StringValue.class
                .isInstance(function.getParameters().getExpressions().get(2))) {
            regexFunction.setOptions(
                    ((StringValue) (function.getParameters().getExpressions().get(2))).getValue());
        }
        return regexFunction;
    }

    /**
     * <p>Validate that the argument condition is <code>true</code>; otherwise
     * throwing an exception with the specified message. This method is useful when
     * validating according to an arbitrary boolean expression, such as validating an
     * object or using your own custom validation expression.</p>
     *
     * <pre>SqlUtils.isTrue( myObject.isOk(), "The object is not OK: ");</pre>
     *
     * <p>For performance reasons, the object value is passed as a separate parameter and
     * appended to the exception message only in the case of an error.</p>
     *
     * @param expression the boolean expression to check
     * @param message the exception message if invalid
     * @throws ParseException if expression is <code>false</code>
     */
    public static void isTrue(final boolean expression, final String message) throws ParseException {
        if (!expression) {
            throw new ParseException(message);
        }
    }

    /**
     * <p>Validate that the argument condition is <code>false</code>; otherwise
     * throwing an exception with the specified message. This method is useful when
     * validating according to an arbitrary boolean expression, such as validating an
     * object or using your own custom validation expression.</p>
     *
     * <pre>SqlUtils.isFalse( myObject.isOk(), "The object is not OK: ");</pre>
     *
     * <p>For performance reasons, the object value is passed as a separate parameter and
     * appended to the exception message only in the case of an error.</p>
     *
     * @param expression the boolean expression to check
     * @param message the exception message if invalid
     * @throws ParseException if expression is <code>false</code>
     */
    public static void isFalse(final Boolean expression, final String message) throws ParseException {
        if (expression) {
            throw new ParseException(message);
        }
    }

    /**
     * Is the expression a column.
     * @param expression
     * @return true if is a column.
     */
    public static boolean isColumn(final Expression expression) {
        return (expression instanceof Column && !((Column) expression).getName(false).matches("^(\".*\"|true|false)$"));
    }

    /**
     * Remove tablename from column.  For instance will rename column from c.column1 to column1.
     * @param column
     * @param aliasBase
     * @return the column without the tablename
     */
    public static Column removeAliasFromColumn(final Column column, final String aliasBase) {
        column.setColumnName(column.getName(false).startsWith(aliasBase + ".")
                ? column.getName(false).substring(aliasBase.length() + 1) : column.getName(false));
        column.setTable(null);
        return column;
    }

    /**
     * For nested fields we need it, no alias, clear first part "t1."column1.nested1, alias is mandatory.
     * @param column
     * @return the nested fields without the first part
     */
    public static String getColumnNameFromColumn(final Column column) {
        String[] splitedNestedField = column.getName(false).split("\\.");
        if (splitedNestedField.length > 2) {
            return String.join(".", Arrays.copyOfRange(splitedNestedField, 1, splitedNestedField.length));
        } else {
            return splitedNestedField[splitedNestedField.length - 1];
        }
    }

    /**
     * Will return true if the alias is an alias referenced in the column.  For instance, i.e: tableAlias r is a
     * table alias in the column r.cuisine.
     * @param column
     * @param tableAlias
     * @return true if the alias is an alias referenced in the column.
     */
    public static boolean isTableAliasOfColumn(final Column column, final String tableAlias) {
        String columnName = column.getName(false);
        return columnName.startsWith(tableAlias);
    }

    /**
     * If join starts with the text "join " set the join as an inner join.
     * @param j
     */
    public static void updateJoinType(final Join j) {
        if (j.toString().toLowerCase().startsWith("join ")) {
            j.setInner(true);
        }
    }

    /**
     * Is this expression one that would justify aggregation, like: max().
     * @param field
     * @return true if the expession would justify aggregation.
     */
    public static boolean isAggregateExpression(final String field) {
        String fieldForAgg = field.trim().toLowerCase();
        return fieldForAgg.startsWith("sum(") || fieldForAgg.startsWith("avg(")
                || fieldForAgg.startsWith("min(") || fieldForAgg.startsWith("max(")
                || fieldForAgg.startsWith("count(");
    }

    /**
     * Get the name for field to be used in aggregation based on the function name and the alias.
     * @param f
     * @param alias
     * @return the name for the field.
     * @throws ParseException
     */
    public static String generateAggField(final Function f, final Alias alias) throws ParseException {
        String aliasStr = (alias == null ? null : alias.getName());
        return generateAggField(f, aliasStr);
    }

    /**
     * Get the name for field to be used in aggregation based on the function name and the alias.
     * @param f
     * @param alias
     * @return the name for the field.
     * @throws ParseException
     */
    public static String generateAggField(final Function f, final String alias) throws ParseException {
        String field = getFieldFromFunction(f);
        String function = f.getName().toLowerCase();
        if ("*".equals(field) || function.equals("count")) {
            return (alias == null ? function : alias);
        } else {
            return (alias == null ? function + "_" + field.replaceAll("\\.", "_") : alias);
        }

    }

    /**
     * Get field name from the function, i.e: MAX(advance_amount) -> advance_amount.
     * @param function
     * @return the field name from the function.
     * @throws ParseException
     */
    public static String getFieldFromFunction(final Function function) throws ParseException {
        List<String> parameters = function.getParameters() == null
                ? Collections.<String>emptyList() : Lists.transform(function.getParameters().getExpressions(),
                new com.google.common.base.Function<Expression, String>() {
                    @Override
                    public String apply(@Nonnull final Expression expression) {
                        return SqlUtils.getStringValue(expression);
                    }
                });
        if (parameters.size() > 1) {
            throw new ParseException(function.getName() + " function can only have one parameter");
        }
        return parameters.size() > 0 ? Iterables.get(parameters, 0) : null;
    }

    /**
     * Will prepend $ to the expression if it is a column.
     * @param exp
     * @return string with prepended $ to the expression if it is a column.
     * @throws ParseException
     */
    public static Object nonFunctionToNode(final Expression exp) throws ParseException {
        return (SqlUtils.isColumn(exp) && !exp.toString().startsWith("$"))
                ? ("$" + exp) : getNormalizedValue(exp, null, FieldType.UNKNOWN, null);
    }

    /**
     * Will return true if any of the {@link SelectItem}s has a function that justifies aggregation like max().
     * @param selectItems
     * @return true if any of the {@link SelectItem}s has a function that justifies aggregation like max()
     */
    public static boolean isTotalGroup(final List<SelectItem> selectItems) {
        for (SelectItem sitem : selectItems) {
            if (isAggregateExpression(sitem.toString())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Clone an {@link Expression}.
     * @param expression the expression
     * @return the clone of an expression
     */
    public static Expression cloneExpression(final Expression expression) {
        if (expression == null) {
            return null;
        }
        try {
            return CCJSqlParserUtil.parseCondExpression(expression.toString());
        } catch (JSQLParserException e) {
            // Never exception because clone
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Will translate function name for speciality function names.
     * @param functionName
     * @return the translated function name.
     * @throws ParseException
     */
    public static String translateFunctionName(final String functionName) throws ParseException {
        String transfunction = FUNCTION_MAPPER.get(functionName);
        if (transfunction != null) {
            return transfunction;
        } else {
            return functionName;
        }

    }

}
