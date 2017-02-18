package com.github.vincentrussell.query.mongodb.sql.converter;

import com.google.common.base.*;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.google.common.base.MoreObjects.firstNonNull;

public class QueryConverter {

    private static final DateTimeFormatter YY_MM_DDFORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final DateTimeFormatter YYMMDDFORMATTER = DateTimeFormat.forPattern("yyyyMMdd");

    private static final Collection<DateTimeFormatter> FORMATTERS = Collections.unmodifiableList(Arrays.asList(
            ISODateTimeFormat.dateTime(),
            YY_MM_DDFORMATTER,
            YYMMDDFORMATTER));

    public static final String D_AGGREGATION_ALLOW_DISK_USE = "aggregationAllowDiskUse";
    public static final String D_AGGREGATION_BATCH_SIZE = "aggregationBatchSize";
    private static Pattern SURROUNDED_IN_QUOTES = Pattern.compile("^\"(.+)*\"$");
    private static Pattern LIKE_RANGE_REGEX = Pattern.compile("(\\[.+?\\])");
    private final MongoDBQueryHolder mongoDBQueryHolder;

    private final List<SelectItem> selectItems;
    private final Expression where;
    private final List<Join> joins;
    private final List<OrderByElement> orderByElements;
    private final List<String> groupBys;
    private final String table;
    private final boolean isDistinct;
    private final boolean isCountAll;
    private final long limit;
    private final Map<String,FieldType> fieldNameToFieldTypeMapping;
    private final FieldType defaultFieldType;

    /**
     * Create a QueryConverter with a string
     * @param sql the sql statement
     * @throws ParseException when the sql query cannot be parsed
     */
    public QueryConverter(String sql) throws ParseException {
        this(new ByteArrayInputStream(sql.getBytes()), Collections.<String, FieldType>emptyMap(), FieldType.UNKNOWN);
    }

    /**
     * Create a QueryConverter with a string
     * @param sql the sql statement
     * @param fieldNameToFieldTypeMapping mapping for each field
     * @throws ParseException when the sql query cannot be parsed
     */
    public QueryConverter(String sql, Map<String,FieldType> fieldNameToFieldTypeMapping) throws ParseException {
        this(new ByteArrayInputStream(sql.getBytes()),fieldNameToFieldTypeMapping, FieldType.UNKNOWN);
    }

    /**
     * Create a QueryConverter with a string
     * @param sql sql string
     * @param fieldType the default {@link FieldType} to be used
     * @throws ParseException
     */
    public QueryConverter(String sql, FieldType fieldType) throws ParseException {
        this(new ByteArrayInputStream(sql.getBytes()), Collections.<String, FieldType>emptyMap(), fieldType);
    }

    /**
     * Create a QueryConverter with a string
     * @param sql sql string
     * @param fieldNameToFieldTypeMapping mapping for each field
     * @param defaultFieldType defaultFieldType the default {@link FieldType} to be used
     * @throws ParseException
     */
    public QueryConverter(String sql, ImmutableMap<String, FieldType> fieldNameToFieldTypeMapping, FieldType defaultFieldType) throws ParseException {
        this(new ByteArrayInputStream(sql.getBytes()), fieldNameToFieldTypeMapping, defaultFieldType);
    }

    /**
     * Create a QueryConverter with a string
     * @param inputStream an input stream that has the sql statement in it
     * @throws ParseException when the sql query cannot be parsed
     */
    public QueryConverter(InputStream inputStream) throws ParseException {
        this(inputStream,Collections.<String, FieldType>emptyMap(), FieldType.UNKNOWN);
    }

    /**
     * Create a QueryConverter with an InputStream
     * @param inputStream an input stream that has the sql statement in it
     * @param fieldNameToFieldTypeMapping mapping for each field
     * @param defaultFieldType the default {@link FieldType} to be used
     * @throws ParseException when the sql query cannot be parsed
     */
    public QueryConverter(InputStream inputStream, Map<String,FieldType> fieldNameToFieldTypeMapping,
                          FieldType defaultFieldType) throws ParseException {
        CCJSqlParser jSqlParser = new CCJSqlParser(inputStream);
        try {
            this.defaultFieldType = defaultFieldType != null ? defaultFieldType : FieldType.UNKNOWN;
            final PlainSelect plainSelect = jSqlParser.PlainSelect();
            this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping != null
                    ? fieldNameToFieldTypeMapping : Collections.<String, FieldType>emptyMap();
            isTrue(plainSelect!=null,"could not parseNaturalLanguageDate SELECT statement from query");
            isDistinct = (plainSelect.getDistinct() != null);
            isCountAll = isCountAll(plainSelect.getSelectItems());
            where = plainSelect.getWhere();
            orderByElements = plainSelect.getOrderByElements();
            selectItems = plainSelect.getSelectItems();
            joins = plainSelect.getJoins();
            groupBys = getGroupByColumnReferences(plainSelect);
            isTrue(plainSelect.getFromItem()!=null,"could not find table to query.  Only one simple table name is supported.");
            table = plainSelect.getFromItem().toString();
            limit = getLimit(plainSelect.getLimit());
            mongoDBQueryHolder = getMongoQueryInternal();
            validate();
        } catch (net.sf.jsqlparser.parser.ParseException e) {
            throw convertParseException(e);
        }
    }

    private long getLimit(Limit limit) throws ParseException {
        if (limit!=null) {
            long incomingLimit = limit.getRowCount();
            BigInteger bigInt = new BigInteger(""+incomingLimit);
            isFalse(bigInt.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0, incomingLimit + ": value is too large");
            return limit.getRowCount();
        }
        return -1;
    }

    private List<String> getGroupByColumnReferences(PlainSelect plainSelect) {
        if (plainSelect.getGroupByColumnReferences()==null) {
            return Collections.emptyList();
        }
        return Lists.transform(plainSelect.getGroupByColumnReferences(), new com.google.common.base.Function<Expression, String>() {
            @Override
            public String apply(Expression expression) {
                return getStringValue(expression);
            }
        });
    }

    private ParseException convertParseException(net.sf.jsqlparser.parser.ParseException incomingException) {
        try {
            return new ParseException(new Token(incomingException.currentToken.kind,
                    incomingException.currentToken.image), incomingException.expectedTokenSequences,
                    incomingException.tokenImage);
        } catch (NullPointerException e1) {
            if (incomingException.getMessage().contains("Was expecting:\n" +
                    "    \"SELECT\"")) {
                return new ParseException("Could not parseNaturalLanguageDate query.  Only select statements are supported.");
            }
            if (incomingException.getMessage()!=null) {
                return new ParseException(incomingException.getMessage());
            }
            return new ParseException("Count not parseNaturalLanguageDate query.");
        }
    }

    private void validate() throws ParseException {
        List<SelectItem> filteredItems = Lists.newArrayList(Iterables.filter(selectItems, new Predicate<SelectItem>() {
            @Override
            public boolean apply(SelectItem selectItem) {
                try {
                    if (SelectExpressionItem.class.isInstance(selectItem)
                            && Column.class.isInstance(((SelectExpressionItem) selectItem).getExpression())) {
                        return true;
                    }
                } catch (NullPointerException e) {
                    return false;
                }
                return false;
            }
        }));

        isFalse((selectItems.size() >1 || isSelectAll(selectItems)) && isDistinct,"cannot run distinct one more than one column");
        isFalse(groupBys.size() == 0 && selectItems.size()!=filteredItems.size() && !isSelectAll(selectItems)
                && !isCountAll(selectItems),"illegal expression(s) found in select clause.  Only column names supported");
        isTrue(joins==null,"Joins are not supported.  Only one simple table name is supported.");
    }

    /**
     * get the object that you need to submit a query
     * @return the {@link com.github.vincentrussell.query.mongodb.sql.converter.MongoDBQueryHolder}
     * that contains all that is needed to describe the query to be run.
     */
    public MongoDBQueryHolder getMongoQuery() {
        return mongoDBQueryHolder;
    }

    private MongoDBQueryHolder getMongoQueryInternal() throws ParseException {
        MongoDBQueryHolder mongoDBQueryHolder = new MongoDBQueryHolder(table);
        Document document = new Document();
        if (isDistinct) {
            document.put(selectItems.get(0).toString(), 1);
            mongoDBQueryHolder.setProjection(document);
            mongoDBQueryHolder.setDistinct(isDistinct);
        } else if (groupBys.size() > 0) {
            mongoDBQueryHolder.setGroupBys(groupBys);
            mongoDBQueryHolder.setProjection(createProjectionsFromSelectItems(selectItems,groupBys));
        } else if (isCountAll) {
            mongoDBQueryHolder.setCountAll(isCountAll);
        } else if (!isSelectAll(selectItems)) {
            document.put("_id",0);
            for (SelectItem selectItem : selectItems) {
                document.put(selectItem.toString(),1);
            }
            mongoDBQueryHolder.setProjection(document);
        }

        if (orderByElements!=null && orderByElements.size() > 0) {
            mongoDBQueryHolder.setSort(createSortInfoFromOrderByElements(orderByElements));
        }

        if (where!=null) {
            mongoDBQueryHolder.setQuery((Document) parseExpression(new Document(), where, null));
        }
        mongoDBQueryHolder.setLimit(limit);
        return mongoDBQueryHolder;
    }

    private Document createSortInfoFromOrderByElements(List<OrderByElement> orderByElements) throws ParseException {
        Document document = new Document();
        if (orderByElements==null && orderByElements.size()==0) {
            return document;
        }

        final List<OrderByElement> functionItems = Lists.newArrayList(Iterables.filter(orderByElements, new Predicate<OrderByElement>() {
            @Override
            public boolean apply(OrderByElement orderByElement) {
                try {
                    if (Function.class.isInstance(orderByElement.getExpression())) {
                        return true;
                    }
                } catch (NullPointerException e) {
                    return false;
                }
                return false;
            }
        }));
        final List<OrderByElement> nonFunctionItems = Lists.newArrayList(Collections2.filter(orderByElements, new Predicate<OrderByElement>() {
            @Override
            public boolean apply(OrderByElement orderByElement) {
                return !functionItems.contains(orderByElement);
            }

        }));

        Document sortItems = new Document();
        for (OrderByElement orderByElement : orderByElements) {
            if (nonFunctionItems.contains(orderByElement)) {
                sortItems.put(getStringValue(orderByElement.getExpression()),orderByElement.isAsc() ? 1 : -1);
            } else {
                Function function = (Function) orderByElement.getExpression();
                Document parseFunctionDocument = new Document();
                parseFunctionForAggregation(function,parseFunctionDocument,Collections.<String>emptyList());
                sortItems.put(Iterables.get(parseFunctionDocument.keySet(),0),orderByElement.isAsc() ? 1 : -1);
            }
        }

        return sortItems;
    }

    private Document createProjectionsFromSelectItems(List<SelectItem> selectItems, List<String> groupBys) throws ParseException {
        Document document = new Document();
        if (selectItems==null && selectItems.size()==0) {
            return document;
        }

        final List<SelectItem> functionItems = Lists.newArrayList(Iterables.filter(selectItems, new Predicate<SelectItem>() {
            @Override
            public boolean apply(SelectItem selectItem) {
                try {
                    if (SelectExpressionItem.class.isInstance(selectItem)
                            && Function.class.isInstance(((SelectExpressionItem) selectItem).getExpression())) {
                        return true;
                    }
                } catch (NullPointerException e) {
                    return false;
                }
                return false;
            }
        }));
        final List<SelectItem> nonFunctionItems = Lists.newArrayList(Collections2.filter(selectItems, new Predicate<SelectItem>() {
            @Override
            public boolean apply(SelectItem selectItem) {
                return !functionItems.contains(selectItem);
            }

        }));

        isTrue(functionItems.size() > 0, "there must be at least one group by function specified in the select clause");
        isTrue(nonFunctionItems.size() > 0, "there must be at least one non-function column specified");


        Document idDocument = new Document();
        for (SelectItem selectItem : nonFunctionItems) {
            Column column = (Column) ((SelectExpressionItem) selectItem).getExpression();
            String columnName = getStringValue(column);
            idDocument.put(columnName,"$" + columnName);
        }

        document.append("_id", idDocument.size() == 1 ? Iterables.get(idDocument.values(),0) : idDocument);

        for (SelectItem selectItem : functionItems) {
            Function function = (Function) ((SelectExpressionItem)selectItem).getExpression();
            parseFunctionForAggregation(function,document,groupBys);
        }

        return document;
    }

    private void parseFunctionForAggregation(Function function, Document document, List<String> groupBys) throws ParseException {
        List<String> parameters = function.getParameters()== null ? Collections.<String>emptyList() : Lists.transform(function.getParameters().getExpressions(), new com.google.common.base.Function<Expression, String>() {
            @Override
            public String apply(Expression expression) {
                return getStringValue(expression);
            }
        });
        if (parameters.size() > 1) {
            throw new ParseException(function.getName()+" function can only have one parameter");
        }
        String field = parameters.size() > 0 ? Iterables.get(parameters, 0).replaceAll("\\.","_") : null;
        if ("sum".equals(function.getName().toLowerCase())) {
            createFunction("sum",field, document,"$"+ field);
        } else if ("avg".equals(function.getName().toLowerCase())) {
            createFunction("avg",field, document,"$"+ field);
        } else if ("count".equals(function.getName().toLowerCase())) {
            document.put("count",new Document("$sum",1));
        } else if ("min".equals(function.getName().toLowerCase())) {
            createFunction("min",field, document,"$"+ field);
        } else if ("max".equals(function.getName().toLowerCase())) {
            createFunction("max",field, document,"$"+ field);
        } else {
            throw new ParseException("could not understand function:" + function.getName());
        }
    }

    private void createFunction(String functionName, String field, Document document, Object value) throws ParseException {
        isTrue(field!=null,"function "+ functionName + " must contain a single field to run on");
        document.put(functionName + "_"+field,new Document("$"+functionName,value));
    }


    private Object parseExpression(Document query,Expression incomingExpression, Expression otherSide) throws ParseException {
        if (ComparisonOperator.class.isInstance(incomingExpression)) {
            RegexFunction regexFunction = isRegexFunction(incomingExpression);
            DateFunction dateFunction = isDateFunction(incomingExpression);
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
                query.put("$not",new Document(parseExpression(new Document(), leftExpression, rightExpression).toString(),parseExpression(new Document(), rightExpression, leftExpression)));
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
            String stringValueLeftSide = getStringValue(likeExpression.getLeftExpression());
            String stringValueRightSide = getStringValue(likeExpression.getRightExpression());
            Document document = new Document("$regex", "^" + replaceRegexCharacters(stringValueRightSide) + "$");
            if (likeExpression.isNot()) {
                document = new Document("$not",new Document(stringValueLeftSide,document));
            } else {
                document = new Document(stringValueLeftSide,document);
            }
            query.putAll(document);
        } else if(IsNullExpression.class.isInstance(incomingExpression)) {
            IsNullExpression isNullExpression = (IsNullExpression) incomingExpression;
            query.put(isNullExpression.getLeftExpression().toString(),new Document("$exists",isNullExpression.isNot()));
        } else if(AndExpression.class.isInstance(incomingExpression)) {
            AndExpression andExpression = (AndExpression) incomingExpression;
            final Expression leftExpression = andExpression.getLeftExpression();
            final Expression rightExpression = andExpression.getRightExpression();
            query.put("$and", Arrays.asList(parseExpression(new Document(), leftExpression, rightExpression), parseExpression(new Document(), rightExpression, leftExpression)));
        } else if(InExpression.class.isInstance(incomingExpression)) {
            final InExpression inExpression = (InExpression) incomingExpression;
            final Expression leftExpression = ((InExpression) incomingExpression).getLeftExpression();
            final String leftExpressionAsString = getStringValue(leftExpression);
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
        } else if(OrExpression.class.isInstance(incomingExpression)) {
            OrExpression orExpression = (OrExpression) incomingExpression;
            final Expression leftExpression = orExpression.getLeftExpression();
            final Expression rightExpression = orExpression.getRightExpression();
            query.put("$or", Arrays.asList(parseExpression(new Document(), leftExpression, rightExpression),
                    parseExpression(new Document(), rightExpression, leftExpression)));
        } else if(Parenthesis.class.isInstance(incomingExpression)) {
            Parenthesis parenthesis = (Parenthesis) incomingExpression;
            return parseExpression(new Document(),parenthesis.getExpression(),null);
        } else {
            return getValue(incomingExpression,otherSide);
        }
        return query;
    }

    private Object getValue(Expression incomingExpression, Expression otherSide) throws ParseException {
        FieldType fieldType = firstNonNull(fieldNameToFieldTypeMapping.get(getStringValue(otherSide)),
                defaultFieldType);
        if (LongValue.class.isInstance(incomingExpression)) {
            return normalizeValue((((LongValue)incomingExpression).getValue()),fieldType);
        } else if (StringValue.class.isInstance(incomingExpression)) {
            return normalizeValue((((StringValue)incomingExpression).getValue()),fieldType);
        } else if (Column.class.isInstance(incomingExpression)) {
            return normalizeValue(getStringValue(incomingExpression),fieldType);
        } else {
            throw new ParseException("can not parseNaturalLanguageDate: " + incomingExpression.toString());
        }
    }

    private Object normalizeValue(Object value, FieldType fieldType) throws ParseException {
        if (fieldType==null || FieldType.UNKNOWN.equals(fieldType)) {
            return value;
        } else {
            if (FieldType.STRING.equals(fieldType)) {
                return forceString(value);
            }
            if (FieldType.NUMBER.equals(fieldType)) {
                return forceNumber(value);
            }
            if (FieldType.DATE.equals(fieldType)) {
                return forceDate(value);
            }
        }
        throw new ParseException("could not normalize value:" + value);
    }

    private Object forceDate(Object value) throws ParseException {
        if (String.class.isInstance(value)){
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

    private static Date parseNaturalLanguageDate(String text) {
        Parser parser = new Parser();
        List<DateGroup> groups = parser.parse(text);
        for (DateGroup group : groups) {
            List<Date> dates = group.getDates();
            if (dates.size() > 0) {
                return dates.get(0);
            }
        }
        throw new IllegalArgumentException("could not natural language date: "+ text);
    }

    private Object forceNumber(Object value) throws ParseException {
        if (String.class.isInstance(value)){
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e1) {
                try {
                    return Double.parseDouble((String) value);
                } catch (NumberFormatException e2) {
                    try {
                        return Float.parseFloat((String) value);
                    } catch (NumberFormatException e3) {
                        throw new ParseException("could not convert " + value + " to number");
                    }
                }
            }
        } else {
            return value;
        }
    }

    private String forceString(Object value) {
        if (String.class.isInstance(value)){
            return (String) value;
        } else {
            return "" + value + "";
        }
    }

    private String replaceRegexCharacters(String value) {
        String newValue = value.replaceAll("%",".*")
                .replaceAll("_",".{1}");

        Matcher m = LIKE_RANGE_REGEX.matcher(newValue);
        StringBuffer sb = new StringBuffer();
        while(m.find())  {
            m.appendReplacement(sb, m.group(1) + "{1}");
        }
        m.appendTail(sb);

        return sb.toString();
    }

    private static String replaceGroup(String source, int groupToReplace, int groupOccurrence, String replacement) {
        Matcher m = LIKE_RANGE_REGEX.matcher(source);
        for (int i = 0; i < groupOccurrence; i++)
            if (!m.find()) return source; // pattern not met, may also throw an exception here
        return new StringBuilder(source).replace(m.start(groupToReplace), m.end(groupToReplace), replacement).toString();
    }


    private DateFunction isDateFunction(Expression incomingExpression) throws ParseException {
        if (ComparisonOperator.class.isInstance(incomingExpression)) {
            ComparisonOperator comparisonOperator = (ComparisonOperator)incomingExpression;
            String rightExpression = getStringValue(comparisonOperator.getRightExpression());
            if (Function.class.isInstance(comparisonOperator.getLeftExpression())) {
                Function function = ((Function)comparisonOperator.getLeftExpression());
                if ("date".equals(function.getName().toLowerCase())
                        && (function.getParameters().getExpressions().size()==2)
                        && StringValue.class.isInstance(function.getParameters().getExpressions().get(1))) {
                    String column = getStringValue(function.getParameters().getExpressions().get(0));
                    DateFunction dateFunction = null;
                    try {
                        dateFunction = new DateFunction(((StringValue)(function.getParameters().getExpressions().get(1))).getValue(),rightExpression,column);
                        dateFunction.setComparisonFunction(comparisonOperator);
                    } catch (IllegalArgumentException e) {
                        throw new ParseException(e.getMessage());
                    }
                    return dateFunction;
                }

            }
        }
        return null;
    }

    private String getStringValue(Expression expression) {
        if (StringValue.class.isInstance(expression)) {
            return ((StringValue)expression).getValue();
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

    private RegexFunction isRegexFunction(Expression incomingExpression) throws ParseException {
        if (EqualsTo.class.isInstance(incomingExpression)) {
            EqualsTo equalsTo = (EqualsTo)incomingExpression;
            String rightExpression = equalsTo.getRightExpression().toString();
            if (Function.class.isInstance(equalsTo.getLeftExpression())) {
                Function function = ((Function)equalsTo.getLeftExpression());
                if ("regexmatch".equals(function.getName().toLowerCase())
                        && (function.getParameters().getExpressions().size()==2
                            || function.getParameters().getExpressions().size()==3)
                        && "true".equals(rightExpression)
                        && StringValue.class.isInstance(function.getParameters().getExpressions().get(1))) {
                    String column = getStringValue(function.getParameters().getExpressions().get(0));
                    String regex = ((StringValue)(function.getParameters().getExpressions().get(1))).getValue();
                    try {
                        Pattern.compile(regex);
                    } catch (PatternSyntaxException e) {
                        throw new ParseException(e.getMessage());
                    }
                    RegexFunction regexFunction = new RegexFunction(column,regex);

                    if (function.getParameters().getExpressions().size()==3 && StringValue.class.isInstance(function.getParameters().getExpressions().get(2))) {
                        regexFunction.setOptions(((StringValue)(function.getParameters().getExpressions().get(2))).getValue());
                    }

                    return regexFunction;
                }

            }
        }
        return null;
    }

    private boolean isSelectAll(List<SelectItem> selectItems) {
        if (selectItems !=null && selectItems.size()==1) {
            SelectItem firstItem = selectItems.get(0);
            return AllColumns.class.isInstance(firstItem);
        } else {
            return false;
        }
    }

    private boolean isCountAll(List<SelectItem> selectItems) {
        if (selectItems !=null && selectItems.size()==1) {
            SelectItem firstItem = selectItems.get(0);
            if ((SelectExpressionItem.class.isInstance(firstItem))
                    && Function.class.isInstance(((SelectExpressionItem)firstItem).getExpression())) {
                Function function = (Function) ((SelectExpressionItem) firstItem).getExpression();

                if ("count(*)".equals(function.toString())) {
                    return true;
                }

            }
        }
        return false;
    }

    /**
     * Build a mongo shell statement with the code to run the specified query.
     * @param outputStream the {@link java.io.OutputStream} to write the data to
     * @throws IOException when there is an issue writing to the {@link java.io.OutputStream}
     */
    public void write(OutputStream outputStream) throws IOException {
        MongoDBQueryHolder mongoDBQueryHolder = getMongoQuery();
        if (mongoDBQueryHolder.isDistinct()) {
            IOUtils.write("db." + mongoDBQueryHolder.getCollection() + ".distinct(", outputStream);
            IOUtils.write("\""+getDistinctFieldName(mongoDBQueryHolder) + "\"", outputStream);
            IOUtils.write(" , ", outputStream);
            IOUtils.write(prettyPrintJson(mongoDBQueryHolder.getQuery().toJson()), outputStream);
        } else if (groupBys.size() > 0) {
            IOUtils.write("db." + mongoDBQueryHolder.getCollection() + ".aggregate(", outputStream);
            IOUtils.write("[", outputStream);
            List<Document> documents = new ArrayList<>();
            documents.add(new Document("$match",mongoDBQueryHolder.getQuery()));
            documents.add(new Document("$group",mongoDBQueryHolder.getProjection()));

            if (mongoDBQueryHolder.getSort()!=null && mongoDBQueryHolder.getSort().size() > 0) {
                documents.add(new Document("$sort",mongoDBQueryHolder.getSort()));
            }

            if (mongoDBQueryHolder.getLimit()!= -1) {
                documents.add(new Document("$limit",mongoDBQueryHolder.getLimit()));
            }

            IOUtils.write(Joiner.on(",").join(Lists.transform(documents, new com.google.common.base.Function<Document, String>() {
                @Override
                public String apply(Document document) {
                    return prettyPrintJson(document.toJson());
                }
            })),outputStream);
            IOUtils.write("]", outputStream);

            Document options = new Document();
            if (System.getProperty(D_AGGREGATION_ALLOW_DISK_USE)!=null) {
                options.put("allowDiskUse",Boolean.valueOf(System.getProperty(D_AGGREGATION_ALLOW_DISK_USE)));
            }

            if (System.getProperty(D_AGGREGATION_BATCH_SIZE)!=null) {
                options.put("cursor",new Document("batchSize",Integer.valueOf(System.getProperty(D_AGGREGATION_BATCH_SIZE))));
            }

            if (options.size() > 0) {
                IOUtils.write(",",outputStream);
                IOUtils.write(prettyPrintJson(options.toJson()),outputStream);
            }



        } else if (isCountAll) {
            IOUtils.write("db." + mongoDBQueryHolder.getCollection() + ".count(", outputStream);
            IOUtils.write(prettyPrintJson(mongoDBQueryHolder.getQuery().toJson()), outputStream);
        } else {
            IOUtils.write("db." + mongoDBQueryHolder.getCollection() + ".find(", outputStream);
            IOUtils.write(prettyPrintJson(mongoDBQueryHolder.getQuery().toJson()), outputStream);
            if (mongoDBQueryHolder.getProjection() != null && mongoDBQueryHolder.getProjection().size() > 0) {
                IOUtils.write(" , ", outputStream);
                IOUtils.write(prettyPrintJson(mongoDBQueryHolder.getProjection().toJson()), outputStream);
            }
        }
        IOUtils.write(")", outputStream);

        if (mongoDBQueryHolder.getSort()!=null && mongoDBQueryHolder.getSort().size() > 0 && !isCountAll && !isDistinct && groupBys.isEmpty()) {
            IOUtils.write(".sort(", outputStream);
            IOUtils.write(prettyPrintJson(mongoDBQueryHolder.getSort().toJson()), outputStream);
            IOUtils.write(")", outputStream);
        }

        if (mongoDBQueryHolder.getLimit()!=-1 && !isCountAll && !isDistinct && groupBys.isEmpty()) {
            IOUtils.write(".limit(", outputStream);
            IOUtils.write(mongoDBQueryHolder.getLimit()+"", outputStream);
            IOUtils.write(")", outputStream);
        }
    }

    private String getDistinctFieldName(MongoDBQueryHolder mongoDBQueryHolder) {
        return Iterables.get(mongoDBQueryHolder.getProjection().keySet(),0);
    }

    /**
     * @param mongoDatabase the database to run the query against.
     * @param <T> variable based on the type of query run.
     * @return When query does a find will return QueryResultIterator&lt;{@link org.bson.Document}&gt;
     *           When query does a count will return a Long
     *           When query does a distinct will return QueryResultIterator&lt;{@link java.lang.String}&gt;
     */
    @SuppressWarnings("unchecked")
    public <T> T run(MongoDatabase mongoDatabase) {
        MongoDBQueryHolder mongoDBQueryHolder = getMongoQuery();

        MongoCollection mongoCollection = mongoDatabase.getCollection(mongoDBQueryHolder.getCollection());

        if (mongoDBQueryHolder.isDistinct()) {
            return (T)new QueryResultIterator<>(mongoCollection.distinct(getDistinctFieldName(mongoDBQueryHolder), mongoDBQueryHolder.getQuery(), String.class));
        } else if (mongoDBQueryHolder.isCountAll()) {
            return (T)Long.valueOf(mongoCollection.count(mongoDBQueryHolder.getQuery()));
        } else if (groupBys.size() > 0) {
            List<Document> documents = new ArrayList<>();
            if (mongoDBQueryHolder.getQuery() !=null && mongoDBQueryHolder.getQuery().size() > 0) {
                documents.add(new Document("$match", mongoDBQueryHolder.getQuery()));
            }
            documents.add(new Document("$group",mongoDBQueryHolder.getProjection()));
            if (mongoDBQueryHolder.getSort()!=null && mongoDBQueryHolder.getSort().size() > 0) {
                documents.add(new Document("$sort",mongoDBQueryHolder.getSort()));
            }
            if (mongoDBQueryHolder.getLimit()!= -1) {
                documents.add(new Document("$limit",mongoDBQueryHolder.getLimit()));
            }
            AggregateIterable aggregate = mongoCollection.aggregate(documents);

            if (System.getProperty(D_AGGREGATION_ALLOW_DISK_USE)!=null) {
                aggregate.allowDiskUse(Boolean.valueOf(System.getProperty(D_AGGREGATION_ALLOW_DISK_USE)));
            }

            if (System.getProperty(D_AGGREGATION_BATCH_SIZE)!=null) {
                aggregate.batchSize(Integer.valueOf(System.getProperty(D_AGGREGATION_BATCH_SIZE)));
            }

            return (T)new QueryResultIterator<>(aggregate);
        } else {
            FindIterable findIterable = mongoCollection.find(mongoDBQueryHolder.getQuery()).projection(mongoDBQueryHolder.getProjection());
            if (mongoDBQueryHolder.getSort()!=null && mongoDBQueryHolder.getSort().size() > 0) {
                findIterable.sort(mongoDBQueryHolder.getSort());
            }
            if (mongoDBQueryHolder.getLimit()!= -1) {
                findIterable.limit((int)mongoDBQueryHolder.getLimit());
            }

            return (T)new QueryResultIterator<>(findIterable);
        }
    }


    private String prettyPrintJson(String json) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonParser jp = new JsonParser();
        JsonElement je = jp.parse(json);
        return gson.toJson(je);
    }

    private static class RegexFunction {
        private final String column;
        private final String regex;
        private String options;

        private RegexFunction(String column, String regex) {
            this.column = column;
            this.regex = regex;
        }

        public String getColumn() {
            return column;
        }

        public String getRegex() {
            return regex;
        }

        public void setOptions(String options) {
            this.options = options;
        }

        public String getOptions() {
            return options;
        }
    }

    private static class DateFunction {
        private final Date date;
        private final String column;
        private String comparisonExpression = "$eq";

        private DateFunction(String format,String value, String column) {
            if ("natural".equals(format)) {
                this.date = parseNaturalLanguageDate(value);
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

    private static void isTrue(boolean expression, String message) throws ParseException {
        if (!expression) {
            throw new ParseException(message);
        }
    }

    private static void isFalse(boolean expression, String message) throws ParseException {
        if (expression) {
            throw new ParseException(message);
        }
    }

}
