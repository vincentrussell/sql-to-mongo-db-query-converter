package com.github.vincentrussell.query.mongodb.sql.converter;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class QueryConverter {

    private static Pattern SURROUNDED_IN_QUOTES = Pattern.compile("^\"(.+)*\"$");
    private final MongoDBQueryHolder mongoDBQueryHolder;

    private final List<SelectItem> selectItems;
    private final Expression where;
    private final List<Join> joins;
    private final String table;
    private final boolean isDistinct;

    /**
     * Create a QueryConverter with a string
     * @param sql
     * @throws ParseException
     */
    public QueryConverter(String sql) throws ParseException {
        this(new ByteArrayInputStream(sql.getBytes()));
    }

    /**
     * Create a QueryConverter with an InputStream
     * @param inputStream
     * @throws ParseException
     */
    public QueryConverter(InputStream inputStream) throws ParseException {
        CCJSqlParser jSqlParser = new CCJSqlParser(inputStream);
        try {
            try {
                final PlainSelect plainSelect = jSqlParser.PlainSelect();
                isDistinct = (plainSelect.getDistinct() != null);
                where = plainSelect.getWhere();
                selectItems = plainSelect.getSelectItems();
                joins = plainSelect.getJoins();
                table = plainSelect.getFromItem().toString();
                mongoDBQueryHolder = getMongoQueryInternal();
            } catch (NullPointerException e) {
                throw new ParseException("Could not find table in query.  Only one simple table name is supported.");
            }
            validate();
        } catch (net.sf.jsqlparser.parser.ParseException e) {
            throw convertParseException(e);
        }
    }

    private ParseException convertParseException(net.sf.jsqlparser.parser.ParseException incomingException) {
        try {
            return new ParseException(new Token(incomingException.currentToken.kind, incomingException.currentToken.image), incomingException.expectedTokenSequences, incomingException.tokenImage);
        } catch (NullPointerException e1) {
            if (incomingException.getMessage().contains("Was expecting:\n" +
                    "    \"SELECT\"")) {
                return new ParseException("Could not parse query.  Only select statements are supported.");
            }
            return new ParseException("Count not parse query.");
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
        if ((selectItems.size() >1 || isSelectAll(selectItems)) && isDistinct) {
            throw new ParseException("cannot run distinct one more than one column");
        }
        if (selectItems.size()!=filteredItems.size() && !isSelectAll(selectItems)) {
            throw new ParseException("illegal expression(s) found in select clause.  Only column names supported");
        }

        if (joins!=null) {
            throw new ParseException("Joins are not supported.  Only one simple table name is supported.");
        }
    }

    /**
     * get the object that you need to submit a query
     * @return
     */
    public MongoDBQueryHolder getMongoQuery() {
        return mongoDBQueryHolder;
    }

    private MongoDBQueryHolder getMongoQueryInternal() throws ParseException {
        MongoDBQueryHolder mongoDBQueryHolder = new MongoDBQueryHolder(table);
        Document document = new Document();
        if (isDistinct) {
            document.put(selectItems.get(0).toString(),1);
            mongoDBQueryHolder.setProjection(document);
            mongoDBQueryHolder.setDistinct(isDistinct);
        } else if (!isSelectAll(selectItems)) {
            document.put("_id",0);
            for (SelectItem selectItem : selectItems) {
                document.put(selectItem.toString(),1);
            }
            mongoDBQueryHolder.setProjection(document);
        }
        if (where!=null) {
            mongoDBQueryHolder.setQuery((Document) parseExpression(new Document(), where));
        }
        return mongoDBQueryHolder;
    }

    private Object parseExpression(Document query, Expression incomingExpression) throws ParseException {
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
                query.put(dateFunction.getColumn(),new Document(dateFunction.getComparisonExpresion(),dateFunction.getDate()));
            } else if (EqualsTo.class.isInstance(incomingExpression)) {
                query.put(parseExpression(new Document(),((EqualsTo)incomingExpression).getLeftExpression()).toString(),parseExpression(new Document(),((EqualsTo)incomingExpression).getRightExpression()));
            } else if (NotEqualsTo.class.isInstance(incomingExpression)) {
                query.put("$not",new Document(parseExpression(new Document(),((NotEqualsTo)incomingExpression).getLeftExpression()).toString(),parseExpression(new Document(),((NotEqualsTo)incomingExpression).getRightExpression())));
            } else if (GreaterThan.class.isInstance(incomingExpression)) {
                query.put(((GreaterThan)incomingExpression).getLeftExpression().toString(),new Document("$gt",parseExpression(new Document(),((GreaterThan)incomingExpression).getRightExpression())));
            } else if (MinorThan.class.isInstance(incomingExpression)) {
                query.put(((MinorThan)incomingExpression).getLeftExpression().toString(),new Document("$lt", parseExpression(new Document(),((MinorThan)incomingExpression).getRightExpression())));
            } else if (GreaterThanEquals.class.isInstance(incomingExpression)) {
                query.put(((GreaterThanEquals)incomingExpression).getLeftExpression().toString(),new Document("$gte",parseExpression(new Document(),((GreaterThanEquals)incomingExpression).getRightExpression())));
            } else if (MinorThanEquals.class.isInstance(incomingExpression)) {
                query.put(((MinorThanEquals)incomingExpression).getLeftExpression().toString(),new Document("$lte", parseExpression(new Document(),((MinorThanEquals)incomingExpression).getRightExpression())));
            }
        } else if(IsNullExpression.class.isInstance(incomingExpression)) {
            IsNullExpression isNullExpression = (IsNullExpression) incomingExpression;
            query.put(isNullExpression.getLeftExpression().toString(),new Document("$exists",isNullExpression.isNot()));
        } else if(AndExpression.class.isInstance(incomingExpression)) {
            AndExpression andExpression = (AndExpression) incomingExpression;
            query.put("$and", Arrays.asList(parseExpression(new Document(),andExpression.getLeftExpression()), parseExpression(new Document(),andExpression.getRightExpression())));
        } else if(InExpression.class.isInstance(incomingExpression)) {
            InExpression inExpression = (InExpression) incomingExpression;
            query.put(inExpression.isNot() ? "$nin" : "$in", Lists.transform(((ExpressionList)inExpression.getRightItemsList()).getExpressions(), new com.google.common.base.Function<Expression, Object>() {
                @Override
                public Object apply(Expression expression) {
                    try {
                        return parseExpression(new Document(),expression);
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                }
            }));
        } else if(OrExpression.class.isInstance(incomingExpression)) {
            OrExpression orExpression = (OrExpression) incomingExpression;
            query.put("$or", Arrays.asList(parseExpression(new Document(), orExpression.getLeftExpression()), parseExpression(new Document(), orExpression.getRightExpression())));
        } else if(Parenthesis.class.isInstance(incomingExpression)) {
            Parenthesis parenthesis = (Parenthesis) incomingExpression;
            return parseExpression(new Document(),parenthesis.getExpression());
        } else if (LongValue.class.isInstance(incomingExpression)) {
            return (((LongValue)incomingExpression).getValue());
        } else if (StringValue.class.isInstance(incomingExpression)) {
            return (((StringValue)incomingExpression).getValue());
        } else if (Column.class.isInstance(incomingExpression)) {
            return getStringValue(incomingExpression);
        } else {
            throw new ParseException("can not parse: " + incomingExpression.toString());
        }
        return query;
    }

    private DateFunction isDateFunction(Expression incomingExpression) throws ParseException {
        if (ComparisonOperator.class.isInstance(incomingExpression)) {
            ComparisonOperator comparisonOperator = (ComparisonOperator)incomingExpression;
            String rightExpression = getStringValue(comparisonOperator.getRightExpression());
            if (Function.class.isInstance(comparisonOperator.getLeftExpression())) {
                Function function = ((Function)comparisonOperator.getLeftExpression());
                if ("date".equals(function.getName())
                        && (function.getParameters().getExpressions().size()==2)
                        && StringValue.class.isInstance(function.getParameters().getExpressions().get(1))) {
                    String column = function.getParameters().getExpressions().get(0).toString();
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
            String columnName = ((Column)expression).getColumnName();
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
                if ("regexMatch".equals(function.getName())
                        && (function.getParameters().getExpressions().size()==2
                            || function.getParameters().getExpressions().size()==3)
                        && "true".equals(rightExpression)
                        && StringValue.class.isInstance(function.getParameters().getExpressions().get(1))) {
                    String column = function.getParameters().getExpressions().get(0).toString();
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

    /**
     * Build a mongo shell statement with the code to run the specified query.
     * @param outputStream
     * @throws IOException
     */
    public void write(OutputStream outputStream) throws IOException {
        MongoDBQueryHolder mongoDBQueryHolder = getMongoQuery();
        if (!mongoDBQueryHolder.isDistinct()) {
            IOUtils.write("db." + mongoDBQueryHolder.getCollection() + ".find(", outputStream);
            IOUtils.write(prettyPrintJson(mongoDBQueryHolder.getQuery().toJson()), outputStream);
            if (mongoDBQueryHolder.getProjection() != null && mongoDBQueryHolder.getProjection().size() > 0) {
                IOUtils.write(" , ", outputStream);
                IOUtils.write(prettyPrintJson(mongoDBQueryHolder.getProjection().toJson()), outputStream);
            }
            IOUtils.write(")", outputStream);
        } else {
            IOUtils.write("db." + mongoDBQueryHolder.getCollection() + ".distinct(", outputStream);
            IOUtils.write("\""+Iterables.get(mongoDBQueryHolder.getProjection().keySet(),0) + "\"", outputStream);
            IOUtils.write(" , ", outputStream);
            IOUtils.write(prettyPrintJson(mongoDBQueryHolder.getQuery().toJson()), outputStream);
            IOUtils.write(")", outputStream);
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
        private String comparisonExpresion = "$eq";

        private DateFunction(String format,String value, String column) {
            if ("natural".equals(format)) {
                this.date = parse(value);
            } else {
                DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(format).withZoneUTC();
                this.date = dateTimeFormatter.parseDateTime(value).toDate();
            }
            this.column = column;
        }

        private static Date parse(String text) {
            Parser parser = new Parser();
            List<DateGroup> groups = parser.parse(text);
            for (DateGroup group : groups) {
                List<Date> dates = group.getDates();
                if (dates.size() > 0) {
                    return dates.get(0);
                }
            }
            throw new IllegalArgumentException("could not parse natural date: "+ text);
        }

        public Date getDate() {
            return date;
        }

        public String getColumn() {
            return column;
        }

        public void setComparisonFunction(ComparisonOperator comparisonFunction) throws ParseException {
            if (GreaterThanEquals.class.isInstance(comparisonFunction)) {
                this.comparisonExpresion = "$gte";
            } else if (GreaterThan.class.isInstance(comparisonFunction)) {
                this.comparisonExpresion = "$gt";
            } else if (MinorThanEquals.class.isInstance(comparisonFunction)) {
                this.comparisonExpresion = "$lte";
            } else if (MinorThan.class.isInstance(comparisonFunction)) {
                this.comparisonExpresion = "$lt";
            } else {
                throw new ParseException("could not parse string expression: " + comparisonFunction.getStringExpression());
            }
        }

        public String getComparisonExpresion() {
            return comparisonExpresion;
        }
    }
}
