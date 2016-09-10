package com.github.vincentrussell.query.mongodb.sql.converter;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;
import org.bson.Document;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
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

    public QueryConverter(String sql) throws ParseException {
        this(new ByteArrayInputStream(sql.getBytes()));
    }

    public QueryConverter(InputStream inputStream) throws ParseException {
        CCJSqlParser jSqlParser = new CCJSqlParser(inputStream);
        try {
            try {
                final PlainSelect plainSelect = jSqlParser.PlainSelect();
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
        if (selectItems.size()!=filteredItems.size() && !isSelectAll(selectItems)) {
            throw new ParseException("illegal expression(s) found in select clause.  Only column names supported");
        }

        if (joins!=null) {
            throw new ParseException("Joins are not supported.  Only one simple table name is supported.");
        }
    }

    public MongoDBQueryHolder getMongoQuery() {
        return mongoDBQueryHolder;
    }

    private MongoDBQueryHolder getMongoQueryInternal() throws ParseException {
        MongoDBQueryHolder mongoDBQueryHolder = new MongoDBQueryHolder(table);
        if (!isSelectAll(selectItems)) {
            Document document = new Document();
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
            if (isRegexFunction(incomingExpression)!=null) {
                RegexFunction regexFunction = isRegexFunction(incomingExpression);
                Document regexDocument = new Document("$regex", regexFunction.getRegex());
                if (regexFunction.getOptions()!=null) {
                    regexDocument.append("$options",regexFunction.getOptions());
                }
                query.put(regexFunction.getColumn(), regexDocument);
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
            String columnName = incomingExpression.toString();
            Matcher matcher = SURROUNDED_IN_QUOTES.matcher(columnName);
            if (matcher.matches()) {
                return matcher.group(1);
            }
            return columnName;
        } else {
            throw new ParseException("can not parse: " + incomingExpression.toString());
        }
        return query;
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
}
