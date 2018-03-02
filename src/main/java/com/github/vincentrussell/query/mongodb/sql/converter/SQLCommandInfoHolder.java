package com.github.vincentrussell.query.mongodb.sql.converter;

import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SQLCommandInfoHolder {
    private final SQLCommandType sqlCommandType;
    private final boolean isDistinct;
    private final boolean isCountAll;
    private final String table;
    private final long limit;
    private final Expression whereClause;
    private final List<SelectItem> selectItems;
    private final List<Join> joins;
    private final List<String> groupBys;
    private final List<OrderByElement> orderByElements;

    public SQLCommandInfoHolder(SQLCommandType sqlCommandType, Expression whereClause,
                                boolean isDistinct, boolean isCountAll, String table, long limit, List<SelectItem> selectItems, List<Join> joins, List<String> groupBys, List<OrderByElement> orderByElements) {
        this.sqlCommandType = sqlCommandType;
        this.whereClause = whereClause;
        this.isDistinct = isDistinct;
        this.isCountAll = isCountAll;
        this.table = table;
        this.limit = limit;
        this.selectItems = selectItems;
        this.joins = joins;
        this.groupBys = groupBys;
        this.orderByElements = orderByElements;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public boolean isCountAll() {
        return isCountAll;
    }

    public String getTable() {
        return table;
    }

    public long getLimit() {
        return limit;
    }

    public Expression getWhereClause() {
        return whereClause;
    }

    public List<SelectItem> getSelectItems() {
        return selectItems;
    }

    public List<Join> getJoins() {
        return joins;
    }

    public List<String> getGoupBys() {
        return groupBys;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public SQLCommandType getSqlCommandType() {
        return sqlCommandType;
    }

    public static class Builder {
        private final FieldType defaultFieldType;
        private final Map<String, FieldType> fieldNameToFieldTypeMapping;
        private SQLCommandType sqlCommandType;
        private Expression whereClause;
        private boolean isDistinct = false;
        private boolean isCountAll = false;
        private String table;
        private long limit = -1;
        private List<SelectItem> selectItems = new ArrayList<>();
        private List<Join> joins = new ArrayList<>();
        private List<String> groupBys = new ArrayList<>();
        private List<OrderByElement> orderByElements1 = new ArrayList<>();


        private Builder(FieldType defaultFieldType, Map<String, FieldType> fieldNameToFieldTypeMapping){
            this.defaultFieldType = defaultFieldType;
            this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping;
        }

        public Builder setJSqlParser(CCJSqlParser jSqlParser) throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
            final Statement statement = jSqlParser.Statement();
            if (Select.class.isAssignableFrom(statement.getClass())) {
                sqlCommandType = SQLCommandType.SELECT;
                final PlainSelect plainSelect = (PlainSelect)(((Select)statement).getSelectBody());
                SqlUtils.isTrue(plainSelect != null, "could not parseNaturalLanguageDate SELECT statement from query");
                SqlUtils.isTrue(plainSelect.getFromItem()!=null,"could not find table to query.  Only one simple table name is supported.");
                whereClause = plainSelect.getWhere();
                isDistinct = (plainSelect.getDistinct() != null);
                isCountAll = SqlUtils.isCountAll(plainSelect.getSelectItems());
                SqlUtils.isTrue(plainSelect.getFromItem() != null, "could not find table to query.  Only one simple table name is supported.");
                table = plainSelect.getFromItem().toString();
                limit = SqlUtils.getLimit(plainSelect.getLimit());
                orderByElements1 = plainSelect.getOrderByElements();
                selectItems = plainSelect.getSelectItems();
                joins = plainSelect.getJoins();
                groupBys = SqlUtils.getGroupByColumnReferences(plainSelect);
                SqlUtils.isTrue(plainSelect.getFromItem() != null, "could not find table to query.  Only one simple table name is supported.");
            } else if (Delete.class.isAssignableFrom(statement.getClass())) {
                sqlCommandType = SQLCommandType.DELETE;
                Delete delete = (Delete)statement;
                SqlUtils.isTrue(delete.getTables().size() == 0, "there should only be on table specified for deletes");
                table = delete.getTable().toString();
                whereClause = delete.getWhere();
            }
            return this;
        }

        public SQLCommandInfoHolder build() {
            return new SQLCommandInfoHolder(sqlCommandType, whereClause,
                    isDistinct, isCountAll, table, limit, selectItems, joins, groupBys, orderByElements1);
        }

        public static Builder create(FieldType defaultFieldType, Map<String, FieldType> fieldNameToFieldTypeMapping) {
            return new Builder(defaultFieldType, fieldNameToFieldTypeMapping);
        }
    }
}
