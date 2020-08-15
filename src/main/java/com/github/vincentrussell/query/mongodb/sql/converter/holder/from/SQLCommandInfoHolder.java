package com.github.vincentrussell.query.mongodb.sql.converter.holder.from;

import com.github.vincentrussell.query.mongodb.sql.converter.FieldType;
import com.github.vincentrussell.query.mongodb.sql.converter.SQLCommandType;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.AliasHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import com.github.vincentrussell.query.mongodb.sql.converter.visitor.ExpVisitorEraseAliasTableBaseBuilder;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link SQLInfoHolder} to hold information about the sql query and it's structure.
 */
public final class SQLCommandInfoHolder implements SQLInfoHolder {
    private final SQLCommandType sqlCommandType;
    private final boolean isDistinct;
    private final boolean isCountAll;
    private final boolean isTotalGroup;
    private final FromHolder from;
    private final long limit;
    private final long offset;
    private final Expression whereClause;
    private final List<SelectItem> selectItems;
    private final List<Join> joins;
    private final List<String> groupBys;
    private final List<OrderByElement> orderByElements;
    private final AliasHolder aliasHolder;
    private final Expression havingClause;


    private SQLCommandInfoHolder(final Builder builder) {
        this.sqlCommandType = builder.sqlCommandType;
        this.whereClause = builder.whereClause;
        this.isDistinct = builder.isDistinct;
        this.isCountAll = builder.isCountAll;
        this.isTotalGroup = builder.isTotalGroup;
        this.from = builder.from;
        this.limit = builder.limit;
        this.offset = builder.offset;
        this.selectItems = builder.selectItems;
        this.joins = builder.joins;
        this.groupBys = builder.groupBys;
        this.havingClause = builder.havingClause;
        this.orderByElements = builder.orderByElements;
        this.aliasHolder = builder.aliasHolder;
    }

    @Override
    public String getBaseTableName() {
        return from.getBaseFromTableName();
    }

    /**
     * get if distinct was used in the sql query.
     * @return true if distinct
     */
    public boolean isDistinct() {
        return isDistinct;
    }

    /**
     * true if count(*) is used.
     * @return true if count(*) is used
     */
    public boolean isCountAll() {
        return isCountAll;
    }

    /**
     * Will return true if any of the {@link SelectItem}s has a function that justifies aggregation like max().
     * @return true if any of the {@link SelectItem}s has a function that justifies aggregation like max()
     */
    public boolean isTotalGroup() {
        return isTotalGroup;
    }

    /**
     * get the base table name from this query.
     * @return the base table name
     */
    public String getTable() {
        return from.getBaseFromTableName();
    }

    /**
     * get the {@link FromHolder} that holds information about the from information in the query.
     * @return the {@link FromHolder}
     */
    public FromHolder getFromHolder() {
        return this.from;
    }

    /**
     * get the limit used in the sql query.
     * @return the limit
     */
    public long getLimit() {
        return limit;
    }

    /**
     * get the offset from the sql query.
     * @return the offset from the sql query
     */
    public long getOffset() {
        return offset;
    }

    /**
     * get the where clause from the query.
     * @return the where clause
     */
    public Expression getWhereClause() {
        return whereClause;
    }

    /**
     * get the select items from the query.
     * @return the select items from the query
     */
    public List<SelectItem> getSelectItems() {
        return selectItems;
    }

    /**
     * get the joins from the query.
     * @return the joins from the query
     */
    public List<Join> getJoins() {
        return joins;
    }

    /**
     * get the groupbys from the query.
     * @return the groupbys
     */
    public List<String> getGroupBys() {
        return groupBys;
    }

    /**
     * get the having clause from the sql query.
     * @return the having clause from the sql query
     */
    public Expression getHavingClause() {
        return havingClause;
    }

    /**
     * get the order by elements from the query.
     * @return the order by elements
     */
    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    /**
     * Get the {@link SQLCommandType} for this query.
     * @return the {@link SQLCommandType}
     */
    public SQLCommandType getSqlCommandType() {
        return sqlCommandType;
    }

    /**
     * Get the {@link AliasHolder} for this query.
     * @return the alias holder
     */
    public AliasHolder getAliasHolder() {
        return aliasHolder;
    }

    /**
     * Builder for {@link SQLCommandInfoHolder}.
     */
    public static final class Builder {
        private final FieldType defaultFieldType;
        private final Map<String, FieldType> fieldNameToFieldTypeMapping;
        private SQLCommandType sqlCommandType;
        private Expression whereClause;
        private boolean isDistinct = false;
        private boolean isCountAll = false;
        private boolean isTotalGroup = false;
        private FromHolder from;
        private long limit = -1;
        private long offset = -1;
        private List<SelectItem> selectItems = new ArrayList<>();
        private List<Join> joins = new ArrayList<>();
        private List<String> groupBys = new ArrayList<>();
        private Expression havingClause;
        private List<OrderByElement> orderByElements = new ArrayList<>();
        private AliasHolder aliasHolder;

        private Builder(final FieldType defaultFieldType, final Map<String, FieldType> fieldNameToFieldTypeMapping) {
            this.defaultFieldType = defaultFieldType;
            this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping;
        }

        private FromHolder generateFromHolder(final FromHolder tholder,
                                              final FromItem fromItem, final List<Join> ljoin)
                throws ParseException, com.github.vincentrussell.query.mongodb.sql.converter.ParseException {

            FromHolder returnValue = tholder;
            Alias alias = fromItem.getAlias();
            returnValue.addFrom(fromItem, (alias != null ? alias.getName() : null));

            if (ljoin != null) {
                for (Join j : ljoin) {
                    SqlUtils.updateJoinType(j);
                    if (j.isInner() || j.isLeft()) {
                        returnValue = generateFromHolder(returnValue, j.getRightItem(), null);
                    } else {
                        throw new ParseException("Join type not supported");
                    }
                }
            }
            return returnValue;
        }

        /**
         * Set the select or delete statement from the parsed sql string.
         * @param statement the {@link Statement}
         * @return the builder
         * @throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException if there is an issue
         * the parsing the sql
         * @throws ParseException if there is an issue the parsing the sql
         */
        public Builder setStatement(final Statement statement)
                throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {

            if (Select.class.isAssignableFrom(statement.getClass())) {
                sqlCommandType = SQLCommandType.SELECT;
                SelectBody selectBody = ((Select) statement).getSelectBody();

                if (SetOperationList.class.isInstance(selectBody)) {
                    SetOperationList setOperationList = (SetOperationList) selectBody;
                    if (setOperationList.getSelects() != null
                            && setOperationList.getSelects().size() == 1
                            && PlainSelect.class.isInstance(setOperationList.getSelects().get(0))) {
                        return setPlainSelect((PlainSelect) setOperationList.getSelects().get(0));
                    }
                } else if (PlainSelect.class.isInstance(selectBody)) {
                    return setPlainSelect((PlainSelect) selectBody);
                }

                throw new ParseException("No supported sentence");


            } else if (Delete.class.isAssignableFrom(statement.getClass())) {
                sqlCommandType = SQLCommandType.DELETE;
                Delete delete = (Delete) statement;
                return setDelete(delete);
            } else {
                throw new ParseException("No supported sentence");
            }
        }

        /**
         * Set the select query information for this query.
         * @param plainSelect the {@link PlainSelect}
         * @return the builder
         * @throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException if there is an issue
         * the parsing the sql
         * @throws ParseException if there is an issue the parsing the sql
         */
        public Builder setPlainSelect(final PlainSelect plainSelect)
                throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
            SqlUtils.isTrue(plainSelect != null,
                    "could not parseNaturalLanguageDate SELECT statement from query");
            SqlUtils.isTrue(plainSelect.getFromItem() != null,
                    "could not find table to query.  Only one simple table name is supported.");
            whereClause = plainSelect.getWhere();
            isDistinct = (plainSelect.getDistinct() != null);
            isCountAll = SqlUtils.isCountAll(plainSelect.getSelectItems());
            SqlUtils.isTrue(plainSelect.getFromItem() != null,
                    "could not find table to query.  Only one simple table name is supported.");
            from = generateFromHolder(new FromHolder(this.defaultFieldType,
                    this.fieldNameToFieldTypeMapping), plainSelect.getFromItem(), plainSelect.getJoins());
            limit = SqlUtils.getLimitAsLong(plainSelect.getLimit());
            offset = SqlUtils.getOffsetAsLong(plainSelect.getOffset());
            orderByElements = plainSelect.getOrderByElements();
            selectItems = plainSelect.getSelectItems();
            joins = plainSelect.getJoins();
            groupBys = SqlUtils.getGroupByColumnReferences(plainSelect);
            havingClause = plainSelect.getHaving();
            aliasHolder = generateHashAliasFromSelectItems(selectItems);
            isTotalGroup = SqlUtils.isTotalGroup(selectItems);
            SqlUtils.isTrue(plainSelect.getFromItem() != null,
                    "could not find table to query.  Only one simple table name is supported.");
            return this;
        }

        /**
         * Set the delete information for this query if it is a delete query.
         * @param delete the {@link Delete} object
         * @return the builder
         * @throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException if there is an issue
         * parsing the sql
         * @throws ParseException if there is an issue parsing the sql
         */
        public Builder setDelete(final Delete delete)
                throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
            SqlUtils.isTrue(delete.getTables().size() == 0,
                    "there should only be on table specified for deletes");
            from = generateFromHolder(new FromHolder(this.defaultFieldType,
                    this.fieldNameToFieldTypeMapping), delete.getTable(), null);
            whereClause = delete.getWhere();
            return this;
        }

        private AliasHolder generateHashAliasFromSelectItems(final List<SelectItem> selectItems) {
            HashMap<String, String> aliasFromFieldHash = new HashMap<>();
            HashMap<String, String> fieldFromAliasHash = new HashMap<>();
            for (SelectItem sitem : selectItems) {
                if (!(sitem instanceof AllColumns)) {
                    if (sitem instanceof SelectExpressionItem) {
                        SelectExpressionItem seitem = (SelectExpressionItem) sitem;
                        if (seitem.getAlias() != null) {
                            Expression selectExp = seitem.getExpression();
                            selectExp.accept(new ExpVisitorEraseAliasTableBaseBuilder(
                                    this.from.getBaseAliasTable()));
                            String expStr = selectExp.toString();
                            String aliasStr = seitem.getAlias().getName();
                            aliasFromFieldHash.put(expStr, aliasStr);
                            fieldFromAliasHash.put(aliasStr, expStr);
                        }
                    }
                }
            }
            return new AliasHolder(aliasFromFieldHash, fieldFromAliasHash);
        }

        /**
         * Build a {@link SQLCommandInfoHolder}.
         * @return the {@link SQLCommandInfoHolder}
         */
        public SQLCommandInfoHolder build() {
            return new SQLCommandInfoHolder(this);
        }

        /**
         * create a {@link Builder}.
         * @param defaultFieldType the default {@link FieldType}
         * @param fieldNameToFieldTypeMapping the field name to {@link FieldType} map
         * @return the builder
         */
        public static Builder create(final FieldType defaultFieldType,
                                     final Map<String, FieldType> fieldNameToFieldTypeMapping) {
            return new Builder(defaultFieldType, fieldNameToFieldTypeMapping);
        }
    }
}
