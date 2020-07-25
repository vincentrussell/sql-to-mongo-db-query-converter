package com.github.vincentrussell.query.mongodb.sql.converter.holder.from;

import com.github.vincentrussell.query.mongodb.sql.converter.FieldType;
import com.github.vincentrussell.query.mongodb.sql.converter.SQLCommandType;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.AliasHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import com.github.vincentrussell.query.mongodb.sql.converter.visitor.ExpVisitorEraseAliasTableBaseBuilder;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLCommandInfoHolder implements SQLInfoHolder{
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

    public SQLCommandInfoHolder(SQLCommandType sqlCommandType, Expression whereClause, boolean isDistinct, boolean isCountAll, boolean isTotalGroup, FromHolder from, long limit, long offset, List<SelectItem> selectItems, List<Join> joins, List<String> groupBys, List<OrderByElement> orderByElements, AliasHolder aliasHolder, Expression havingClause) {
        this.sqlCommandType = sqlCommandType;
        this.whereClause = whereClause;
        this.isDistinct = isDistinct;
        this.isCountAll = isCountAll;
        this.isTotalGroup = isTotalGroup;
        this.from = from;
        this.limit = limit;
        this.offset = offset;
        this.selectItems = selectItems;
        this.joins = joins;
        this.groupBys = groupBys;
        this.havingClause = havingClause;
        this.orderByElements = orderByElements;
        this.aliasHolder = aliasHolder;
    }
    
    @Override
    public String getBaseTableName() throws ParseException {
    	return from.getBaseFromTableName();
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public boolean isCountAll() {
        return isCountAll;
    }
    
    public boolean isTotalGroup() {
		return isTotalGroup;
	}

    public String getTable() throws ParseException {
        return from.getBaseFromTableName();
    }
    
    public FromHolder getFromHolder() {
        return this.from;
    }

    public long getLimit() {
        return limit;
    }
    
    public long getOffset() {
        return offset;
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
    
    public Expression getHavingClause() {
		return havingClause;
	}

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public SQLCommandType getSqlCommandType() {
        return sqlCommandType;
    }

    public AliasHolder getAliasHolder() {
		return aliasHolder;
	}

	public static class Builder {
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
        private List<OrderByElement> orderByElements1 = new ArrayList<>();
        private AliasHolder aliasHolder;

        private Builder(FieldType defaultFieldType, Map<String, FieldType> fieldNameToFieldTypeMapping){
            this.defaultFieldType = defaultFieldType;
            this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping;
        }
        
        private FromHolder generateFromHolder(FromHolder tholder, FromItem fromItem, List<Join> ljoin) throws ParseException, com.github.vincentrussell.query.mongodb.sql.converter.ParseException {
        	Alias alias = fromItem.getAlias();
        	tholder.addFrom(fromItem,(alias != null ? alias.getName() : null));
        	
        	if(ljoin != null) {
	        	for (Join j : ljoin) {
	        		SqlUtils.updateJoinType(j);
	        		if(j.isInner() || j.isLeft()) {
	        			tholder = generateFromHolder(tholder,j.getRightItem(),null);
	        		}	
	        		else{
	        			throw new ParseException("Join type not supported");
	        		}
	        	}
        	}
        	return tholder;
        }
        
        public Builder setJSqlParser(CCJSqlParser jSqlParser) throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
        	final Statement statement = jSqlParser.Statement();
        	return setStatement(statement);
        }

        public Builder setStatement(Statement statement) throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
            
            if (Select.class.isAssignableFrom(statement.getClass())) {
                sqlCommandType = SQLCommandType.SELECT;
                SelectBody selectBody = ((Select) statement).getSelectBody();

                if (SetOperationList.class.isInstance(selectBody)) {
                    SetOperationList setOperationList = (SetOperationList)selectBody;
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
                Delete delete = (Delete)statement;
                return setDelete(delete); 
            } else {
            	throw new ParseException("No supported sentence");
            }
        }
        
        public Builder setPlainSelect(PlainSelect plainSelect) throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
        	SqlUtils.isTrue(plainSelect != null, "could not parseNaturalLanguageDate SELECT statement from query");
            SqlUtils.isTrue(plainSelect.getFromItem()!=null,"could not find table to query.  Only one simple table name is supported.");
            whereClause = plainSelect.getWhere();
            isDistinct = (plainSelect.getDistinct() != null);
            isCountAll = SqlUtils.isCountAll(plainSelect.getSelectItems());
            SqlUtils.isTrue(plainSelect.getFromItem() != null, "could not find table to query.  Only one simple table name is supported.");
            from = generateFromHolder(new FromHolder(this.defaultFieldType,this.fieldNameToFieldTypeMapping),plainSelect.getFromItem(),plainSelect.getJoins());
            limit = SqlUtils.getLimit(plainSelect.getLimit());
            offset = SqlUtils.getOffset(plainSelect.getOffset());
            orderByElements1 = plainSelect.getOrderByElements();
            selectItems = plainSelect.getSelectItems();
            joins = plainSelect.getJoins();
            groupBys = SqlUtils.getGroupByColumnReferences(plainSelect);
            havingClause = plainSelect.getHaving();
            aliasHolder = generateHashAliasFromSelectItems(selectItems);
            isTotalGroup = SqlUtils.isTotalGroup(selectItems);
            SqlUtils.isTrue(plainSelect.getFromItem() != null, "could not find table to query.  Only one simple table name is supported.");
            return this;
        }
        
        public Builder setDelete(Delete delete) throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
        	SqlUtils.isTrue(delete.getTables().size() == 0, "there should only be on table specified for deletes");
            from = generateFromHolder(new FromHolder(this.defaultFieldType,this.fieldNameToFieldTypeMapping),delete.getTable(),null);
            whereClause = delete.getWhere();
            return this;
        }
        
        private AliasHolder generateHashAliasFromSelectItems(List<SelectItem> selectItems) {
        	HashMap<String,String> aliasFromFieldHash = new HashMap<String,String>();
        	HashMap<String,String> fieldFromAliasHash = new HashMap<String,String>();
        	for(SelectItem sitem: selectItems) {
        		if(!(sitem instanceof AllColumns)) {
        			if(sitem instanceof SelectExpressionItem) {
	        			SelectExpressionItem seitem = (SelectExpressionItem) sitem;
	        			if(seitem.getAlias() != null) {
		        			Expression selectExp = seitem.getExpression();
		        			selectExp.accept(new ExpVisitorEraseAliasTableBaseBuilder(this.from.getBaseAliasTable()));
		        			String expStr = selectExp.toString();
		        			String aliasStr = seitem.getAlias().getName();
	        				aliasFromFieldHash.put( expStr, aliasStr);
	        				fieldFromAliasHash.put( aliasStr, expStr);
	        			}
        			}
        		}
        	}
        	return new AliasHolder(aliasFromFieldHash, fieldFromAliasHash);
        }

        public SQLCommandInfoHolder build() {
            return new SQLCommandInfoHolder(sqlCommandType, whereClause,
                    isDistinct, isCountAll, isTotalGroup, from, limit, offset, selectItems, joins, groupBys, orderByElements1, aliasHolder, havingClause);
        }

        public static Builder create(FieldType defaultFieldType, Map<String, FieldType> fieldNameToFieldTypeMapping) {
            return new Builder(defaultFieldType, fieldNameToFieldTypeMapping);
        }
    }
}
