package com.github.vincentrussell.query.mongodb.sql.converter;

import com.github.vincentrussell.query.mongodb.sql.converter.holder.TablesHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLCommandInfoHolder {
    private final SQLCommandType sqlCommandType;
    private final boolean isDistinct;
    private final boolean isCountAll;
    private final TablesHolder tables;
    private final long limit;
    private final long offset;
    private final Expression whereClause;
    private final List<SelectItem> selectItems;
    private final List<Join> joins;
    private final List<String> groupBys;
    private final List<OrderByElement> orderByElements;
    private final HashMap<String,String> aliasHash;

    public SQLCommandInfoHolder(SQLCommandType sqlCommandType, Expression whereClause, boolean isDistinct, boolean isCountAll, TablesHolder tables, long limit, long offset, List<SelectItem> selectItems, List<Join> joins, List<String> groupBys, List<OrderByElement> orderByElements, HashMap<String,String> aliasHash) {
        this.sqlCommandType = sqlCommandType;
        this.whereClause = whereClause;
        this.isDistinct = isDistinct;
        this.isCountAll = isCountAll;
        this.tables = tables;
        this.limit = limit;
        this.offset = offset;
        this.selectItems = selectItems;
        this.joins = joins;
        this.groupBys = groupBys;
        this.orderByElements = orderByElements;
        this.aliasHash = aliasHash;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public boolean isCountAll() {
        return isCountAll;
    }

    public String getTable() {
        return this.tables.getBaseTable();
    }
    
    public TablesHolder getTablesHolder() {
        return this.tables;
    }
    
    public String getAliasTable() {
    	return this.tables.getAlias(this.tables.getBaseTable());
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

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public SQLCommandType getSqlCommandType() {
        return sqlCommandType;
    }

    public HashMap<String,String> getAliasHash() {
		return aliasHash;
	}

	public static class Builder {
        private final FieldType defaultFieldType;
        private final Map<String, FieldType> fieldNameToFieldTypeMapping;
        private SQLCommandType sqlCommandType;
        private Expression whereClause;
        private boolean isDistinct = false;
        private boolean isCountAll = false;
        private TablesHolder tables;
        private long limit = -1;
        private long offset = -1;
        private List<SelectItem> selectItems = new ArrayList<>();
        private List<Join> joins = new ArrayList<>();
        private List<String> groupBys = new ArrayList<>();
        private List<OrderByElement> orderByElements1 = new ArrayList<>();
        private HashMap<String,String> aliasHash;

        private Builder(FieldType defaultFieldType, Map<String, FieldType> fieldNameToFieldTypeMapping){
            this.defaultFieldType = defaultFieldType;
            this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping;
        }
        
        private TablesHolder generateTableHolder(String baseTable) {
        	TablesHolder tholder = new TablesHolder();
        	tholder.addTable(baseTable,null);
        	return tholder;
        }
        
        private TablesHolder generateTableHolder(TablesHolder tholder, FromItem fromItem, List<Join> ljoin) throws ParseException {
        	if (fromItem instanceof Table) {
        		Table t = (Table)fromItem;
        		Alias alias = t.getAlias();
        		tholder.addTable(t.getName(),(alias != null ? t.getAlias().getName() : null));
        	}
        	else if(fromItem instanceof SubSelect){
        		throw new ParseException("Subselect not supported");
        	}
        	else {
        		throw new ParseException("SubJoin not supported");
        	}
        	
        	if(ljoin != null) {
	        	for (Join j : ljoin) {
	        		if(j.isInner()) {
	        			tholder = generateTableHolder(tholder,j.getRightItem(),null);
	        		}	
	        		else{
	        			throw new ParseException("Join type not suported");
	        		}
	        	}
        	}
        	return tholder;
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
                tables = generateTableHolder(new TablesHolder(),plainSelect.getFromItem(),plainSelect.getJoins());
                limit = SqlUtils.getLimit(plainSelect.getLimit());
                offset = SqlUtils.getOffset(plainSelect.getOffset());
                orderByElements1 = plainSelect.getOrderByElements();
                selectItems = plainSelect.getSelectItems();
                joins = plainSelect.getJoins();
                groupBys = SqlUtils.getGroupByColumnReferences(plainSelect);
                aliasHash = generateHashAliasFromSelectItems(selectItems);
                SqlUtils.isTrue(plainSelect.getFromItem() != null, "could not find table to query.  Only one simple table name is supported.");
            } else if (Delete.class.isAssignableFrom(statement.getClass())) {
                sqlCommandType = SQLCommandType.DELETE;
                Delete delete = (Delete)statement;
                SqlUtils.isTrue(delete.getTables().size() == 0, "there should only be on table specified for deletes");
                tables = generateTableHolder(delete.getTable().toString());
                whereClause = delete.getWhere();
            }
            return this;
        }
        
        private HashMap<String,String> generateHashAliasFromSelectItems(List<SelectItem> selectItems) {
        	HashMap<String,String> aliasHashAux = new HashMap<String,String>();
        	for(SelectItem sitem: selectItems) {
        		if(!(sitem instanceof AllColumns)) {
        			SelectExpressionItem seitem = (SelectExpressionItem) sitem;
        			if(seitem.getAlias() != null) {
		        		aliasHashAux.put( seitem.getExpression().toString(), seitem.getAlias().getName());
        			}
        		}
        	}
        	return aliasHashAux;
        }

        public SQLCommandInfoHolder build() {
            return new SQLCommandInfoHolder(sqlCommandType, whereClause,
                    isDistinct, isCountAll, tables, limit, offset, selectItems, joins, groupBys, orderByElements1, aliasHash);
        }

        public static Builder create(FieldType defaultFieldType, Map<String, FieldType> fieldNameToFieldTypeMapping) {
            return new Builder(defaultFieldType, fieldNameToFieldTypeMapping);
        }
    }
}
