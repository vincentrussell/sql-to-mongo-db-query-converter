package com.github.vincentrussell.query.mongodb.sql.converter.holder.from;

import com.github.vincentrussell.query.mongodb.sql.converter.FieldType;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.HashMap;
import java.util.Map;

/**
 * Class that holds information about the FROM section of the query.
 */
public class FromHolder {
    private FieldType defaultFieldType;
    private Map<String, FieldType> fieldNameToFieldTypeMapping;

    private FromItem baseFrom;
    private String baseAlias;
    private Map<String, FromItem> aliasToTable = new HashMap<>();
    private Map<FromItem, String> tableToAlias = new HashMap<>();
    private Map<FromItem, SQLInfoHolder> fromToSQLHolder = new HashMap<>();

    /**
     * Default Constructor.
     * @param defaultFieldType
     * @param fieldNameToFieldTypeMapping
     */
    public FromHolder(final FieldType defaultFieldType, final Map<String, FieldType> fieldNameToFieldTypeMapping) {
        this.defaultFieldType = defaultFieldType;
        this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping;
    }

    private void addToSQLHolderMap(final FromItem from)
            throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
        if (from instanceof Table) {
            Table table = (Table) from;
            fromToSQLHolder.put(table, new SQLTableInfoHolder(table.getName()));
        } else if (from instanceof SubSelect) {
            SubSelect subselect = (SubSelect) from;
            fromToSQLHolder.put(from, SQLCommandInfoHolder.Builder
                    .create(defaultFieldType, fieldNameToFieldTypeMapping)
                    .setPlainSelect((PlainSelect) subselect.getSelectBody())
                    .build());
        } else {
            //Not happen SubJoin, not supported previously
            return;
        }
    }

    private void addBaseFrom(final FromItem from)
            throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
        baseFrom = from;
        addToSQLHolderMap(from);
    }

    private void addBaseFrom(final FromItem from, final String alias)
            throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
        addBaseFrom(from);
        if (alias != null) {
            baseAlias = alias;
            aliasToTable.put(alias, from);
        }
        tableToAlias.put(from, alias);
    }

    /**
     * Add information from the From clause of this query.
     * @param from
     * @param alias
     * @throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException
     * @throws ParseException
     */
    public void addFrom(final FromItem from, final String alias)
            throws com.github.vincentrussell.query.mongodb.sql.converter.ParseException, ParseException {
        if (baseFrom != null) {
            if (alias != null) {
                aliasToTable.put(alias, from);
            }
            tableToAlias.put(from, alias);
            addToSQLHolderMap(from);
        } else {
            addBaseFrom(from, alias);
        }
    }

    /**
     * Get the table name from the base from.
     * @return the table name from the base from.
     * @throws ParseException
     */
    public String getBaseFromTableName() throws ParseException {
        return fromToSQLHolder.get(baseFrom).getBaseTableName();
    }

    /**
     * Get the base {@link FromItem}.
     * @return the base {@link FromItem}
     * @throws ParseException
     */
    public FromItem getBaseFrom() throws ParseException {
        return baseFrom;
    }

    /**
     * get the {@link SQLInfoHolder} from base {@link FromItem}.
     * @return the {@link SQLInfoHolder} from base {@link FromItem}
     * @throws ParseException
     */
    public SQLInfoHolder getBaseSQLHolder() throws ParseException {
        return fromToSQLHolder.get(baseFrom);
    }

    /**
     * get the {@link SQLInfoHolder} from the provided {@link FromItem}.
     * @param fromItem
     * @return the {@link SQLInfoHolder} from the provided {@link FromItem}
     * @throws ParseException
     */
    public SQLInfoHolder getSQLHolder(final FromItem fromItem) throws ParseException {
        return fromToSQLHolder.get(fromItem);
    }

    /**
     * get the base alias table.
     * @return the base alias table
     */
    public String getBaseAliasTable() {
        return baseAlias;
    }

}
