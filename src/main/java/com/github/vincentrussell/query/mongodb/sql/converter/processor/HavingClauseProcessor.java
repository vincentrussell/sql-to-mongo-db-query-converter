package com.github.vincentrussell.query.mongodb.sql.converter.processor;

import com.github.vincentrussell.query.mongodb.sql.converter.FieldType;
import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.AliasHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import net.sf.jsqlparser.expression.Function;
import org.bson.Document;

import java.util.Map;

/**
 * Processor for handling sql having clause.
 */
public class HavingClauseProcessor extends WhereClauseProcessor {

    private AliasHolder aliasHolder;

    /**
     * Default Constructor.
     * @param defaultFieldType the default {@link FieldType}
     * @param fieldNameToFieldTypeMapping the field name to {@link FieldType} mapping
     * @param aliasHolder the {@link AliasHolder}
     * @param requiresAggregation true if this sql statement requires aggregation
     */
    public HavingClauseProcessor(final FieldType defaultFieldType,
                                 final Map<String, FieldType> fieldNameToFieldTypeMapping,
                                 final AliasHolder aliasHolder, final boolean requiresAggregation) {
        super(defaultFieldType, fieldNameToFieldTypeMapping, requiresAggregation);
        this.aliasHolder = aliasHolder;
    }

    /**
     * Recurse through functions in the sql structure to generate mongo query structure.
     * @param query the query object
     * @param object the value that needs to be parsed
     * @param defaultFieldType the default {@link FieldType}
     * @param fieldNameToFieldTypeMapping the field name to {@link FieldType}
     * @return the mongo structure
     * @throws ParseException if the value cannot be parsed
     */
    @Override //for use alias because it's after group expression so alias is already applied
    protected Object recurseFunctions(final Document query,
                                      final Object object,
                                      final FieldType defaultFieldType,
                                      final Map<String, FieldType> fieldNameToFieldTypeMapping)
            throws ParseException {
        if (Function.class.isInstance(object)) {
            Function function = (Function) object;
            String strFunction = function.toString();
            if (SqlUtils.isAggregateExpression(strFunction)) {
                String alias = aliasHolder.getAliasFromFieldExp(function.toString());
                return "$" + SqlUtils.generateAggField(function, alias);
            }
        }
        return super.recurseFunctions(query, object, defaultFieldType, fieldNameToFieldTypeMapping);
    }

}
