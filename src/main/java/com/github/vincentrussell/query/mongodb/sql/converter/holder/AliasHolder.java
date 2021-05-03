package com.github.vincentrussell.query.mongodb.sql.converter.holder;

import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;

import java.util.Collections;
import java.util.Map;

/**
 * The class holds mapping between fields and aliases.
 */
public class AliasHolder {
    private Map<String, String> aliasFromFieldHash;
    private Map<String, String> fieldFromAliasHash;

    /**
     * Default constructor.
     * @param aliasFromFieldMap the alias from the field map.
     * @param fieldFromAliasMap the field to alias map.
     */
    public AliasHolder(final Map<String, String> aliasFromFieldMap,
                       final Map<String, String> fieldFromAliasMap) {
        this.aliasFromFieldHash = aliasFromFieldMap;
        this.fieldFromAliasHash = fieldFromAliasMap;
    }

    /**
     * Constructor with empty values.
     */
    public AliasHolder() {
        this(Collections.<String, String>emptyMap(), Collections.<String, String>emptyMap());
    }

    /**
     * Get the alias from a field name.
     * @param field the field that you want the alias for.
     * @return the alias from a field name.
     */
    public String getAliasFromFieldExp(final String field) {
        return aliasFromFieldHash.get(field);
    }

    /**
     * is there an alias for this field?
     * @param field
     * @return true if there is an alias for this field
     */
    public boolean containsAliasForFieldExp(final String field) {
        return aliasFromFieldHash.containsKey(field);
    }

    /**
     * Will return the field name given an alias.
     * @param fieldOrAlias the field
     * @return the field name given an alias
     * @throws ParseException if the field is an ambiguous field
     */
    public String getFieldFromAliasOrField(final String fieldOrAlias) throws ParseException {
        if (!isAmbiguous(fieldOrAlias)) {
            String field = fieldFromAliasHash.get(fieldOrAlias);
            if (field == null) {
                return fieldOrAlias;
            } else {
                return field;
            }
        } else {
            throw new ParseException("Ambiguous field: " + fieldOrAlias);
        }
    }

    /**
     * Will return true if there are no alias used.
     * @return true if there are no alias used
     */
    public boolean isEmpty() {
        return aliasFromFieldHash.isEmpty();
    }

    private boolean isAmbiguous(final String fieldOrAlias) {
        String aliasFromField = aliasFromFieldHash.get(fieldOrAlias);
        String fieldFromAlias = fieldFromAliasHash.get(fieldOrAlias);
        return aliasFromField != null && fieldFromAlias != null && !aliasFromField.equals(fieldFromAlias);
    }

    /**
     * Combine two alias holders.
     * @param aliasHolder
     */
    public void combine(final AliasHolder aliasHolder) {
        aliasFromFieldHash.putAll(aliasHolder.aliasFromFieldHash);
        fieldFromAliasHash.putAll(aliasHolder.fieldFromAliasHash);
    }
}
