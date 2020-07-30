package com.github.vincentrussell.query.mongodb.sql.converter.holder;

import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;

import java.util.HashMap;

/**
 * The class holds mapping between fields and aliases.
 */
public class AliasHolder {
    private HashMap<String, String> aliasFromFieldHash;
    private HashMap<String, String> fieldFromAliasHash;

    /**
     * Default constructor.
     * @param aliasFromFieldHash
     * @param fieldFromAliasHash
     */
    public AliasHolder(final HashMap<String, String> aliasFromFieldHash,
                       final HashMap<String, String> fieldFromAliasHash) {
        this.aliasFromFieldHash = aliasFromFieldHash;
        this.fieldFromAliasHash = fieldFromAliasHash;
    }

    /**
     * Get the alias from a field name.
     * @param field
     * @return the alias from a field name.
     */
    public String getAliasFromFieldExp(final String field) {
        return aliasFromFieldHash.get(field);
    }

    /**
     * Will return the field name given an alias.
     * @param fieldOrAlias
     * @return the field name given an alias
     * @throws ParseException
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

}
