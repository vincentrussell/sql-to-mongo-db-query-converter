package com.github.vincentrussell.query.mongodb.sql.converter.util;

/**
 * Object that serves as a wrapper around a regex.
 */
public class RegexFunction {
    private final String column;
    private final String regex;
    private final boolean isNot;
    private String options;

    /**
     * Default Constructor.
     * @param column
     * @param regex
     * @param isNot
     */
    public RegexFunction(final String column, final String regex, final boolean isNot) {
        this.column = column;
        this.regex = regex;
        this.isNot = isNot;
    }

    /**
     * Returns the column that this regex is run on.
     * @return the column.
     */
    public String getColumn() {
        return column;
    }

    /**
     * Returns a string representation of the regex.
     * @return the regex.
     */
    public String getRegex() {
        return regex;
    }

    /**
     * Sets any mongo-specific options that are available for the regex.
     * @param options
     */
    public void setOptions(final String options) {
        this.options = options;
    }

    /**
     * Returns any mongo-specific options that are available for the regex.
     * @return the options
     */
    public String getOptions() {
        return options;
    }

    /**
     * Returns if this is a "not" regex.
     * @return true if not
     */
    public boolean isNot() {
        return isNot;
    }
}
