package com.github.vincentrussell.query.mongodb.sql.converter.util;

public class RegexFunction {
    private final String column;
    private final String regex;
    private final boolean isNot;
    private String options;

    public RegexFunction(String column, String regex, boolean isNot) {
        this.column = column;
        this.regex = regex;
        this.isNot = isNot;
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

    public boolean isNot() {
        return isNot;
    }
}