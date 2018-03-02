package com.github.vincentrussell.query.mongodb.sql.converter.util;

public class RegexFunction {
    private final String column;
    private final String regex;
    private String options;

    public RegexFunction(String column, String regex) {
        this.column = column;
        this.regex = regex;
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
}