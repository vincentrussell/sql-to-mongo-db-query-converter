package com.github.vincentrussell.query.mongodb.sql.converter;

import java.io.Serializable;

public class Token implements Serializable {
    private static final long serialVersionUID = 1L;
    private int kind;
    private int beginLine;
    private int beginColumn;
    private int endLine;
    private int endColumn;
    private String image;
    private Token next;
    private Token specialToken;

    public Object getValue() {
        return null;
    }

    public Token() {
    }

    public Token(int kind) {
        this(kind, (String)null);
    }

    public Token(int kind, String image) {
        this.kind = kind;
        this.image = image;
    }

    public String toString() {
        return this.image;
    }

    public static Token newToken(int ofKind, String image) {
        switch(ofKind) {
            default:
                return new Token(ofKind, image);
        }
    }

    public int getKind() {
        return kind;
    }

    public int getBeginLine() {
        return beginLine;
    }

    public int getBeginColumn() {
        return beginColumn;
    }

    public int getEndLine() {
        return endLine;
    }

    public int getEndColumn() {
        return endColumn;
    }

    public String getImage() {
        return image;
    }

    public Token getNext() {
        return next;
    }

    public Token getSpecialToken() {
        return specialToken;
    }

    public static Token newToken(int ofKind) {
        return newToken(ofKind, (String)null);
    }
}
