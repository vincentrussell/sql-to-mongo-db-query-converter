package com.github.vincentrussell.query.mongodb.sql.converter;

public class ParseException extends Exception {
    private static final long serialVersionUID = 1L;
    protected static String EOL = System.getProperty("line.separator", "\n");
    public Token currentToken;
    public int[][] expectedTokenSequences;
    public String[] tokenImage;

    public ParseException(Token currentTokenVal, int[][] expectedTokenSequencesVal, String[] tokenImageVal) {
        super(initialise(currentTokenVal, expectedTokenSequencesVal, tokenImageVal));
        this.currentToken = currentTokenVal;
        this.expectedTokenSequences = expectedTokenSequencesVal;
        this.tokenImage = tokenImageVal;
    }

    public ParseException() {
    }

    public ParseException(String message) {
        super(message);
    }

    private static String initialise(Token currentToken, int[][] expectedTokenSequences, String[] tokenImage) {
        StringBuffer expected = new StringBuffer();
        int maxSize = 0;

        for(int retval = 0; retval < expectedTokenSequences.length; ++retval) {
            if(maxSize < expectedTokenSequences[retval].length) {
                maxSize = expectedTokenSequences[retval].length;
            }

            for(int tok = 0; tok < expectedTokenSequences[retval].length; ++tok) {
                expected.append(tokenImage[expectedTokenSequences[retval][tok]]).append(' ');
            }

            if(expectedTokenSequences[retval][expectedTokenSequences[retval].length - 1] != 0) {
                expected.append("...");
            }

            expected.append(EOL).append("    ");
        }

        String var8 = "Encountered \"";
        Token var9 = currentToken.next;

        for(int i = 0; i < maxSize; ++i) {
            if(i != 0) {
                var8 = var8 + " ";
            }

            if(var9.kind == 0) {
                var8 = var8 + tokenImage[0];
                break;
            }

            var8 = var8 + " " + tokenImage[var9.kind];
            var8 = var8 + " \"";
            var8 = var8 + add_escapes(var9.image);
            var8 = var8 + " \"";
            var9 = var9.next;
        }

        var8 = var8 + "\" at line " + currentToken.next.beginLine + ", column " + currentToken.next.beginColumn;
        var8 = var8 + "." + EOL;
        if(expectedTokenSequences.length != 0) {
            if(expectedTokenSequences.length == 1) {
                var8 = var8 + "Was expecting:" + EOL + "    ";
            } else {
                var8 = var8 + "Was expecting one of:" + EOL + "    ";
            }

            var8 = var8 + expected.toString();
        }

        return var8;
    }

    static String add_escapes(String str) {
        StringBuffer retval = new StringBuffer();

        for(int i = 0; i < str.length(); ++i) {
            switch(str.charAt(i)) {
                case '\b':
                    retval.append("\\b");
                    break;
                case '\t':
                    retval.append("\\t");
                    break;
                case '\n':
                    retval.append("\\n");
                    break;
                case '\f':
                    retval.append("\\f");
                    break;
                case '\r':
                    retval.append("\\r");
                    break;
                case '\"':
                    retval.append("\\\"");
                    break;
                case '\'':
                    retval.append("\\\'");
                    break;
                case '\\':
                    retval.append("\\\\");
                    break;
                default:
                    char ch;
                    if((ch = str.charAt(i)) >= 32 && ch <= 126) {
                        retval.append(ch);
                    } else {
                        String s = "0000" + Integer.toString(ch, 16);
                        retval.append("\\u" + s.substring(s.length() - 4, s.length()));
                    }
            }
        }

        return retval.toString();
    }
}
