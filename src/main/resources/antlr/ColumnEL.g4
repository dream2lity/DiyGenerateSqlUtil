grammar ColumnEL;

@header {package com.sunlands.bi.dataset.sql.function; }

expr
    : expression EOF
    ;

expression
    :   NumberConstant                                                                  # number
    |   BooleanConstant                                                                 # boolean
    |   StringLiteral                                                                   # string
    |   Identifier                                                                      # identifier
    |   expression op=(Equal|NotEqual|Less|Greater|LessEqual|GreaterEqual) expression   # booleanExpression
    |   expression op=(MUL|DIV) expression                                              # multiplicativeExpression
    |   expression op=(ADD|SUB) expression                                              # additiveExpression
    |   LPAREN expression RPAREN                                                        # parenExpression
    |   FunctionName LPAREN (expression (',' expression)*)? RPAREN                      # functionExpression
    ;

BooleanConstant
    :   BooleanTrue
    |   BooleanFalse
    ;

NumberConstant
    :   (ADD|SUB)? IntegerConstant
    |   (ADD|SUB)? FloatingConstant
    ;

LPAREN : '(';
RPAREN : ')';
ADD : '+';
SUB : '-';
MUL : '*';
DIV : '/';
Equal : '=';
NotEqual : '!=';
Less : '<';
LessEqual : '<=';
Greater : '>';
GreaterEqual : '>=';
BooleanTrue : 'true';
BooleanFalse : 'false';

FunctionName
    :   Nondigit+
    ;

fragment
Nondigit
    :   [a-zA-Z_]
    ;

Identifier
    :   '${' .+? '}'
    ;

StringLiteral
    :   '"' .*? '"'
    ;

IntegerConstant
    :   '0'
    |   NonzeroDigit Digit*
    ;

FloatingConstant
    :   Digit+ '.' Digit+
    |   '.' Digit+
    ;

fragment
Digit
    :   [0-9]
    ;

fragment
NonzeroDigit
    :   [1-9]
    ;

WS
    :   [ \t\r\n]+ -> skip
    ;