// Generated from D:/workspace/projects/feature/dc-bi/dc-eagle-model/src/main/resources/antlr\ColumnEL.g4 by ANTLR 4.10.1
package org.example.sql.function;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class ColumnELLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.10.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, BooleanConstant=2, NumberConstant=3, LPAREN=4, RPAREN=5, ADD=6, 
		SUB=7, MUL=8, DIV=9, Equal=10, NotEqual=11, Less=12, LessEqual=13, Greater=14, 
		GreaterEqual=15, BooleanTrue=16, BooleanFalse=17, FunctionName=18, Identifier=19, 
		StringLiteral=20, IntegerConstant=21, FloatingConstant=22, WS=23;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"T__0", "BooleanConstant", "NumberConstant", "LPAREN", "RPAREN", "ADD", 
			"SUB", "MUL", "DIV", "Equal", "NotEqual", "Less", "LessEqual", "Greater", 
			"GreaterEqual", "BooleanTrue", "BooleanFalse", "FunctionName", "Nondigit", 
			"Identifier", "StringLiteral", "IntegerConstant", "FloatingConstant", 
			"Digit", "NonzeroDigit", "WS"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "','", null, null, "'('", "')'", "'+'", "'-'", "'*'", "'/'", "'='", 
			"'!='", "'<'", "'<='", "'>'", "'>='", "'true'", "'false'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, "BooleanConstant", "NumberConstant", "LPAREN", "RPAREN", 
			"ADD", "SUB", "MUL", "DIV", "Equal", "NotEqual", "Less", "LessEqual", 
			"Greater", "GreaterEqual", "BooleanTrue", "BooleanFalse", "FunctionName", 
			"Identifier", "StringLiteral", "IntegerConstant", "FloatingConstant", 
			"WS"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public ColumnELLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "ColumnEL.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\u0004\u0000\u0017\u00af\u0006\uffff\uffff\u0002\u0000\u0007\u0000\u0002"+
		"\u0001\u0007\u0001\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002"+
		"\u0004\u0007\u0004\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002"+
		"\u0007\u0007\u0007\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002"+
		"\u000b\u0007\u000b\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e"+
		"\u0002\u000f\u0007\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011"+
		"\u0002\u0012\u0007\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014"+
		"\u0002\u0015\u0007\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017"+
		"\u0002\u0018\u0007\u0018\u0002\u0019\u0007\u0019\u0001\u0000\u0001\u0000"+
		"\u0001\u0001\u0001\u0001\u0003\u0001:\b\u0001\u0001\u0002\u0001\u0002"+
		"\u0003\u0002>\b\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0003\u0002"+
		"C\b\u0002\u0001\u0002\u0003\u0002F\b\u0002\u0001\u0003\u0001\u0003\u0001"+
		"\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0001\u0006\u0001\u0006\u0001"+
		"\u0007\u0001\u0007\u0001\b\u0001\b\u0001\t\u0001\t\u0001\n\u0001\n\u0001"+
		"\n\u0001\u000b\u0001\u000b\u0001\f\u0001\f\u0001\f\u0001\r\u0001\r\u0001"+
		"\u000e\u0001\u000e\u0001\u000e\u0001\u000f\u0001\u000f\u0001\u000f\u0001"+
		"\u000f\u0001\u000f\u0001\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0001"+
		"\u0010\u0001\u0010\u0001\u0011\u0004\u0011o\b\u0011\u000b\u0011\f\u0011"+
		"p\u0001\u0012\u0001\u0012\u0001\u0013\u0001\u0013\u0001\u0013\u0001\u0013"+
		"\u0004\u0013y\b\u0013\u000b\u0013\f\u0013z\u0001\u0013\u0001\u0013\u0001"+
		"\u0014\u0001\u0014\u0005\u0014\u0081\b\u0014\n\u0014\f\u0014\u0084\t\u0014"+
		"\u0001\u0014\u0001\u0014\u0001\u0015\u0001\u0015\u0001\u0015\u0005\u0015"+
		"\u008b\b\u0015\n\u0015\f\u0015\u008e\t\u0015\u0003\u0015\u0090\b\u0015"+
		"\u0001\u0016\u0004\u0016\u0093\b\u0016\u000b\u0016\f\u0016\u0094\u0001"+
		"\u0016\u0001\u0016\u0004\u0016\u0099\b\u0016\u000b\u0016\f\u0016\u009a"+
		"\u0001\u0016\u0001\u0016\u0004\u0016\u009f\b\u0016\u000b\u0016\f\u0016"+
		"\u00a0\u0003\u0016\u00a3\b\u0016\u0001\u0017\u0001\u0017\u0001\u0018\u0001"+
		"\u0018\u0001\u0019\u0004\u0019\u00aa\b\u0019\u000b\u0019\f\u0019\u00ab"+
		"\u0001\u0019\u0001\u0019\u0002z\u0082\u0000\u001a\u0001\u0001\u0003\u0002"+
		"\u0005\u0003\u0007\u0004\t\u0005\u000b\u0006\r\u0007\u000f\b\u0011\t\u0013"+
		"\n\u0015\u000b\u0017\f\u0019\r\u001b\u000e\u001d\u000f\u001f\u0010!\u0011"+
		"#\u0012%\u0000\'\u0013)\u0014+\u0015-\u0016/\u00001\u00003\u0017\u0001"+
		"\u0000\u0004\u0003\u0000AZ__az\u0001\u000009\u0001\u000019\u0003\u0000"+
		"\t\n\r\r  \u00bb\u0000\u0001\u0001\u0000\u0000\u0000\u0000\u0003\u0001"+
		"\u0000\u0000\u0000\u0000\u0005\u0001\u0000\u0000\u0000\u0000\u0007\u0001"+
		"\u0000\u0000\u0000\u0000\t\u0001\u0000\u0000\u0000\u0000\u000b\u0001\u0000"+
		"\u0000\u0000\u0000\r\u0001\u0000\u0000\u0000\u0000\u000f\u0001\u0000\u0000"+
		"\u0000\u0000\u0011\u0001\u0000\u0000\u0000\u0000\u0013\u0001\u0000\u0000"+
		"\u0000\u0000\u0015\u0001\u0000\u0000\u0000\u0000\u0017\u0001\u0000\u0000"+
		"\u0000\u0000\u0019\u0001\u0000\u0000\u0000\u0000\u001b\u0001\u0000\u0000"+
		"\u0000\u0000\u001d\u0001\u0000\u0000\u0000\u0000\u001f\u0001\u0000\u0000"+
		"\u0000\u0000!\u0001\u0000\u0000\u0000\u0000#\u0001\u0000\u0000\u0000\u0000"+
		"\'\u0001\u0000\u0000\u0000\u0000)\u0001\u0000\u0000\u0000\u0000+\u0001"+
		"\u0000\u0000\u0000\u0000-\u0001\u0000\u0000\u0000\u00003\u0001\u0000\u0000"+
		"\u0000\u00015\u0001\u0000\u0000\u0000\u00039\u0001\u0000\u0000\u0000\u0005"+
		"E\u0001\u0000\u0000\u0000\u0007G\u0001\u0000\u0000\u0000\tI\u0001\u0000"+
		"\u0000\u0000\u000bK\u0001\u0000\u0000\u0000\rM\u0001\u0000\u0000\u0000"+
		"\u000fO\u0001\u0000\u0000\u0000\u0011Q\u0001\u0000\u0000\u0000\u0013S"+
		"\u0001\u0000\u0000\u0000\u0015U\u0001\u0000\u0000\u0000\u0017X\u0001\u0000"+
		"\u0000\u0000\u0019Z\u0001\u0000\u0000\u0000\u001b]\u0001\u0000\u0000\u0000"+
		"\u001d_\u0001\u0000\u0000\u0000\u001fb\u0001\u0000\u0000\u0000!g\u0001"+
		"\u0000\u0000\u0000#n\u0001\u0000\u0000\u0000%r\u0001\u0000\u0000\u0000"+
		"\'t\u0001\u0000\u0000\u0000)~\u0001\u0000\u0000\u0000+\u008f\u0001\u0000"+
		"\u0000\u0000-\u00a2\u0001\u0000\u0000\u0000/\u00a4\u0001\u0000\u0000\u0000"+
		"1\u00a6\u0001\u0000\u0000\u00003\u00a9\u0001\u0000\u0000\u000056\u0005"+
		",\u0000\u00006\u0002\u0001\u0000\u0000\u00007:\u0003\u001f\u000f\u0000"+
		"8:\u0003!\u0010\u000097\u0001\u0000\u0000\u000098\u0001\u0000\u0000\u0000"+
		":\u0004\u0001\u0000\u0000\u0000;>\u0003\u000b\u0005\u0000<>\u0003\r\u0006"+
		"\u0000=;\u0001\u0000\u0000\u0000=<\u0001\u0000\u0000\u0000=>\u0001\u0000"+
		"\u0000\u0000>?\u0001\u0000\u0000\u0000?F\u0003+\u0015\u0000@C\u0003\u000b"+
		"\u0005\u0000AC\u0003\r\u0006\u0000B@\u0001\u0000\u0000\u0000BA\u0001\u0000"+
		"\u0000\u0000BC\u0001\u0000\u0000\u0000CD\u0001\u0000\u0000\u0000DF\u0003"+
		"-\u0016\u0000E=\u0001\u0000\u0000\u0000EB\u0001\u0000\u0000\u0000F\u0006"+
		"\u0001\u0000\u0000\u0000GH\u0005(\u0000\u0000H\b\u0001\u0000\u0000\u0000"+
		"IJ\u0005)\u0000\u0000J\n\u0001\u0000\u0000\u0000KL\u0005+\u0000\u0000"+
		"L\f\u0001\u0000\u0000\u0000MN\u0005-\u0000\u0000N\u000e\u0001\u0000\u0000"+
		"\u0000OP\u0005*\u0000\u0000P\u0010\u0001\u0000\u0000\u0000QR\u0005/\u0000"+
		"\u0000R\u0012\u0001\u0000\u0000\u0000ST\u0005=\u0000\u0000T\u0014\u0001"+
		"\u0000\u0000\u0000UV\u0005!\u0000\u0000VW\u0005=\u0000\u0000W\u0016\u0001"+
		"\u0000\u0000\u0000XY\u0005<\u0000\u0000Y\u0018\u0001\u0000\u0000\u0000"+
		"Z[\u0005<\u0000\u0000[\\\u0005=\u0000\u0000\\\u001a\u0001\u0000\u0000"+
		"\u0000]^\u0005>\u0000\u0000^\u001c\u0001\u0000\u0000\u0000_`\u0005>\u0000"+
		"\u0000`a\u0005=\u0000\u0000a\u001e\u0001\u0000\u0000\u0000bc\u0005t\u0000"+
		"\u0000cd\u0005r\u0000\u0000de\u0005u\u0000\u0000ef\u0005e\u0000\u0000"+
		"f \u0001\u0000\u0000\u0000gh\u0005f\u0000\u0000hi\u0005a\u0000\u0000i"+
		"j\u0005l\u0000\u0000jk\u0005s\u0000\u0000kl\u0005e\u0000\u0000l\"\u0001"+
		"\u0000\u0000\u0000mo\u0003%\u0012\u0000nm\u0001\u0000\u0000\u0000op\u0001"+
		"\u0000\u0000\u0000pn\u0001\u0000\u0000\u0000pq\u0001\u0000\u0000\u0000"+
		"q$\u0001\u0000\u0000\u0000rs\u0007\u0000\u0000\u0000s&\u0001\u0000\u0000"+
		"\u0000tu\u0005$\u0000\u0000uv\u0005{\u0000\u0000vx\u0001\u0000\u0000\u0000"+
		"wy\t\u0000\u0000\u0000xw\u0001\u0000\u0000\u0000yz\u0001\u0000\u0000\u0000"+
		"z{\u0001\u0000\u0000\u0000zx\u0001\u0000\u0000\u0000{|\u0001\u0000\u0000"+
		"\u0000|}\u0005}\u0000\u0000}(\u0001\u0000\u0000\u0000~\u0082\u0005\"\u0000"+
		"\u0000\u007f\u0081\t\u0000\u0000\u0000\u0080\u007f\u0001\u0000\u0000\u0000"+
		"\u0081\u0084\u0001\u0000\u0000\u0000\u0082\u0083\u0001\u0000\u0000\u0000"+
		"\u0082\u0080\u0001\u0000\u0000\u0000\u0083\u0085\u0001\u0000\u0000\u0000"+
		"\u0084\u0082\u0001\u0000\u0000\u0000\u0085\u0086\u0005\"\u0000\u0000\u0086"+
		"*\u0001\u0000\u0000\u0000\u0087\u0090\u00050\u0000\u0000\u0088\u008c\u0003"+
		"1\u0018\u0000\u0089\u008b\u0003/\u0017\u0000\u008a\u0089\u0001\u0000\u0000"+
		"\u0000\u008b\u008e\u0001\u0000\u0000\u0000\u008c\u008a\u0001\u0000\u0000"+
		"\u0000\u008c\u008d\u0001\u0000\u0000\u0000\u008d\u0090\u0001\u0000\u0000"+
		"\u0000\u008e\u008c\u0001\u0000\u0000\u0000\u008f\u0087\u0001\u0000\u0000"+
		"\u0000\u008f\u0088\u0001\u0000\u0000\u0000\u0090,\u0001\u0000\u0000\u0000"+
		"\u0091\u0093\u0003/\u0017\u0000\u0092\u0091\u0001\u0000\u0000\u0000\u0093"+
		"\u0094\u0001\u0000\u0000\u0000\u0094\u0092\u0001\u0000\u0000\u0000\u0094"+
		"\u0095\u0001\u0000\u0000\u0000\u0095\u0096\u0001\u0000\u0000\u0000\u0096"+
		"\u0098\u0005.\u0000\u0000\u0097\u0099\u0003/\u0017\u0000\u0098\u0097\u0001"+
		"\u0000\u0000\u0000\u0099\u009a\u0001\u0000\u0000\u0000\u009a\u0098\u0001"+
		"\u0000\u0000\u0000\u009a\u009b\u0001\u0000\u0000\u0000\u009b\u00a3\u0001"+
		"\u0000\u0000\u0000\u009c\u009e\u0005.\u0000\u0000\u009d\u009f\u0003/\u0017"+
		"\u0000\u009e\u009d\u0001\u0000\u0000\u0000\u009f\u00a0\u0001\u0000\u0000"+
		"\u0000\u00a0\u009e\u0001\u0000\u0000\u0000\u00a0\u00a1\u0001\u0000\u0000"+
		"\u0000\u00a1\u00a3\u0001\u0000\u0000\u0000\u00a2\u0092\u0001\u0000\u0000"+
		"\u0000\u00a2\u009c\u0001\u0000\u0000\u0000\u00a3.\u0001\u0000\u0000\u0000"+
		"\u00a4\u00a5\u0007\u0001\u0000\u0000\u00a50\u0001\u0000\u0000\u0000\u00a6"+
		"\u00a7\u0007\u0002\u0000\u0000\u00a72\u0001\u0000\u0000\u0000\u00a8\u00aa"+
		"\u0007\u0003\u0000\u0000\u00a9\u00a8\u0001\u0000\u0000\u0000\u00aa\u00ab"+
		"\u0001\u0000\u0000\u0000\u00ab\u00a9\u0001\u0000\u0000\u0000\u00ab\u00ac"+
		"\u0001\u0000\u0000\u0000\u00ac\u00ad\u0001\u0000\u0000\u0000\u00ad\u00ae"+
		"\u0006\u0019\u0000\u0000\u00ae4\u0001\u0000\u0000\u0000\u000f\u00009="+
		"BEpz\u0082\u008c\u008f\u0094\u009a\u00a0\u00a2\u00ab\u0001\u0006\u0000"+
		"\u0000";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}