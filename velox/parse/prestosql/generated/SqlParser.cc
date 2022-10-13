/* SqlParser.cc */
#include "SqlParser.h"
#include "TokenMgrError.h"
#include "SimpleNode.h"
namespace commonsql {
namespace parser {



Node    * SqlParser::compilation_unit() {Token *begin;/*@bgen(jjtree) CompilationUnit */
  CompilationUnit *jjtn000 = new CompilationUnit(JJTCOMPILATIONUNIT);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      while (!hasError) {
        if (jj_2_1(3)) {
          ;
        } else {
          goto end_label_1;
        }
        jj_consume_token(semicolon);
      }
      end_label_1: ;
      while (!hasError) {
        if (NotEof()) {
          ;
        } else {
          goto end_label_2;
        }
begin = getToken(1);
        statement_list();
if (hasError) cout << "Error parsing statement at: " << begin->beginLine;
SyncToSemicolon();
      }
      end_label_2: ;
      jj_consume_token(0);
jjtree.closeNodeScope(jjtn000, true);
      jjtc000 = false;
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn000);
      }
if (jjtree.peekNode() != nullptr) return jjtree.peekNode(); return null;
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
assert(false);
}


void SqlParser::statement_list() {
    while (!hasError) {
      direct_SQL_statement();
      while (!hasError) {
        if (jj_2_2(3)) {
          ;
        } else {
          goto end_label_4;
        }
        jj_consume_token(semicolon);
      }
      end_label_4: ;
      if (NotEof()) {
        ;
      } else {
        goto end_label_3;
      }
    }
    end_label_3: ;
SyncToSemicolon();
}


void SqlParser::non_reserved_word() {
    if (jj_2_3(3)) {
      jj_consume_token(A);
    } else if (jj_2_4(3)) {
      jj_consume_token(ABSOLUTE);
    } else if (jj_2_5(3)) {
      jj_consume_token(ACTION);
    } else if (jj_2_6(3)) {
      jj_consume_token(ADA);
    } else if (jj_2_7(3)) {
      jj_consume_token(ADD);
    } else if (jj_2_8(3)) {
      jj_consume_token(ADMIN);
    } else if (jj_2_9(3)) {
      jj_consume_token(AFTER);
    } else if (jj_2_10(3)) {
      jj_consume_token(ALWAYS);
    } else if (jj_2_11(3)) {
      jj_consume_token(ASC);
    } else if (jj_2_12(3)) {
      jj_consume_token(ASSERTION);
    } else if (jj_2_13(3)) {
      jj_consume_token(ASSIGNMENT);
    } else if (jj_2_14(3)) {
      jj_consume_token(ATTRIBUTE);
    } else if (jj_2_15(3)) {
      jj_consume_token(ATTRIBUTES);
    } else if (jj_2_16(3)) {
      jj_consume_token(BEFORE);
    } else if (jj_2_17(3)) {
      jj_consume_token(BERNOULLI);
    } else if (jj_2_18(3)) {
      jj_consume_token(BREADTH);
    } else if (jj_2_19(3)) {
      jj_consume_token(C);
    } else if (jj_2_20(3)) {
      jj_consume_token(CASCADE);
    } else if (jj_2_21(3)) {
      jj_consume_token(CATALOG);
    } else if (jj_2_22(3)) {
      jj_consume_token(CATALOG_NAME);
    } else if (jj_2_23(3)) {
      jj_consume_token(CHAIN);
    } else if (jj_2_24(3)) {
      jj_consume_token(CHARACTER_SET_CATALOG);
    } else if (jj_2_25(3)) {
      jj_consume_token(CHARACTER_SET_NAME);
    } else if (jj_2_26(3)) {
      jj_consume_token(CHARACTER_SET_SCHEMA);
    } else if (jj_2_27(3)) {
      jj_consume_token(CHARACTERISTICS);
    } else if (jj_2_28(3)) {
      jj_consume_token(CHARACTERS);
    } else if (jj_2_29(3)) {
      jj_consume_token(CLASS_ORIGIN);
    } else if (jj_2_30(3)) {
      jj_consume_token(COBOL);
    } else if (jj_2_31(3)) {
      jj_consume_token(COLLATION);
    } else if (jj_2_32(3)) {
      jj_consume_token(COLLATION_CATALOG);
    } else if (jj_2_33(3)) {
      jj_consume_token(COLLATION_NAME);
    } else if (jj_2_34(3)) {
      jj_consume_token(COLLATION_SCHEMA);
    } else if (jj_2_35(3)) {
      jj_consume_token(COLUMN_NAME);
    } else if (jj_2_36(3)) {
      jj_consume_token(COMMAND_FUNCTION);
    } else if (jj_2_37(3)) {
      jj_consume_token(COMMAND_FUNCTION_CODE);
    } else if (jj_2_38(3)) {
      jj_consume_token(COMMITTED);
    } else if (jj_2_39(3)) {
      jj_consume_token(CONDITION_NUMBER);
    } else if (jj_2_40(3)) {
      jj_consume_token(CONNECTION);
    } else if (jj_2_41(3)) {
      jj_consume_token(CONNECTION_NAME);
    } else if (jj_2_42(3)) {
      jj_consume_token(CONSTRAINT_CATALOG);
    } else if (jj_2_43(3)) {
      jj_consume_token(CONSTRAINT_NAME);
    } else if (jj_2_44(3)) {
      jj_consume_token(CONSTRAINT_SCHEMA);
    } else if (jj_2_45(3)) {
      jj_consume_token(CONSTRAINTS);
    } else if (jj_2_46(3)) {
      jj_consume_token(CONSTRUCTOR);
    } else if (jj_2_47(3)) {
      jj_consume_token(CONTAINS);
    } else if (jj_2_48(3)) {
      jj_consume_token(CONTINUE);
    } else if (jj_2_49(3)) {
      jj_consume_token(CURSOR_NAME);
    } else if (jj_2_50(3)) {
      jj_consume_token(DATA);
    } else if (jj_2_51(3)) {
      jj_consume_token(DATETIME_INTERVAL_CODE);
    } else if (jj_2_52(3)) {
      jj_consume_token(DATETIME_INTERVAL_PRECISION);
    } else if (jj_2_53(3)) {
      jj_consume_token(DEFAULTS);
    } else if (jj_2_54(3)) {
      jj_consume_token(DEFERRABLE);
    } else if (jj_2_55(3)) {
      jj_consume_token(DEFERRED);
    } else if (jj_2_56(3)) {
      jj_consume_token(DEFINED);
    } else if (jj_2_57(3)) {
      jj_consume_token(DEFINER);
    } else if (jj_2_58(3)) {
      jj_consume_token(DEGREE);
    } else if (jj_2_59(3)) {
      jj_consume_token(DEPTH);
    } else if (jj_2_60(3)) {
      jj_consume_token(DERIVED);
    } else if (jj_2_61(3)) {
      jj_consume_token(DESC);
    } else if (jj_2_62(3)) {
      jj_consume_token(DESCRIPTOR);
    } else if (jj_2_63(3)) {
      jj_consume_token(DIAGNOSTICS);
    } else if (jj_2_64(3)) {
      jj_consume_token(DISPATCH);
    } else if (jj_2_65(3)) {
      jj_consume_token(DOMAIN);
    } else if (jj_2_66(3)) {
      jj_consume_token(DYNAMIC_FUNCTION);
    } else if (jj_2_67(3)) {
      jj_consume_token(DYNAMIC_FUNCTION_CODE);
    } else if (jj_2_68(3)) {
      jj_consume_token(ENFORCED);
    } else if (jj_2_69(3)) {
      jj_consume_token(EQUALS);
    } else if (jj_2_70(3)) {
      jj_consume_token(EXCLUDE);
    } else if (jj_2_71(3)) {
      jj_consume_token(EXCLUDING);
    } else if (jj_2_72(3)) {
      jj_consume_token(EXPRESSION);
    } else if (jj_2_73(3)) {
      jj_consume_token(FINAL);
    } else if (jj_2_74(3)) {
      jj_consume_token(FIRST);
    } else if (jj_2_75(3)) {
      jj_consume_token(FLAG);
    } else if (jj_2_76(3)) {
      jj_consume_token(FOLLOWING);
    } else if (jj_2_77(3)) {
      jj_consume_token(FORTRAN);
    } else if (jj_2_78(3)) {
      jj_consume_token(FOUND);
    } else if (jj_2_79(3)) {
      jj_consume_token(G);
    } else if (jj_2_80(3)) {
      jj_consume_token(GENERAL);
    } else if (jj_2_81(3)) {
      jj_consume_token(GENERATED);
    } else if (jj_2_82(3)) {
      jj_consume_token(GO);
    } else if (jj_2_83(3)) {
      jj_consume_token(GOTO);
    } else if (jj_2_84(3)) {
      jj_consume_token(GRANTED);
    } else if (jj_2_85(3)) {
      jj_consume_token(HIERARCHY);
    } else if (jj_2_86(3)) {
      jj_consume_token(IF);
    } else if (jj_2_87(3)) {
      jj_consume_token(IGNORE);
    } else if (jj_2_88(3)) {
      jj_consume_token(IMMEDIATE);
    } else if (jj_2_89(3)) {
      jj_consume_token(IMPLEMENTATION);
    } else if (jj_2_90(3)) {
      jj_consume_token(INCLUDING);
    } else if (jj_2_91(3)) {
      jj_consume_token(INCREMENT);
    } else if (jj_2_92(3)) {
      jj_consume_token(INITIALLY);
    } else if (jj_2_93(3)) {
      jj_consume_token(INPUT);
    } else if (jj_2_94(3)) {
      jj_consume_token(INSTANCE);
    } else if (jj_2_95(3)) {
      jj_consume_token(INSTANTIABLE);
    } else if (jj_2_96(3)) {
      jj_consume_token(INSTEAD);
    } else if (jj_2_97(3)) {
      jj_consume_token(INVOKER);
    } else if (jj_2_98(3)) {
      jj_consume_token(ISOLATION);
    } else if (jj_2_99(3)) {
      jj_consume_token(K);
    } else if (jj_2_100(3)) {
      jj_consume_token(KEY);
    } else if (jj_2_101(3)) {
      jj_consume_token(KEY_MEMBER);
    } else if (jj_2_102(3)) {
      jj_consume_token(KEY_TYPE);
    } else if (jj_2_103(3)) {
      jj_consume_token(LAST);
    } else if (jj_2_104(3)) {
      jj_consume_token(LENGTH);
    } else if (jj_2_105(3)) {
      jj_consume_token(LEVEL);
    } else if (jj_2_106(3)) {
      jj_consume_token(LOCATOR);
    } else if (jj_2_107(3)) {
      jj_consume_token(M);
    } else if (jj_2_108(3)) {
      jj_consume_token(MAP);
    } else if (jj_2_109(3)) {
      jj_consume_token(MATCHED);
    } else if (jj_2_110(3)) {
      jj_consume_token(MAXVALUE);
    } else if (jj_2_111(3)) {
      jj_consume_token(MESSAGE_LENGTH);
    } else if (jj_2_112(3)) {
      jj_consume_token(MESSAGE_OCTET_LENGTH);
    } else if (jj_2_113(3)) {
      jj_consume_token(MESSAGE_TEXT);
    } else if (jj_2_114(3)) {
      jj_consume_token(MINVALUE);
    } else if (jj_2_115(3)) {
      jj_consume_token(MORE_);
    } else if (jj_2_116(3)) {
      jj_consume_token(MUMPS);
    } else if (jj_2_117(3)) {
      jj_consume_token(NAMES);
    } else if (jj_2_118(3)) {
      jj_consume_token(NESTING);
    } else if (jj_2_119(3)) {
      jj_consume_token(NEXT);
    } else if (jj_2_120(3)) {
      jj_consume_token(NFC);
    } else if (jj_2_121(3)) {
      jj_consume_token(NFD);
    } else if (jj_2_122(3)) {
      jj_consume_token(NFKC);
    } else if (jj_2_123(3)) {
      jj_consume_token(NFKD);
    } else if (jj_2_124(3)) {
      jj_consume_token(NORMALIZED);
    } else if (jj_2_125(3)) {
      jj_consume_token(NULLABLE);
    } else if (jj_2_126(3)) {
      jj_consume_token(NULLS);
    } else if (jj_2_127(3)) {
      jj_consume_token(NUMBER);
    } else if (jj_2_128(3)) {
      jj_consume_token(OBJECT);
    } else if (jj_2_129(3)) {
      jj_consume_token(OCTETS);
    } else if (jj_2_130(3)) {
      jj_consume_token(OPTION);
    } else if (jj_2_131(3)) {
      jj_consume_token(OPTIONS);
    } else if (jj_2_132(3)) {
      jj_consume_token(ORDERING);
    } else if (jj_2_133(3)) {
      jj_consume_token(ORDINALITY);
    } else if (jj_2_134(3)) {
      jj_consume_token(OTHERS);
    } else if (jj_2_135(3)) {
      jj_consume_token(OUTPUT);
    } else if (jj_2_136(3)) {
      jj_consume_token(OVERRIDING);
    } else if (jj_2_137(3)) {
      jj_consume_token(P);
    } else if (jj_2_138(3)) {
      jj_consume_token(PAD);
    } else if (jj_2_139(3)) {
      jj_consume_token(PARAMETER_MODE);
    } else if (jj_2_140(3)) {
      jj_consume_token(PARAMETER_NAME);
    } else if (jj_2_141(3)) {
      jj_consume_token(PARAMETER_ORDINAL_POSITION);
    } else if (jj_2_142(3)) {
      jj_consume_token(PARAMETER_SPECIFIC_CATALOG);
    } else if (jj_2_143(3)) {
      jj_consume_token(PARAMETER_SPECIFIC_NAME);
    } else if (jj_2_144(3)) {
      jj_consume_token(PARAMETER_SPECIFIC_SCHEMA);
    } else if (jj_2_145(3)) {
      jj_consume_token(PARTIAL);
    } else if (jj_2_146(3)) {
      jj_consume_token(PASCAL);
    } else if (jj_2_147(3)) {
      jj_consume_token(PATH);
    } else if (jj_2_148(3)) {
      jj_consume_token(PLACING);
    } else if (jj_2_149(3)) {
      jj_consume_token(PLI);
    } else if (jj_2_150(3)) {
      jj_consume_token(PRECEDING);
    } else if (jj_2_151(3)) {
      jj_consume_token(PRESERVE);
    } else if (jj_2_152(3)) {
      jj_consume_token(PRIOR);
    } else if (jj_2_153(3)) {
      jj_consume_token(PRIVILEGES);
    } else if (jj_2_154(3)) {
      jj_consume_token(PROPERTIES);
    } else if (jj_2_155(3)) {
      jj_consume_token(PUBLIC);
    } else if (jj_2_156(3)) {
      jj_consume_token(READ);
    } else if (jj_2_157(3)) {
      jj_consume_token(RELATIVE);
    } else if (jj_2_158(3)) {
      jj_consume_token(REPEATABLE);
    } else if (jj_2_159(3)) {
      jj_consume_token(RESPECT);
    } else if (jj_2_160(3)) {
      jj_consume_token(RESTART);
    } else if (jj_2_161(3)) {
      jj_consume_token(RESTRICT);
    } else if (jj_2_162(3)) {
      jj_consume_token(RETURNED_CARDINALITY);
    } else if (jj_2_163(3)) {
      jj_consume_token(RETURNED_LENGTH);
    } else if (jj_2_164(3)) {
      jj_consume_token(RETURNED_OCTET_LENGTH);
    } else if (jj_2_165(3)) {
      jj_consume_token(RETURNED_SQLSTATE);
    } else if (jj_2_166(3)) {
      jj_consume_token(ROLE);
    } else if (jj_2_167(3)) {
      jj_consume_token(ROUTINE);
    } else if (jj_2_168(3)) {
      jj_consume_token(ROUTINE_CATALOG);
    } else if (jj_2_169(3)) {
      jj_consume_token(ROUTINE_NAME);
    } else if (jj_2_170(3)) {
      jj_consume_token(ROUTINE_SCHEMA);
    } else if (jj_2_171(3)) {
      jj_consume_token(ROW_COUNT);
    } else if (jj_2_172(3)) {
      jj_consume_token(SCALE);
    } else if (jj_2_173(3)) {
      jj_consume_token(SCHEMA);
    } else if (jj_2_174(3)) {
      jj_consume_token(SCHEMA_NAME);
    } else if (jj_2_175(3)) {
      jj_consume_token(SCOPE_CATALOG);
    } else if (jj_2_176(3)) {
      jj_consume_token(SCOPE_NAME);
    } else if (jj_2_177(3)) {
      jj_consume_token(SCOPE_SCHEMA);
    } else if (jj_2_178(3)) {
      jj_consume_token(SECTION);
    } else if (jj_2_179(3)) {
      jj_consume_token(SECURITY);
    } else if (jj_2_180(3)) {
      jj_consume_token(SELF);
    } else if (jj_2_181(3)) {
      jj_consume_token(SEQUENCE);
    } else if (jj_2_182(3)) {
      jj_consume_token(SERIALIZABLE);
    } else if (jj_2_183(3)) {
      jj_consume_token(SERVER_NAME);
    } else if (jj_2_184(3)) {
      jj_consume_token(SESSION);
    } else if (jj_2_185(3)) {
      jj_consume_token(SETS);
    } else if (jj_2_186(3)) {
      jj_consume_token(SIMPLE);
    } else if (jj_2_187(3)) {
      jj_consume_token(SIZE);
    } else if (jj_2_188(3)) {
      jj_consume_token(SOURCE);
    } else if (jj_2_189(3)) {
      jj_consume_token(SPACE);
    } else if (jj_2_190(3)) {
      jj_consume_token(SPECIFIC_NAME);
    } else if (jj_2_191(3)) {
      jj_consume_token(STATE);
    } else if (jj_2_192(3)) {
      jj_consume_token(STATEMENT);
    } else if (jj_2_193(3)) {
      jj_consume_token(STRUCTURE);
    } else if (jj_2_194(3)) {
      jj_consume_token(STYLE);
    } else if (jj_2_195(3)) {
      jj_consume_token(SUBCLASS_ORIGIN);
    } else if (jj_2_196(3)) {
      jj_consume_token(T);
    } else if (jj_2_197(3)) {
      jj_consume_token(TABLE_NAME);
    } else if (jj_2_198(3)) {
      jj_consume_token(TEMPORARY);
    } else if (jj_2_199(3)) {
      jj_consume_token(TIES);
    } else if (jj_2_200(3)) {
      jj_consume_token(TOP_LEVEL_COUNT);
    } else if (jj_2_201(3)) {
      jj_consume_token(TRANSACTION);
    } else if (jj_2_202(3)) {
      jj_consume_token(TRANSACTION_ACTIVE);
    } else if (jj_2_203(3)) {
      jj_consume_token(TRANSACTIONS_COMMITTED);
    } else if (jj_2_204(3)) {
      jj_consume_token(TRANSACTIONS_ROLLED_BACK);
    } else if (jj_2_205(3)) {
      jj_consume_token(TRANSFORM);
    } else if (jj_2_206(3)) {
      jj_consume_token(TRANSFORMS);
    } else if (jj_2_207(3)) {
      jj_consume_token(TRIGGER_CATALOG);
    } else if (jj_2_208(3)) {
      jj_consume_token(TRIGGER_NAME);
    } else if (jj_2_209(3)) {
      jj_consume_token(TRIGGER_SCHEMA);
    } else if (jj_2_210(3)) {
      jj_consume_token(TRY_CAST);
    } else if (jj_2_211(3)) {
      jj_consume_token(TYPE);
    } else if (jj_2_212(3)) {
      jj_consume_token(UNBOUNDED);
    } else if (jj_2_213(3)) {
      jj_consume_token(UNCOMMITTED);
    } else if (jj_2_214(3)) {
      jj_consume_token(UNDER);
    } else if (jj_2_215(3)) {
      jj_consume_token(UNNAMED);
    } else if (jj_2_216(3)) {
      jj_consume_token(USAGE);
    } else if (jj_2_217(3)) {
      jj_consume_token(USER_DEFINED_TYPE_CATALOG);
    } else if (jj_2_218(3)) {
      jj_consume_token(USER_DEFINED_TYPE_CODE);
    } else if (jj_2_219(3)) {
      jj_consume_token(USER_DEFINED_TYPE_NAME);
    } else if (jj_2_220(3)) {
      jj_consume_token(USER_DEFINED_TYPE_SCHEMA);
    } else if (jj_2_221(3)) {
      jj_consume_token(VIEW);
    } else if (jj_2_222(3)) {
      jj_consume_token(WORK);
    } else if (jj_2_223(3)) {
      jj_consume_token(WRITE);
    } else if (jj_2_224(3)) {
      jj_consume_token(ZONE);
    } else if (jj_2_225(3)) {
      jj_consume_token(ABS);
    } else if (jj_2_226(3)) {
      jj_consume_token(ALL);
    } else if (jj_2_227(3)) {
      jj_consume_token(ARRAY_AGG);
    } else if (jj_2_228(3)) {
      jj_consume_token(AT);
    } else if (jj_2_229(3)) {
      jj_consume_token(AVG);
    } else if (jj_2_230(3)) {
      jj_consume_token(BLOB);
    } else if (jj_2_231(3)) {
      jj_consume_token(BOTH);
    } else if (jj_2_232(3)) {
      jj_consume_token(CARDINALITY);
    } else if (jj_2_233(3)) {
      jj_consume_token(CLOSE);
    } else if (jj_2_234(3)) {
      jj_consume_token(COLUMN);
    } else if (jj_2_235(3)) {
      jj_consume_token(CONDITION);
    } else if (jj_2_236(3)) {
      jj_consume_token(COUNT);
    } else if (jj_2_237(3)) {
      jj_consume_token(CUBE);
    } else if (jj_2_238(3)) {
      jj_consume_token(CURRENT);
    } else if (jj_2_239(3)) {
      jj_consume_token(CURRENT_CATALOG);
    } else if (jj_2_240(3)) {
      jj_consume_token(CURRENT_DATE);
    } else if (jj_2_241(3)) {
      jj_consume_token(CURRENT_DEFAULT_TRANSFORM_GROUP);
    } else if (jj_2_242(3)) {
      jj_consume_token(CURRENT_PATH);
    } else if (jj_2_243(3)) {
      jj_consume_token(CURRENT_ROLE);
    } else if (jj_2_244(3)) {
      jj_consume_token(CURRENT_SCHEMA);
    } else if (jj_2_245(3)) {
      jj_consume_token(CURRENT_TIME);
    } else if (jj_2_246(3)) {
      jj_consume_token(CURRENT_TIMESTAMP);
    } else if (jj_2_247(3)) {
      jj_consume_token(CURRENT_TRANSFORM_GROUP_FOR_TYPE);
    } else if (jj_2_248(3)) {
      jj_consume_token(CURRENT_USER);
    } else if (jj_2_249(3)) {
      jj_consume_token(CURSOR);
    } else if (jj_2_250(3)) {
      jj_consume_token(CYCLE);
    } else if (jj_2_251(3)) {
      jj_consume_token(DATE);
    } else if (jj_2_252(3)) {
      jj_consume_token(DAY);
    } else if (jj_2_253(3)) {
      jj_consume_token(DAYS);
    } else if (jj_2_254(3)) {
      jj_consume_token(DEC);
    } else if (jj_2_255(3)) {
      jj_consume_token(DYNAMIC);
    } else if (jj_2_256(3)) {
      jj_consume_token(EXP);
    } else if (jj_2_257(3)) {
      jj_consume_token(EXTERNAL);
    } else if (jj_2_258(3)) {
      jj_consume_token(FILTER);
    } else if (jj_2_259(3)) {
      jj_consume_token(FLOOR);
    } else if (jj_2_260(3)) {
      jj_consume_token(FREE);
    } else if (jj_2_261(3)) {
      jj_consume_token(FUNCTION);
    } else if (jj_2_262(3)) {
      jj_consume_token(GLOBAL);
    } else if (jj_2_263(3)) {
      jj_consume_token(HOLD);
    } else if (jj_2_264(3)) {
      jj_consume_token(HOUR);
    } else if (jj_2_265(3)) {
      jj_consume_token(HOURS);
    } else if (jj_2_266(3)) {
      jj_consume_token(IDENTITY);
    } else if (jj_2_267(3)) {
      jj_consume_token(INDICATOR);
    } else if (jj_2_268(3)) {
      jj_consume_token(INTERSECTION);
    } else if (jj_2_269(3)) {
      jj_consume_token(INTERVAL);
    } else if (jj_2_270(3)) {
      jj_consume_token(LANGUAGE);
    } else if (jj_2_271(3)) {
      jj_consume_token(LEAD);
    } else if (jj_2_272(3)) {
      jj_consume_token(LOCAL);
    } else if (jj_2_273(3)) {
      jj_consume_token(LOWER);
    } else if (jj_2_274(3)) {
      jj_consume_token(MAX);
    } else if (jj_2_275(3)) {
      jj_consume_token(MERGE);
    } else if (jj_2_276(3)) {
      jj_consume_token(METHOD);
    } else if (jj_2_277(3)) {
      jj_consume_token(MIN);
    } else if (jj_2_278(3)) {
      jj_consume_token(MINUTE);
    } else if (jj_2_279(3)) {
      jj_consume_token(MINUTES);
    } else if (jj_2_280(3)) {
      jj_consume_token(MOD);
    } else if (jj_2_281(3)) {
      jj_consume_token(MODULE);
    } else if (jj_2_282(3)) {
      jj_consume_token(MONTH);
    } else if (jj_2_283(3)) {
      jj_consume_token(MONTHS);
    } else if (jj_2_284(3)) {
      jj_consume_token(NAME);
    } else if (jj_2_285(3)) {
      jj_consume_token(NEW);
    } else if (jj_2_286(3)) {
      jj_consume_token(NONE);
    } else if (jj_2_287(3)) {
      jj_consume_token(OCCURRENCE);
    } else if (jj_2_288(3)) {
      jj_consume_token(OFFSET);
    } else if (jj_2_289(3)) {
      jj_consume_token(OLD);
    } else if (jj_2_290(3)) {
      jj_consume_token(OPEN);
    } else if (jj_2_291(3)) {
      jj_consume_token(PARTITION);
    } else if (jj_2_292(3)) {
      jj_consume_token(POSITION);
    } else if (jj_2_293(3)) {
      jj_consume_token(POWER);
    } else if (jj_2_294(3)) {
      jj_consume_token(PRECISION);
    } else if (jj_2_295(3)) {
      jj_consume_token(RANGE);
    } else if (jj_2_296(3)) {
      jj_consume_token(RANK);
    } else if (jj_2_297(3)) {
      jj_consume_token(READS);
    } else if (jj_2_298(3)) {
      jj_consume_token(REF);
    } else if (jj_2_299(3)) {
      jj_consume_token(REFERENCES);
    } else if (jj_2_300(3)) {
      jj_consume_token(RELEASE);
    } else if (jj_2_301(3)) {
      jj_consume_token(RESULT);
    } else if (jj_2_302(3)) {
      jj_consume_token(RETURNS);
    } else if (jj_2_303(3)) {
      jj_consume_token(ROLLUP);
    } else if (jj_2_304(3)) {
      jj_consume_token(ROW);
    } else if (jj_2_305(3)) {
      jj_consume_token(ROW_NUMBER);
    } else if (jj_2_306(3)) {
      jj_consume_token(ROWS);
    } else if (jj_2_307(3)) {
      jj_consume_token(SAVEPOINT);
    } else if (jj_2_308(3)) {
      jj_consume_token(SCOPE);
    } else if (jj_2_309(3)) {
      jj_consume_token(SEARCH);
    } else if (jj_2_310(3)) {
      jj_consume_token(SECOND);
    } else if (jj_2_311(3)) {
      jj_consume_token(SECONDS);
    } else if (jj_2_312(3)) {
      jj_consume_token(SESSION_USER);
    } else if (jj_2_313(3)) {
      jj_consume_token(SQL);
    } else if (jj_2_314(3)) {
      jj_consume_token(START);
    } else if (jj_2_315(3)) {
      jj_consume_token(STATIC);
    } else if (jj_2_316(3)) {
      jj_consume_token(SUM);
    } else if (jj_2_317(3)) {
      jj_consume_token(SYSTEM);
    } else if (jj_2_318(3)) {
      jj_consume_token(TIME);
    } else if (jj_2_319(3)) {
      jj_consume_token(TIMESTAMP);
    } else if (jj_2_320(3)) {
      jj_consume_token(TIMEZONE_HOUR);
    } else if (jj_2_321(3)) {
      jj_consume_token(TIMEZONE_MINUTE);
    } else if (jj_2_322(3)) {
      jj_consume_token(TRIGGER);
    } else if (jj_2_323(3)) {
      jj_consume_token(TRUNCATE);
    } else if (jj_2_324(3)) {
      jj_consume_token(UNKNOWN);
    } else if (jj_2_325(3)) {
      jj_consume_token(UPDATE);
    } else if (jj_2_326(3)) {
      jj_consume_token(UPPER);
    } else if (jj_2_327(3)) {
      jj_consume_token(USER);
    } else if (jj_2_328(3)) {
      jj_consume_token(VALUE);
    } else if (jj_2_329(3)) {
      jj_consume_token(VALUES);
    } else if (jj_2_330(3)) {
      jj_consume_token(VERSION);
    } else if (jj_2_331(3)) {
      jj_consume_token(VERSIONS);
    } else if (jj_2_332(3)) {
      jj_consume_token(WINDOW);
    } else if (jj_2_333(3)) {
      jj_consume_token(YEAR);
    } else if (jj_2_334(3)) {
      jj_consume_token(YEARS);
    } else if (jj_2_335(3)) {
      jj_consume_token(COMMENT);
    } else if (jj_2_336(3)) {
      jj_consume_token(DEFAULT_);
    } else if (jj_2_337(3)) {
      jj_consume_token(USE);
    } else if (jj_2_338(3)) {
      jj_consume_token(LIMIT);
    } else if (jj_2_339(3)) {
      jj_consume_token(338);
    } else if (jj_2_340(3)) {
      jj_consume_token(REPLACE);
    } else if (jj_2_341(3)) {
      jj_consume_token(340);
    } else if (jj_2_342(3)) {
      jj_consume_token(341);
    } else if (jj_2_343(3)) {
      jj_consume_token(342);
    } else if (jj_2_344(3)) {
      jj_consume_token(343);
    } else if (jj_2_345(3)) {
      jj_consume_token(344);
    } else if (jj_2_346(3)) {
      jj_consume_token(COUNT_QUOTED);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::left_bracket_or_trigraph() {
    if (jj_2_347(3)) {
      jj_consume_token(562);
    } else if (jj_2_348(3)) {
      jj_consume_token(563);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::right_bracket_or_trigraph() {
    if (jj_2_349(3)) {
      jj_consume_token(564);
    } else if (jj_2_350(3)) {
      jj_consume_token(565);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::literal() {
    if (jj_2_351(3)) {
      signed_numeric_literal();
    } else if (jj_2_352(3)) {
      general_literal();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::signed_numeric_literal() {
    if (jj_2_355(3)) {
      unsigned_numeric_literal();
    } else if (jj_2_356(3)) {
UnaryExpression *jjtn001 = new UnaryExpression(JJTUNARYEXPRESSION);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        if (jj_2_353(3)) {
          jj_consume_token(PLUS);
        } else if (jj_2_354(3)) {
          jj_consume_token(MINUS);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
        unsigned_numeric_literal();
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001,  1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::unsigned_literal() {
    if (jj_2_357(3)) {
      unsigned_numeric_literal();
    } else if (jj_2_358(3)) {
      general_literal();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::unsigned_numeric_literal() {/*@bgen(jjtree) UnsignedNumericLiteral */
  UnsignedNumericLiteral *jjtn000 = new UnsignedNumericLiteral(JJTUNSIGNEDNUMERICLITERAL);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_359(3)) {
        exact_numeric_literal();
      } else if (jj_2_360(3)) {
        jj_consume_token(approximate_numeric_literal);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::exact_numeric_literal() {
    if (jj_2_361(3)) {
      jj_consume_token(unsigned_integer);
    } else if (jj_2_362(3)) {
      jj_consume_token(float_literal);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::general_literal() {
    if (jj_2_363(3)) {
      character_string_literal();
    } else if (jj_2_364(3)) {
      jj_consume_token(national_character_string_literal);
    } else if (jj_2_365(3)) {
      Unicode_character_string_literal();
    } else if (jj_2_366(3)) {
      jj_consume_token(binary_string_literal);
    } else if (jj_2_367(3)) {
      datetime_literal();
    } else if (jj_2_368(3)) {
      interval_literal();
    } else if (jj_2_369(3)) {
      boolean_literal();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::character_string_literal() {/*@bgen(jjtree) CharStringLiteral */
  CharStringLiteral *jjtn000 = new CharStringLiteral(JJTCHARSTRINGLITERAL);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_370(3)) {
        jj_consume_token(underscore);
        character_set_specification();
      } else {
        ;
      }
      while (!hasError) {
        jj_consume_token(quoted_string);
        if (jj_2_371(3)) {
          ;
        } else {
          goto end_label_5;
        }
      }
      end_label_5: ;
    } catch ( ...) {
if (jjtc000) {
      jjtree.clearNodeScope(jjtn000);
      jjtc000 = false;
    } else {
      jjtree.popNode();
    }
    }
if (jjtc000) {
      jjtree.closeNodeScope(jjtn000, true);
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn000);
      }
    }
}


void SqlParser::Unicode_character_string_literal() {/*@bgen(jjtree) CharStringLiteral */
  CharStringLiteral *jjtn000 = new CharStringLiteral(JJTCHARSTRINGLITERAL);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_372(3)) {
        jj_consume_token(underscore);
        character_set_specification();
      } else {
        ;
      }
      jj_consume_token(unicode_literal);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::datetime_literal() {
    if (jj_2_373(3)) {
      date_literal();
    } else if (jj_2_374(3)) {
      time_literal();
    } else if (jj_2_375(3)) {
      timestamp_literal();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::date_literal() {/*@bgen(jjtree) DateLiteral */
  DateLiteral *jjtn000 = new DateLiteral(JJTDATELITERAL);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(DATE);
      character_string_literal();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::time_literal() {/*@bgen(jjtree) TimeLiteral */
  TimeLiteral *jjtn000 = new TimeLiteral(JJTTIMELITERAL);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(TIME);
      character_string_literal();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::timestamp_literal() {/*@bgen(jjtree) TimestampLiteral */
  TimestampLiteral *jjtn000 = new TimestampLiteral(JJTTIMESTAMPLITERAL);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(TIMESTAMP);
      character_string_literal();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::interval_literal() {/*@bgen(jjtree) IntervalLiteral */
  IntervalLiteral *jjtn000 = new IntervalLiteral(JJTINTERVALLITERAL);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(INTERVAL);
      if (jj_2_378(3)) {
        if (jj_2_376(3)) {
          jj_consume_token(PLUS);
        } else if (jj_2_377(3)) {
          jj_consume_token(MINUS);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } else {
        ;
      }
      character_string_literal();
      interval_qualifier();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::boolean_literal() {
    if (jj_2_381(3)) {
BooleanLiteral *jjtn001 = new BooleanLiteral(JJTBOOLEANLITERAL);
    bool jjtc001 = true;
    jjtree.openNodeScope(jjtn001);
    jjtreeOpenNodeScope(jjtn001);
      try {
        if (jj_2_379(3)) {
          jj_consume_token(TRUE);
        } else if (jj_2_380(3)) {
          jj_consume_token(FALSE);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } catch ( ...) {
if (jjtc001) {
      jjtree.clearNodeScope(jjtn001);
      jjtc001 = false;
    } else {
      jjtree.popNode();
    }
      }
if (jjtc001) {
      jjtree.closeNodeScope(jjtn001, true);
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn001);
      }
    }
    } else if (jj_2_382(3)) {
Unsupported *jjtn002 = new Unsupported(JJTUNSUPPORTED);
      bool jjtc002 = true;
      jjtree.openNodeScope(jjtn002);
      jjtreeOpenNodeScope(jjtn002);
      try {
        jj_consume_token(UNKNOWN);
      } catch ( ...) {
if (jjtc002) {
        jjtree.clearNodeScope(jjtn002);
        jjtc002 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc002) {
        jjtree.closeNodeScope(jjtn002, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn002);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::identifier() {/*@bgen(jjtree) Identifier */
  Identifier *jjtn000 = new Identifier(JJTIDENTIFIER);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_383(3)) {
        actual_identifier();
      } else if (jj_2_384(3)) {
        weird_identifiers();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
      if (jj_2_385(3)) {
        identifier_suffix_chain();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
      jjtree.clearNodeScope(jjtn000);
      jjtc000 = false;
    } else {
      jjtree.popNode();
    }
    }
if (jjtc000) {
      jjtree.closeNodeScope(jjtn000, true);
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn000);
      }
    }
}


void SqlParser::actual_identifier() {
    if (jj_2_386(3)) {
      jj_consume_token(regular_identifier);
    } else if (jj_2_387(3)) {
      jj_consume_token(delimited_identifier);
    } else if (jj_2_388(3)) {
      jj_consume_token(Unicode_delimited_identifier);
    } else if (jj_2_389(1) && (IsIdNonReservedWord())) {
      non_reserved_word();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::table_name() {/*@bgen(jjtree) TableName */
  TableName *jjtn000 = new TableName(JJTTABLENAME);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      identifier_chain();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::schema_name() {/*@bgen(jjtree) SchemaName */
  SchemaName *jjtn000 = new SchemaName(JJTSCHEMANAME);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      identifier_chain();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::catalog_name() {/*@bgen(jjtree) CatalogName */
  CatalogName *jjtn000 = new CatalogName(JJTCATALOGNAME);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      identifier();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::schema_qualified_name() {/*@bgen(jjtree) SchemaQualifiedName */
  SchemaQualifiedName *jjtn000 = new SchemaQualifiedName(JJTSCHEMAQUALIFIEDNAME);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      identifier_chain();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::local_or_schema_qualified_name() {
    identifier_chain();
}


void SqlParser::local_or_schema_qualifier() {
    if (jj_2_390(3)) {
      local_qualifier();
    } else if (jj_2_391(3)) {
      schema_name();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::cursor_name() {
    identifier_chain();
}


void SqlParser::local_qualifier() {
    jj_consume_token(MODULE);
}


void SqlParser::host_parameter_name() {
    jj_consume_token(568);
    identifier();
}


void SqlParser::external_routine_name() {
    if (jj_2_392(3)) {
      identifier();
    } else if (jj_2_393(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        character_string_literal();
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::character_set_name() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_394(3)) {
        schema_name();
        jj_consume_token(569);
      } else {
        ;
      }
      jj_consume_token(SQL_language_identifier);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::schema_resolved_user_defined_type_name() {
    user_defined_type_name();
}


void SqlParser::user_defined_type_name() {
    identifier_chain();
}


void SqlParser::SQL_identifier() {
    if (jj_2_395(3)) {
      identifier();
    } else if (jj_2_396(3)) {
      extended_identifier();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::extended_identifier() {
    if (jj_2_397(3)) {
      scope_option();
    } else {
      ;
    }
    simple_value_specification();
}


void SqlParser::dynamic_cursor_name() {
    if (jj_2_398(3)) {
      cursor_name();
    } else if (jj_2_399(3)) {
      extended_cursor_name();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::extended_cursor_name() {
    if (jj_2_400(3)) {
      scope_option();
    } else {
      ;
    }
    simple_value_specification();
}


void SqlParser::descriptor_name() {
    if (jj_2_401(3)) {
      identifier();
    } else if (jj_2_402(3)) {
      extended_descriptor_name();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::extended_descriptor_name() {
    if (jj_2_403(3)) {
      scope_option();
    } else {
      ;
    }
    simple_value_specification();
}


void SqlParser::scope_option() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_404(3)) {
        jj_consume_token(GLOBAL);
      } else if (jj_2_405(3)) {
        jj_consume_token(LOCAL);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::data_type() {
    if (jj_2_406(3)) {
      predefined_type();
    } else if (jj_2_407(3)) {
      row_type();
    } else if (jj_2_408(3)) {
      reference_type();
    } else if (jj_2_409(3)) {
      presto_generic_type();
    } else if (jj_2_410(3)) {
      path_resolved_user_defined_type_name();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
    if (jj_2_411(3)) {
      collection_type();
    } else {
      ;
    }
}


void SqlParser::predefined_type() {/*@bgen(jjtree) PredefinedType */
  PredefinedType *jjtn000 = new PredefinedType(JJTPREDEFINEDTYPE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_415(3)) {
        character_string_type();
        if (jj_2_412(3)) {
          jj_consume_token(CHARACTER);
          jj_consume_token(SET);
          character_set_specification();
        } else {
          ;
        }
        if (jj_2_413(3)) {
          collate_clause();
        } else {
          ;
        }
      } else if (jj_2_416(3)) {
        national_character_string_type();
        if (jj_2_414(3)) {
          collate_clause();
        } else {
          ;
        }
      } else if (jj_2_417(3)) {
        binary_string_type();
      } else if (jj_2_418(3)) {
        numeric_type();
      } else if (jj_2_419(3)) {
        boolean_type();
      } else if (jj_2_420(3)) {
        datetime_type();
      } else if (jj_2_421(3)) {
        interval_type();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::character_string_type() {
    if (jj_2_425(3)) {
      jj_consume_token(CHARACTER);
      if (jj_2_422(3)) {
        jj_consume_token(lparen);
        character_length();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_426(3)) {
      jj_consume_token(CHAR);
      if (jj_2_423(3)) {
        jj_consume_token(lparen);
        character_length();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_427(3)) {
      jj_consume_token(CHARACTER);
      jj_consume_token(VARYING);
      jj_consume_token(lparen);
      character_length();
      jj_consume_token(rparen);
    } else if (jj_2_428(3)) {
      jj_consume_token(CHAR);
      jj_consume_token(VARYING);
      jj_consume_token(lparen);
      character_length();
      jj_consume_token(rparen);
    } else if (jj_2_429(3)) {
      jj_consume_token(VARCHAR);
      if (jj_2_424(3)) {
        jj_consume_token(lparen);
        character_length();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_430(3)) {
      character_large_object_type();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::character_large_object_type() {
    if (jj_2_434(3)) {
      jj_consume_token(CHARACTER);
      jj_consume_token(LARGE);
      jj_consume_token(OBJECT);
      if (jj_2_431(3)) {
        jj_consume_token(lparen);
        character_large_object_length();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_435(3)) {
      jj_consume_token(CHAR);
      jj_consume_token(LARGE);
      jj_consume_token(OBJECT);
      if (jj_2_432(3)) {
        jj_consume_token(lparen);
        character_large_object_length();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_436(3)) {
      jj_consume_token(CLOB);
      if (jj_2_433(3)) {
        jj_consume_token(lparen);
        character_large_object_length();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::national_character_string_type() {
    if (jj_2_440(3)) {
      jj_consume_token(NATIONAL);
      jj_consume_token(CHARACTER);
      if (jj_2_437(3)) {
        jj_consume_token(lparen);
        character_length();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_441(3)) {
      jj_consume_token(NATIONAL);
      jj_consume_token(CHAR);
      if (jj_2_438(3)) {
        jj_consume_token(lparen);
        character_length();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_442(3)) {
      jj_consume_token(NCHAR);
      if (jj_2_439(3)) {
        jj_consume_token(lparen);
        character_length();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_443(3)) {
      jj_consume_token(NATIONAL);
      jj_consume_token(CHARACTER);
      jj_consume_token(VARYING);
      jj_consume_token(lparen);
      character_length();
      jj_consume_token(rparen);
    } else if (jj_2_444(3)) {
      jj_consume_token(NATIONAL);
      jj_consume_token(CHAR);
      jj_consume_token(VARYING);
      jj_consume_token(lparen);
      character_length();
      jj_consume_token(rparen);
    } else if (jj_2_445(3)) {
      jj_consume_token(NCHAR);
      jj_consume_token(VARYING);
      jj_consume_token(lparen);
      character_length();
      jj_consume_token(rparen);
    } else if (jj_2_446(3)) {
      national_character_large_object_type();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::national_character_large_object_type() {
    if (jj_2_450(3)) {
      jj_consume_token(NATIONAL);
      jj_consume_token(CHARACTER);
      jj_consume_token(LARGE);
      jj_consume_token(OBJECT);
      if (jj_2_447(3)) {
        jj_consume_token(lparen);
        character_large_object_length();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_451(3)) {
      jj_consume_token(NCHAR);
      jj_consume_token(LARGE);
      jj_consume_token(OBJECT);
      if (jj_2_448(3)) {
        jj_consume_token(lparen);
        character_large_object_length();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_452(3)) {
      jj_consume_token(NCLOB);
      if (jj_2_449(3)) {
        jj_consume_token(lparen);
        character_large_object_length();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::binary_string_type() {
    if (jj_2_454(3)) {
      jj_consume_token(BINARY);
      if (jj_2_453(3)) {
        jj_consume_token(lparen);
        jj_consume_token(unsigned_integer);
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_455(3)) {
      jj_consume_token(BINARY);
      jj_consume_token(VARYING);
      jj_consume_token(lparen);
      jj_consume_token(unsigned_integer);
      jj_consume_token(rparen);
    } else if (jj_2_456(3)) {
      jj_consume_token(VARBINARY);
      jj_consume_token(lparen);
      jj_consume_token(unsigned_integer);
      jj_consume_token(rparen);
    } else if (jj_2_457(3)) {
      varbinary();
    } else if (jj_2_458(3)) {
      binary_large_object_string_type();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::binary_large_object_string_type() {
    if (jj_2_461(3)) {
      jj_consume_token(BINARY);
      jj_consume_token(LARGE);
      jj_consume_token(OBJECT);
      if (jj_2_459(3)) {
        jj_consume_token(lparen);
        large_object_length();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_462(3)) {
      jj_consume_token(BLOB);
      if (jj_2_460(3)) {
        jj_consume_token(lparen);
        large_object_length();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::numeric_type() {
    if (jj_2_463(3)) {
      exact_numeric_type();
    } else if (jj_2_464(3)) {
      approximate_numeric_type();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::exact_numeric_type() {
    if (jj_2_471(3)) {
      jj_consume_token(NUMERIC);
      if (jj_2_466(3)) {
        jj_consume_token(lparen);
        jj_consume_token(unsigned_integer);
        if (jj_2_465(3)) {
          jj_consume_token(570);
          jj_consume_token(unsigned_integer);
        } else {
          ;
        }
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_472(3)) {
      jj_consume_token(DECIMAL);
      if (jj_2_468(3)) {
        jj_consume_token(lparen);
        jj_consume_token(unsigned_integer);
        if (jj_2_467(3)) {
          jj_consume_token(570);
          jj_consume_token(unsigned_integer);
        } else {
          ;
        }
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_473(3)) {
      jj_consume_token(DEC);
      if (jj_2_470(3)) {
        jj_consume_token(lparen);
        jj_consume_token(unsigned_integer);
        if (jj_2_469(3)) {
          jj_consume_token(570);
          jj_consume_token(unsigned_integer);
        } else {
          ;
        }
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_474(3)) {
      jj_consume_token(SMALLINT);
    } else if (jj_2_475(3)) {
      jj_consume_token(INTEGER);
    } else if (jj_2_476(3)) {
      jj_consume_token(INT);
    } else if (jj_2_477(3)) {
      jj_consume_token(BIGINT);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::approximate_numeric_type() {
    if (jj_2_480(3)) {
      jj_consume_token(FLOAT);
      if (jj_2_478(3)) {
        jj_consume_token(lparen);
        jj_consume_token(unsigned_integer);
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_481(3)) {
      jj_consume_token(REAL);
    } else if (jj_2_482(3)) {
      jj_consume_token(DOUBLE);
      if (jj_2_479(3)) {
        jj_consume_token(PRECISION);
      } else {
        ;
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::character_length() {
    jj_consume_token(unsigned_integer);
    if (jj_2_483(3)) {
      char_length_units();
    } else {
      ;
    }
}


void SqlParser::large_object_length() {
    if (jj_2_485(3)) {
      jj_consume_token(unsigned_integer);
      if (jj_2_484(3)) {
        jj_consume_token(multiplier);
      } else {
        ;
      }
    } else if (jj_2_486(3)) {
      jj_consume_token(large_object_length_token);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::character_large_object_length() {
    large_object_length();
    if (jj_2_487(3)) {
      char_length_units();
    } else {
      ;
    }
}


void SqlParser::char_length_units() {
    if (jj_2_488(3)) {
      jj_consume_token(CHARACTERS);
    } else if (jj_2_489(3)) {
      jj_consume_token(OCTETS);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::boolean_type() {
    jj_consume_token(BOOLEAN);
}


void SqlParser::datetime_type() {
    if (jj_2_494(3)) {
      jj_consume_token(DATE);
    } else if (jj_2_495(3)) {
      jj_consume_token(TIME);
      if (jj_2_490(3)) {
        jj_consume_token(lparen);
        jj_consume_token(unsigned_integer);
        jj_consume_token(rparen);
      } else {
        ;
      }
      if (jj_2_491(3)) {
        with_or_without_time_zone();
      } else {
        ;
      }
    } else if (jj_2_496(3)) {
      jj_consume_token(TIMESTAMP);
      if (jj_2_492(3)) {
        jj_consume_token(lparen);
        jj_consume_token(unsigned_integer);
        jj_consume_token(rparen);
      } else {
        ;
      }
      if (jj_2_493(3)) {
        with_or_without_time_zone();
      } else {
        ;
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::with_or_without_time_zone() {
    if (jj_2_497(3)) {
      jj_consume_token(WITH);
      jj_consume_token(TIME);
      jj_consume_token(ZONE);
    } else if (jj_2_498(3)) {
      jj_consume_token(WITHOUT);
      jj_consume_token(TIME);
      jj_consume_token(ZONE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::interval_type() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(INTERVAL);
      interval_qualifier();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::row_type() {/*@bgen(jjtree) RowType */
  RowType *jjtn000 = new RowType(JJTROWTYPE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(ROW);
      row_type_body();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::row_type_body() {
    jj_consume_token(lparen);
    field_definition();
    while (!hasError) {
      if (jj_2_499(3)) {
        ;
      } else {
        goto end_label_6;
      }
      jj_consume_token(570);
      field_definition();
    }
    end_label_6: ;
    jj_consume_token(rparen);
}


void SqlParser::reference_type() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(REF);
      jj_consume_token(lparen);
      referenced_type();
      jj_consume_token(rparen);
      if (jj_2_500(3)) {
        scope_clause();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::scope_clause() {
    jj_consume_token(SCOPE);
    table_name();
}


void SqlParser::referenced_type() {
    path_resolved_user_defined_type_name();
}


void SqlParser::path_resolved_user_defined_type_name() {
    user_defined_type_name();
}


void SqlParser::collection_type() {
    if (jj_2_501(3)) {
      array_type();
    } else if (jj_2_502(3)) {
      multiset_type();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::array_type() {/*@bgen(jjtree) #ArrayType(true) */
  ArrayType *jjtn000 = new ArrayType(JJTARRAYTYPE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
PushNode(PopNode());
      jj_consume_token(ARRAY);
      if (jj_2_503(3)) {
        left_bracket_or_trigraph();
        jj_consume_token(unsigned_integer);
        right_bracket_or_trigraph();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::multiset_type() {/*@bgen(jjtree) #Unsupported(true) */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
PushNode(PopNode());
      jj_consume_token(MULTISET);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::field_definition() {/*@bgen(jjtree) FieldDefinition */
  FieldDefinition *jjtn000 = new FieldDefinition(JJTFIELDDEFINITION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      identifier();
      data_type();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::value_expression_primary() {
    if (jj_2_504(3)) {
      parenthesized_value_expression();
    } else if (jj_2_505(3)) {
      nonparenthesized_value_expression_primary();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::parenthesized_value_expression() {
ParenthesizedExpression *jjtn001 = new ParenthesizedExpression(JJTPARENTHESIZEDEXPRESSION);
    bool jjtc001 = true;
    jjtree.openNodeScope(jjtn001);
    jjtreeOpenNodeScope(jjtn001);
    try {
      jj_consume_token(lparen);
      value_expression();
      while (!hasError) {
        if (jj_2_506(3)) {
          ;
        } else {
          goto end_label_7;
        }
        jj_consume_token(570);
        value_expression();
      }
      end_label_7: ;
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc001) {
      jjtree.clearNodeScope(jjtn001);
      jjtc001 = false;
    } else {
      jjtree.popNode();
    }
    }
if (jjtc001) {
      jjtree.closeNodeScope(jjtn001, true);
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn001);
      }
    }
    while (!hasError) {
      if (jj_2_507(3)) {
        ;
      } else {
        goto end_label_8;
      }
      primary_suffix();
    }
    end_label_8: ;
}


void SqlParser::nonparenthesized_value_expression_primary() {
    if (jj_2_523(3)) {
      contextually_typed_value_specification();
    } else if (jj_2_524(3)) {
      if (jj_2_508(3)) {
        set_function_specification();
      } else if (jj_2_509(3)) {
        subquery();
      } else if (jj_2_510(3)) {
        case_expression();
      } else if (jj_2_511(3)) {
        cast_specification();
      } else if (jj_2_512(3)) {
        subtype_treatment();
      } else if (jj_2_513(3)) {
        new_specification();
      } else if (jj_2_514(3)) {
        reference_resolution();
      } else if (jj_2_515(3)) {
        collection_value_constructor();
      } else if (jj_2_516(3)) {
        multiset_element_reference();
      } else if (jj_2_517(3)) {
        next_value_expression();
      } else if (jj_2_518(3)) {
        window_function_type();
      } else if (jj_2_519(3)) {
        jj_consume_token(lparen);
        column_name_list();
        jj_consume_token(rparen);
      } else if (jj_2_520(3)) {
        unsigned_value_specification();
      } else if (jj_2_521(3)) {
        column_reference();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
      while (!hasError) {
        if (jj_2_522(3)) {
          ;
        } else {
          goto end_label_9;
        }
        primary_suffix();
      }
      end_label_9: ;
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::primary_suffix() {
PushNode(PopNode());
    if (jj_2_525(3)) {
      field_reference();
    } else if (jj_2_526(3)) {
      attribute_or_method_reference();
    } else if (jj_2_527(3)) {
      method_invocation();
    } else if (jj_2_528(3)) {
      window_function();
    } else if (jj_2_529(3)) {
      array_element_reference();
    } else if (jj_2_530(3)) {
      static_method_invocation();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::collection_value_constructor() {
    if (jj_2_531(3)) {
      array_value_constructor();
    } else if (jj_2_532(3)) {
      multiset_value_constructor();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::value_specification() {
    if (jj_2_533(3)) {
      literal();
    } else if (jj_2_534(3)) {
      general_value_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::unsigned_value_specification() {
    if (jj_2_535(3)) {
      unsigned_literal();
    } else if (jj_2_536(3)) {
      general_value_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::general_value_specification() {
    if (jj_2_550(3)) {
      identifier_chain();
    } else if (jj_2_551(3)) {
BuiltinValue *jjtn001 = new BuiltinValue(JJTBUILTINVALUE);
    bool jjtc001 = true;
    jjtree.openNodeScope(jjtn001);
    jjtreeOpenNodeScope(jjtn001);
      try {
        if (jj_2_537(3)) {
          jj_consume_token(CURRENT_USER);
        } else if (jj_2_538(3)) {
          jj_consume_token(USER);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } catch ( ...) {
if (jjtc001) {
      jjtree.clearNodeScope(jjtn001);
      jjtc001 = false;
    } else {
      jjtree.popNode();
    }
      }
if (jjtc001) {
      jjtree.closeNodeScope(jjtn001, true);
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn001);
      }
    }
    } else if (jj_2_552(3)) {
Unsupported *jjtn002 = new Unsupported(JJTUNSUPPORTED);
    bool jjtc002 = true;
    jjtree.openNodeScope(jjtn002);
    jjtreeOpenNodeScope(jjtn002);
      try {
        if (jj_2_539(3)) {
          jj_consume_token(571);
        } else if (jj_2_540(3)) {
          current_collation_specification();
        } else if (jj_2_541(3)) {
          jj_consume_token(SESSION_USER);
        } else if (jj_2_542(3)) {
          jj_consume_token(SYSTEM_USER);
        } else if (jj_2_543(3)) {
          jj_consume_token(CURRENT_CATALOG);
        } else if (jj_2_544(3)) {
          jj_consume_token(CURRENT_PATH);
        } else if (jj_2_545(3)) {
          jj_consume_token(CURRENT_ROLE);
        } else if (jj_2_546(3)) {
          jj_consume_token(CURRENT_SCHEMA);
        } else if (jj_2_547(3)) {
          jj_consume_token(VALUE);
        } else if (jj_2_548(3)) {
          jj_consume_token(CURRENT_DEFAULT_TRANSFORM_GROUP);
        } else if (jj_2_549(3)) {
          jj_consume_token(CURRENT_TRANSFORM_GROUP_FOR_TYPE);
          path_resolved_user_defined_type_name();
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } catch ( ...) {
if (jjtc002) {
      jjtree.clearNodeScope(jjtn002);
      jjtc002 = false;
    } else {
      jjtree.popNode();
    }
      }
if (jjtc002) {
      jjtree.closeNodeScope(jjtn002, true);
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn002);
      }
    }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::simple_value_specification() {
    if (jj_2_553(3)) {
      literal();
    } else if (jj_2_554(3)) {
      identifier_chain();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::target_specification() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_555(3)) {
        identifier_chain();
      } else if (jj_2_556(3)) {
        column_reference();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
      if (jj_2_559(3)) {
        if (jj_2_557(3)) {
          target_array_element_specification();
        } else if (jj_2_558(3)) {
          jj_consume_token(571);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
      jjtree.clearNodeScope(jjtn000);
      jjtc000 = false;
    } else {
      jjtree.popNode();
    }
    }
if (jjtc000) {
      jjtree.closeNodeScope(jjtn000, true);
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn000);
      }
    }
}


void SqlParser::simple_target_specification() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_560(3)) {
        identifier_chain();
      } else if (jj_2_561(3)) {
        column_reference();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::target_array_element_specification() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      left_bracket_or_trigraph();
      simple_value_specification();
      right_bracket_or_trigraph();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::current_collation_specification() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(COLLATION);
      jj_consume_token(FOR);
      jj_consume_token(lparen);
      string_value_expression();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::contextually_typed_value_specification() {
    if (jj_2_562(3)) {
      implicitly_typed_value_specification();
    } else if (jj_2_563(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(DEFAULT_);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::implicitly_typed_value_specification() {
    if (jj_2_564(3)) {
NullLiteral *jjtn001 = new NullLiteral(JJTNULLLITERAL);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(NULL_);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else if (jj_2_565(3)) {
      empty_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::empty_specification() {
    if (jj_2_566(3)) {
ArrayLiteral *jjtn001 = new ArrayLiteral(JJTARRAYLITERAL);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(ARRAY);
        left_bracket_or_trigraph();
        right_bracket_or_trigraph();
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else if (jj_2_567(3)) {
Unsupported *jjtn002 = new Unsupported(JJTUNSUPPORTED);
      bool jjtc002 = true;
      jjtree.openNodeScope(jjtn002);
      jjtreeOpenNodeScope(jjtn002);
      try {
        jj_consume_token(MULTISET);
        left_bracket_or_trigraph();
        right_bracket_or_trigraph();
      } catch ( ...) {
if (jjtc002) {
        jjtree.clearNodeScope(jjtn002);
        jjtc002 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc002) {
        jjtree.closeNodeScope(jjtn002, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn002);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::identifier_chain() {/*@bgen(jjtree) #QualifiedName(> 1) */
  QualifiedName *jjtn000 = new QualifiedName(JJTQUALIFIEDNAME);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      identifier();
      while (!hasError) {
        if (jj_2_568(3)) {
          ;
        } else {
          goto end_label_10;
        }
        jj_consume_token(569);
        identifier();
      }
      end_label_10: ;
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::column_reference() {
    if (jj_2_569(3)) {
      identifier_chain();
    } else if (jj_2_570(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(MODULE);
        jj_consume_token(569);
        identifier();
        jj_consume_token(569);
        identifier();
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::set_function_specification() {
    if (jj_2_571(3)) {
      aggregate_function();
    } else if (jj_2_572(3)) {
      grouping_operation();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::grouping_operation() {/*@bgen(jjtree) GroupingOperation */
  GroupingOperation *jjtn000 = new GroupingOperation(JJTGROUPINGOPERATION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(GROUPING);
      jj_consume_token(lparen);
      column_reference();
      while (!hasError) {
        if (jj_2_573(3)) {
          ;
        } else {
          goto end_label_11;
        }
        jj_consume_token(570);
        column_reference();
      }
      end_label_11: ;
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::window_function() {/*@bgen(jjtree) #WindowFunction( 2) */
  WindowFunction *jjtn000 = new WindowFunction(JJTWINDOWFUNCTION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(OVER);
      window_name_or_specification();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000,  2);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::window_function_type() {
    if (jj_2_574(3)) {
      rank_function_type();
      jj_consume_token(lparen);
      jj_consume_token(rparen);
    } else if (jj_2_575(3)) {
RowNumber *jjtn001 = new RowNumber(JJTROWNUMBER);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(ROW_NUMBER);
        jj_consume_token(lparen);
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else if (jj_2_576(3)) {
      aggregate_function();
    } else if (jj_2_577(3)) {
      ntile_function();
    } else if (jj_2_578(3)) {
      lead_or_lag_function();
    } else if (jj_2_579(3)) {
      first_or_last_value_function();
    } else if (jj_2_580(3)) {
      nth_value_function();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::rank_function_type() {/*@bgen(jjtree) RankFunction */
  RankFunction *jjtn000 = new RankFunction(JJTRANKFUNCTION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_581(3)) {
        jj_consume_token(RANK);
      } else if (jj_2_582(3)) {
        jj_consume_token(DENSE_RANK);
      } else if (jj_2_583(3)) {
        jj_consume_token(PERCENT_RANK);
      } else if (jj_2_584(3)) {
        jj_consume_token(CUME_DIST);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::ntile_function() {/*@bgen(jjtree) NtileFunction */
  NtileFunction *jjtn000 = new NtileFunction(JJTNTILEFUNCTION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(NTILE);
      jj_consume_token(lparen);
      number_of_tiles();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::number_of_tiles() {
    if (jj_2_585(3)) {
      value_expression();
    } else if (jj_2_586(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(571);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::lead_or_lag_function() {/*@bgen(jjtree) LeadOrLag */
  LeadOrLag *jjtn000 = new LeadOrLag(JJTLEADORLAG);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      lead_or_lag();
      jj_consume_token(lparen);
      value_expression();
      if (jj_2_588(3)) {
        jj_consume_token(570);
        value_expression();
        if (jj_2_587(3)) {
          jj_consume_token(570);
          value_expression();
        } else {
          ;
        }
      } else {
        ;
      }
      jj_consume_token(rparen);
      if (jj_2_589(3)) {
        null_treatment();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::lead_or_lag() {
    if (jj_2_590(3)) {
      jj_consume_token(LEAD);
    } else if (jj_2_591(3)) {
      jj_consume_token(LAG);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::null_treatment() {/*@bgen(jjtree) NullTreatment */
  NullTreatment *jjtn000 = new NullTreatment(JJTNULLTREATMENT);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_592(3)) {
        jj_consume_token(RESPECT);
        jj_consume_token(NULLS);
      } else if (jj_2_593(3)) {
        jj_consume_token(IGNORE);
        jj_consume_token(NULLS);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::first_or_last_value_function() {/*@bgen(jjtree) FirstOrLastValueFunction */
  FirstOrLastValueFunction *jjtn000 = new FirstOrLastValueFunction(JJTFIRSTORLASTVALUEFUNCTION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      first_or_last_value();
      jj_consume_token(lparen);
      value_expression();
      jj_consume_token(rparen);
      if (jj_2_594(3)) {
        null_treatment();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::first_or_last_value() {
    if (jj_2_595(3)) {
      jj_consume_token(FIRST_VALUE);
    } else if (jj_2_596(3)) {
      jj_consume_token(LAST_VALUE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::nth_value_function() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(NTH_VALUE);
      jj_consume_token(lparen);
      value_expression();
      jj_consume_token(570);
      nth_row();
      jj_consume_token(rparen);
      if (jj_2_597(3)) {
        from_first_or_last();
      } else {
        ;
      }
      if (jj_2_598(3)) {
        null_treatment();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::nth_row() {
    if (jj_2_599(3)) {
      simple_value_specification();
    } else if (jj_2_600(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(571);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::from_first_or_last() {
    if (jj_2_601(3)) {
      jj_consume_token(FROM);
      jj_consume_token(FIRST);
    } else if (jj_2_602(3)) {
      jj_consume_token(FROM);
      jj_consume_token(LAST);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::window_name_or_specification() {
    if (jj_2_603(3)) {
      in_line_window_specification();
    } else if (jj_2_604(3)) {
      identifier();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::in_line_window_specification() {
    window_specification();
}


void SqlParser::case_expression() {
    if (jj_2_605(3)) {
      case_abbreviation();
    } else if (jj_2_606(3)) {
      case_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::case_abbreviation() {
    if (jj_2_608(3)) {
NullIf *jjtn001 = new NullIf(JJTNULLIF);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(NULLIF);
        jj_consume_token(lparen);
        value_expression();
        jj_consume_token(570);
        value_expression();
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else if (jj_2_609(3)) {
Coalesce *jjtn002 = new Coalesce(JJTCOALESCE);
      bool jjtc002 = true;
      jjtree.openNodeScope(jjtn002);
      jjtreeOpenNodeScope(jjtn002);
      try {
        jj_consume_token(COALESCE);
        jj_consume_token(lparen);
        value_expression();
        while (!hasError) {
          jj_consume_token(570);
          value_expression();
          if (jj_2_607(3)) {
            ;
          } else {
            goto end_label_12;
          }
        }
        end_label_12: ;
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc002) {
        jjtree.clearNodeScope(jjtn002);
        jjtc002 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc002) {
        jjtree.closeNodeScope(jjtn002, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn002);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::case_specification() {
    if (jj_2_610(3)) {
      simple_case();
    } else if (jj_2_611(3)) {
      searched_case();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::simple_case() {/*@bgen(jjtree) CaseExpression */
  CaseExpression *jjtn000 = new CaseExpression(JJTCASEEXPRESSION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(CASE);
      case_operand();
      while (!hasError) {
        simple_when_clause();
        if (jj_2_612(3)) {
          ;
        } else {
          goto end_label_13;
        }
      }
      end_label_13: ;
      if (jj_2_613(3)) {
        else_clause();
      } else {
        ;
      }
      jj_consume_token(END);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::searched_case() {/*@bgen(jjtree) CaseExpression */
  CaseExpression *jjtn000 = new CaseExpression(JJTCASEEXPRESSION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(CASE);
      while (!hasError) {
        searched_when_clause();
        if (jj_2_614(3)) {
          ;
        } else {
          goto end_label_14;
        }
      }
      end_label_14: ;
      if (jj_2_615(3)) {
        else_clause();
      } else {
        ;
      }
      jj_consume_token(END);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::simple_when_clause() {/*@bgen(jjtree) WhenClause */
  WhenClause *jjtn000 = new WhenClause(JJTWHENCLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(WHEN);
      when_operand_list();
      jj_consume_token(THEN);
      result();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::searched_when_clause() {/*@bgen(jjtree) WhenClause */
  WhenClause *jjtn000 = new WhenClause(JJTWHENCLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(WHEN);
      search_condition();
      jj_consume_token(THEN);
      result();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::else_clause() {/*@bgen(jjtree) ElseClause */
  ElseClause *jjtn000 = new ElseClause(JJTELSECLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(ELSE);
      result();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::case_operand() {
    if (jj_2_616(3)) {
      row_value_predicand();
    } else if (jj_2_617(3)) {
      overlaps_predicate_part_1();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::when_operand_list() {
    when_operand();
    while (!hasError) {
      if (jj_2_618(3)) {
        ;
      } else {
        goto end_label_15;
      }
      jj_consume_token(570);
      when_operand();
    }
    end_label_15: ;
}


void SqlParser::when_operand() {/*@bgen(jjtree) WhenOperand */
  WhenOperand *jjtn000 = new WhenOperand(JJTWHENOPERAND);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
SearchedCaseOperand *jjtn001 = new SearchedCaseOperand(JJTSEARCHEDCASEOPERAND);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
jjtree.closeNodeScope(jjtn001,  true);
        jjtc001 = false;
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }

      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001,  true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
      if (jj_2_619(3)) {
        row_value_predicand();
      } else if (jj_2_620(3)) {
        comparison_predicate_part_2();
      } else if (jj_2_621(3)) {
        between_predicate_part_2();
      } else if (jj_2_622(3)) {
        in_predicate_part_2();
      } else if (jj_2_623(3)) {
        character_like_predicate_part_2();
      } else if (jj_2_624(3)) {
        octet_like_predicate_part_2();
      } else if (jj_2_625(3)) {
        similar_predicate_part_2();
      } else if (jj_2_626(3)) {
        regex_like_predicate_part_2();
      } else if (jj_2_627(3)) {
        null_predicate_part_2();
      } else if (jj_2_628(3)) {
        quantified_comparison_predicate_part_2();
      } else if (jj_2_629(3)) {
        normalized_predicate_part_2();
      } else if (jj_2_630(3)) {
        match_predicate_part_2();
      } else if (jj_2_631(3)) {
        overlaps_predicate_part_2();
      } else if (jj_2_632(3)) {
        distinct_predicate_part_2();
      } else if (jj_2_633(3)) {
        member_predicate_part_2();
      } else if (jj_2_634(3)) {
        submultiset_predicate_part_2();
      } else if (jj_2_635(3)) {
        set_predicate_part_2();
      } else if (jj_2_636(3)) {
        type_predicate_part_2();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::result() {
    if (jj_2_637(3)) {
      value_expression();
    } else if (jj_2_638(3)) {
NullLiteral *jjtn001 = new NullLiteral(JJTNULLLITERAL);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(NULL_);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::cast_specification() {/*@bgen(jjtree) CastExpression */
  CastExpression *jjtn000 = new CastExpression(JJTCASTEXPRESSION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_639(3)) {
        jj_consume_token(CAST);
        jj_consume_token(lparen);
        cast_operand();
        jj_consume_token(AS);
        cast_target();
        jj_consume_token(rparen);
      } else if (jj_2_640(3)) {
        try_cast();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::cast_operand() {
    if (jj_2_641(3)) {
      value_expression();
    } else if (jj_2_642(3)) {
      implicitly_typed_value_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::cast_target() {
    if (jj_2_643(3)) {
      data_type();
    } else if (jj_2_644(3)) {
      schema_qualified_name();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::next_value_expression() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(NEXT);
      jj_consume_token(VALUE);
      jj_consume_token(FOR);
      schema_qualified_name();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::field_reference() {/*@bgen(jjtree) #FieldReference( 2) */
  FieldReference *jjtn000 = new FieldReference(JJTFIELDREFERENCE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(569);
      identifier();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000,  2);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::subtype_treatment() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(TREAT);
      jj_consume_token(lparen);
      value_expression();
      jj_consume_token(AS);
      target_subtype();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::target_subtype() {
    if (jj_2_645(3)) {
      path_resolved_user_defined_type_name();
    } else if (jj_2_646(3)) {
      reference_type();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::method_invocation() {
    if (jj_2_648(3)) {
FunctionCall *jjtn001 = new FunctionCall(JJTFUNCTIONCALL);
         bool jjtc001 = true;
         jjtree.openNodeScope(jjtn001);
         jjtreeOpenNodeScope(jjtn001);
      try {
        direct_invocation();
      } catch ( ...) {
if (jjtc001) {
           jjtree.clearNodeScope(jjtn001);
           jjtc001 = false;
         } else {
           jjtree.popNode();
         }
      }
if (jjtc001) {
           jjtree.closeNodeScope(jjtn001,  2);
           if (jjtree.nodeCreated()) {
            jjtreeCloseNodeScope(jjtn001);
           }
         }
      if (jj_2_647(3)) {
PushNode(PopNode());
AggregationFunction *jjtn002 = new AggregationFunction(JJTAGGREGATIONFUNCTION);
                                                                           bool jjtc002 = true;
                                                                           jjtree.openNodeScope(jjtn002);
                                                                           jjtreeOpenNodeScope(jjtn002);
        try {
          udaf_filter();
        } catch ( ...) {
if (jjtc002) {
                                                                             jjtree.clearNodeScope(jjtn002);
                                                                             jjtc002 = false;
                                                                           } else {
                                                                             jjtree.popNode();
                                                                           }
        }
if (jjtc002) {
                                                                             jjtree.closeNodeScope(jjtn002,  2);
                                                                             if (jjtree.nodeCreated()) {
                                                                              jjtreeCloseNodeScope(jjtn002);
                                                                             }
                                                                           }
      } else {
        ;
      }
    } else if (jj_2_649(3)) {
      generalized_invocation();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::direct_invocation() {
    SQL_argument_list();
}


void SqlParser::generalized_invocation() {/*@bgen(jjtree) #FunctionCall( 2) */
  FunctionCall *jjtn000 = new FunctionCall(JJTFUNCTIONCALL);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
QualifiedName *jjtn001 = new QualifiedName(JJTQUALIFIEDNAME);
       bool jjtc001 = true;
       jjtree.openNodeScope(jjtn001);
       jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(569);
        identifier();
      } catch ( ...) {
if (jjtc001) {
         jjtree.clearNodeScope(jjtn001);
         jjtc001 = false;
       } else {
         jjtree.popNode();
       }
      }
if (jjtc001) {
         jjtree.closeNodeScope(jjtn001,  2);
         if (jjtree.nodeCreated()) {
          jjtreeCloseNodeScope(jjtn001);
         }
       }
      if (jj_2_650(3)) {
        SQL_argument_list();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
         jjtree.clearNodeScope(jjtn000);
         jjtc000 = false;
       } else {
         jjtree.popNode();
       }
    }
if (jjtc000) {
         jjtree.closeNodeScope(jjtn000,  2);
         if (jjtree.nodeCreated()) {
          jjtreeCloseNodeScope(jjtn000);
         }
       }
}


void SqlParser::static_method_invocation() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(572);
      identifier();
      if (jj_2_651(3)) {
        SQL_argument_list();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::new_specification() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(NEW);
      path_resolved_user_defined_type_name();
      SQL_argument_list();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::new_invocation() {/*@bgen(jjtree) Unused */
  Unused *jjtn000 = new Unused(JJTUNUSED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_652(3)) {
        method_invocation();
      } else if (jj_2_653(3)) {
        routine_invocation();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::attribute_or_method_reference() {
Lambda *jjtn001 = new Lambda(JJTLAMBDA);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
    try {
      lambda_body();
    } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001,  2);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
}


void SqlParser::dereference_operation() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      reference_value_expression();
      jj_consume_token(573);
      identifier();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::reference_resolution() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(DEREF);
      jj_consume_token(lparen);
      reference_value_expression();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::array_element_reference() {/*@bgen(jjtree) #ArrayElement( 2) */
  ArrayElement *jjtn000 = new ArrayElement(JJTARRAYELEMENT);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      left_bracket_or_trigraph();
      value_expression();
      right_bracket_or_trigraph();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000,  2);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::multiset_element_reference() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(ELEMENT);
      jj_consume_token(lparen);
      multiset_value_expression();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::value_expression() {
    if (jj_2_654(3)) {
      boolean_value_expression();
    } else if (jj_2_655(3)) {
      common_value_expression();
    } else if (jj_2_656(3)) {
      row_value_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::common_value_expression() {
    if (jj_2_657(3)) {
      numeric_value_expression();
    } else if (jj_2_658(3)) {
      string_value_expression();
    } else if (jj_2_659(3)) {
      datetime_value_expression();
    } else if (jj_2_660(3)) {
      interval_value_expression();
    } else if (jj_2_661(3)) {
      user_defined_type_value_expression();
    } else if (jj_2_662(3)) {
      reference_value_expression();
    } else if (jj_2_663(3)) {
      collection_value_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::user_defined_type_value_expression() {
    value_expression_primary();
}


void SqlParser::reference_value_expression() {
    value_expression_primary();
}


void SqlParser::collection_value_expression() {
    if (jj_2_664(3)) {
      array_value_expression();
    } else if (jj_2_665(3)) {
      multiset_value_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::numeric_value_expression() {
    term();
    while (!hasError) {
      if (jj_2_666(3)) {
        ;
      } else {
        goto end_label_16;
      }
      if (jj_2_667(3)) {
PushNode(PopNode());
        jj_consume_token(PLUS);
AdditiveExpression *jjtn001 = new AdditiveExpression(JJTADDITIVEEXPRESSION);
                                              bool jjtc001 = true;
                                              jjtree.openNodeScope(jjtn001);
                                              jjtreeOpenNodeScope(jjtn001);
        try {
          term();
        } catch ( ...) {
if (jjtc001) {
                                                jjtree.clearNodeScope(jjtn001);
                                                jjtc001 = false;
                                              } else {
                                                jjtree.popNode();
                                              }
        }
if (jjtc001) {
                                                jjtree.closeNodeScope(jjtn001,  2);
                                                if (jjtree.nodeCreated()) {
                                                 jjtreeCloseNodeScope(jjtn001);
                                                }
                                              }
      } else if (jj_2_668(3)) {
PushNode(PopNode());
        jj_consume_token(MINUS);
AdditiveExpression *jjtn002 = new AdditiveExpression(JJTADDITIVEEXPRESSION);
                                               bool jjtc002 = true;
                                               jjtree.openNodeScope(jjtn002);
                                               jjtreeOpenNodeScope(jjtn002);
        try {
          term();
        } catch ( ...) {
if (jjtc002) {
                                                 jjtree.clearNodeScope(jjtn002);
                                                 jjtc002 = false;
                                               } else {
                                                 jjtree.popNode();
                                               }
        }
if (jjtc002) {
                                                 jjtree.closeNodeScope(jjtn002,  2);
                                                 if (jjtree.nodeCreated()) {
                                                  jjtreeCloseNodeScope(jjtn002);
                                                 }
                                               }
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    }
    end_label_16: ;
}


void SqlParser::term() {
    factor();
    while (!hasError) {
      if (jj_2_669(3)) {
        ;
      } else {
        goto end_label_17;
      }
      if (jj_2_670(3)) {
PushNode(PopNode());
        jj_consume_token(STAR);
MultiplicativeExpression *jjtn001 = new MultiplicativeExpression(JJTMULTIPLICATIVEEXPRESSION);
                                             bool jjtc001 = true;
                                             jjtree.openNodeScope(jjtn001);
                                             jjtreeOpenNodeScope(jjtn001);
        try {
          factor();
        } catch ( ...) {
if (jjtc001) {
                                               jjtree.clearNodeScope(jjtn001);
                                               jjtc001 = false;
                                             } else {
                                               jjtree.popNode();
                                             }
        }
if (jjtc001) {
                                               jjtree.closeNodeScope(jjtn001,  2);
                                               if (jjtree.nodeCreated()) {
                                                jjtreeCloseNodeScope(jjtn001);
                                               }
                                             }
      } else if (jj_2_671(3)) {
PushNode(PopNode());
        jj_consume_token(DIV);
MultiplicativeExpression *jjtn002 = new MultiplicativeExpression(JJTMULTIPLICATIVEEXPRESSION);
                                            bool jjtc002 = true;
                                            jjtree.openNodeScope(jjtn002);
                                            jjtreeOpenNodeScope(jjtn002);
        try {
          factor();
        } catch ( ...) {
if (jjtc002) {
                                              jjtree.clearNodeScope(jjtn002);
                                              jjtc002 = false;
                                            } else {
                                              jjtree.popNode();
                                            }
        }
if (jjtc002) {
                                              jjtree.closeNodeScope(jjtn002,  2);
                                              if (jjtree.nodeCreated()) {
                                               jjtreeCloseNodeScope(jjtn002);
                                              }
                                            }
      } else if (jj_2_672(3)) {
PushNode(PopNode());
        percent_operator();
MultiplicativeExpression *jjtn003 = new MultiplicativeExpression(JJTMULTIPLICATIVEEXPRESSION);
                                                    bool jjtc003 = true;
                                                    jjtree.openNodeScope(jjtn003);
                                                    jjtreeOpenNodeScope(jjtn003);
        try {
          factor();
        } catch ( ...) {
if (jjtc003) {
                                                      jjtree.clearNodeScope(jjtn003);
                                                      jjtc003 = false;
                                                    } else {
                                                      jjtree.popNode();
                                                    }
        }
if (jjtc003) {
                                                      jjtree.closeNodeScope(jjtn003,  2);
                                                      if (jjtree.nodeCreated()) {
                                                       jjtreeCloseNodeScope(jjtn003);
                                                      }
                                                    }
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    }
    end_label_17: ;
}


void SqlParser::factor() {
    if (jj_2_675(3)) {
UnaryExpression *jjtn001 = new UnaryExpression(JJTUNARYEXPRESSION);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        if (jj_2_673(3)) {
          jj_consume_token(PLUS);
        } else if (jj_2_674(3)) {
          jj_consume_token(MINUS);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
        numeric_primary();
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001,  1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else if (jj_2_676(3)) {
      numeric_primary();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::numeric_primary() {
    if (jj_2_677(3)) {
      numeric_value_function();
    } else if (jj_2_678(3)) {
      character_value_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::numeric_value_function() {/*@bgen(jjtree) BuiltinFunctionCall */
  BuiltinFunctionCall *jjtn000 = new BuiltinFunctionCall(JJTBUILTINFUNCTIONCALL);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
ArgumentList *jjtn001 = new ArgumentList(JJTARGUMENTLIST);
    bool jjtc001 = true;
    jjtree.openNodeScope(jjtn001);
    jjtreeOpenNodeScope(jjtn001);
      try {
        if (jj_2_679(3)) {
          position_expression();
        } else if (jj_2_680(3)) {
          regex_occurrences_function();
        } else if (jj_2_681(3)) {
          regex_position_expression();
        } else if (jj_2_682(3)) {
          extract_expression();
        } else if (jj_2_683(3)) {
          length_expression();
        } else if (jj_2_684(3)) {
          cardinality_expression();
        } else if (jj_2_685(3)) {
          max_cardinality_expression();
        } else if (jj_2_686(3)) {
          absolute_value_expression();
        } else if (jj_2_687(3)) {
          modulus_expression();
        } else if (jj_2_688(3)) {
          natural_logarithm();
        } else if (jj_2_689(3)) {
          exponential_function();
        } else if (jj_2_690(3)) {
          power_function();
        } else if (jj_2_691(3)) {
          square_root();
        } else if (jj_2_692(3)) {
          floor_function();
        } else if (jj_2_693(3)) {
          ceiling_function();
        } else if (jj_2_694(3)) {
          width_bucket_function();
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } catch ( ...) {
if (jjtc001) {
      jjtree.clearNodeScope(jjtn001);
      jjtc001 = false;
    } else {
      jjtree.popNode();
    }
      }
if (jjtc001) {
      jjtree.closeNodeScope(jjtn001, jjtree.nodeArity() > 0);
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn001);
      }
    }
    } catch ( ...) {
if (jjtc000) {
      jjtree.clearNodeScope(jjtn000);
      jjtc000 = false;
    } else {
      jjtree.popNode();
    }
    }
if (jjtc000) {
      jjtree.closeNodeScope(jjtn000, true);
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn000);
      }
    }
}


void SqlParser::position_expression() {
    if (jj_2_695(3)) {
      character_position_expression();
    } else if (jj_2_696(3)) {
      binary_position_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::regex_occurrences_function() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(OCCURRENCES_REGEX);
      jj_consume_token(lparen);
      character_value_expression();
      if (jj_2_697(3)) {
        jj_consume_token(FLAG);
        character_value_expression();
      } else {
        ;
      }
      jj_consume_token(IN);
      character_value_expression();
      if (jj_2_698(3)) {
        jj_consume_token(FROM);
        numeric_value_expression();
      } else {
        ;
      }
      if (jj_2_699(3)) {
        jj_consume_token(USING);
        char_length_units();
      } else {
        ;
      }
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::regex_position_expression() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(POSITION_REGEX);
      jj_consume_token(lparen);
      if (jj_2_700(3)) {
        regex_position_start_or_after();
      } else {
        ;
      }
      character_value_expression();
      if (jj_2_701(3)) {
        jj_consume_token(FLAG);
        character_value_expression();
      } else {
        ;
      }
      jj_consume_token(IN);
      character_value_expression();
      if (jj_2_702(3)) {
        jj_consume_token(FROM);
        numeric_value_expression();
      } else {
        ;
      }
      if (jj_2_703(3)) {
        jj_consume_token(USING);
        char_length_units();
      } else {
        ;
      }
      if (jj_2_704(3)) {
        jj_consume_token(OCCURRENCE);
        numeric_value_expression();
      } else {
        ;
      }
      if (jj_2_705(3)) {
        jj_consume_token(GROUP);
        numeric_value_expression();
      } else {
        ;
      }
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::regex_position_start_or_after() {
    if (jj_2_706(3)) {
      jj_consume_token(START);
    } else if (jj_2_707(3)) {
      jj_consume_token(AFTER);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::character_position_expression() {
    jj_consume_token(POSITION);
    jj_consume_token(lparen);
    character_value_expression();
    jj_consume_token(IN);
    character_value_expression();
    if (jj_2_708(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
        bool jjtc001 = true;
        jjtree.openNodeScope(jjtn001);
        jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(USING);
        char_length_units();
      } catch ( ...) {
if (jjtc001) {
          jjtree.clearNodeScope(jjtn001);
          jjtc001 = false;
        } else {
          jjtree.popNode();
        }
      }
if (jjtc001) {
          jjtree.closeNodeScope(jjtn001, true);
          if (jjtree.nodeCreated()) {
           jjtreeCloseNodeScope(jjtn001);
          }
        }
    } else {
      ;
    }
    jj_consume_token(rparen);
}


void SqlParser::binary_position_expression() {
    jj_consume_token(POSITION);
    jj_consume_token(lparen);
    binary_value_expression();
    jj_consume_token(IN);
    binary_value_expression();
    jj_consume_token(rparen);
}


void SqlParser::length_expression() {
    if (jj_2_709(3)) {
      char_length_expression();
    } else if (jj_2_710(3)) {
      octet_length_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::char_length_expression() {
    if (jj_2_711(3)) {
      jj_consume_token(CHAR_LENGTH);
    } else if (jj_2_712(3)) {
      jj_consume_token(CHARACTER_LENGTH);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
    jj_consume_token(lparen);
    character_value_expression();
    if (jj_2_713(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
        bool jjtc001 = true;
        jjtree.openNodeScope(jjtn001);
        jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(USING);
        char_length_units();
      } catch ( ...) {
if (jjtc001) {
          jjtree.clearNodeScope(jjtn001);
          jjtc001 = false;
        } else {
          jjtree.popNode();
        }
      }
if (jjtc001) {
          jjtree.closeNodeScope(jjtn001, true);
          if (jjtree.nodeCreated()) {
           jjtreeCloseNodeScope(jjtn001);
          }
        }
    } else {
      ;
    }
    jj_consume_token(rparen);
}


void SqlParser::octet_length_expression() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(OCTET_LENGTH);
      jj_consume_token(lparen);
      string_value_expression();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::extract_expression() {
    jj_consume_token(EXTRACT);
    jj_consume_token(lparen);
    extract_field();
    jj_consume_token(FROM);
    extract_source();
    jj_consume_token(rparen);
}


void SqlParser::extract_field() {
    if (jj_2_714(3)) {
      primary_datetime_field();
    } else if (jj_2_715(3)) {
      time_zone_field();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::time_zone_field() {/*@bgen(jjtree) TimeZoneField */
  TimeZoneField *jjtn000 = new TimeZoneField(JJTTIMEZONEFIELD);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_716(3)) {
        jj_consume_token(TIMEZONE_HOUR);
      } else if (jj_2_717(3)) {
        jj_consume_token(TIMEZONE_MINUTE);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::extract_source() {
    if (jj_2_718(3)) {
      datetime_value_expression();
    } else if (jj_2_719(3)) {
      interval_value_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::cardinality_expression() {
    jj_consume_token(CARDINALITY);
    jj_consume_token(lparen);
    collection_value_expression();
    jj_consume_token(rparen);
}


void SqlParser::max_cardinality_expression() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(MAX_CARDINALITY);
      jj_consume_token(lparen);
      array_value_expression();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::absolute_value_expression() {
    jj_consume_token(ABS);
    jj_consume_token(lparen);
    numeric_value_expression();
    jj_consume_token(rparen);
}


void SqlParser::modulus_expression() {
    jj_consume_token(MOD);
    jj_consume_token(lparen);
    numeric_value_expression();
    jj_consume_token(570);
    numeric_value_expression();
    jj_consume_token(rparen);
}


void SqlParser::natural_logarithm() {
    jj_consume_token(LN);
    jj_consume_token(lparen);
    numeric_value_expression();
    jj_consume_token(rparen);
}


void SqlParser::exponential_function() {
    jj_consume_token(EXP);
    jj_consume_token(lparen);
    numeric_value_expression();
    jj_consume_token(rparen);
}


void SqlParser::power_function() {
    jj_consume_token(POWER);
    jj_consume_token(lparen);
    numeric_value_expression();
    jj_consume_token(570);
    numeric_value_expression();
    jj_consume_token(rparen);
}


void SqlParser::square_root() {
    jj_consume_token(SQRT);
    jj_consume_token(lparen);
    numeric_value_expression();
    jj_consume_token(rparen);
}


void SqlParser::floor_function() {
    jj_consume_token(FLOOR);
    jj_consume_token(lparen);
    numeric_value_expression();
    jj_consume_token(rparen);
}


void SqlParser::ceiling_function() {
    if (jj_2_720(3)) {
      jj_consume_token(CEIL);
    } else if (jj_2_721(3)) {
      jj_consume_token(CEILING);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
    jj_consume_token(lparen);
    numeric_value_expression();
    jj_consume_token(rparen);
}


void SqlParser::width_bucket_function() {
    jj_consume_token(WIDTH_BUCKET);
    jj_consume_token(lparen);
    numeric_value_expression();
    jj_consume_token(570);
    numeric_value_expression();
    if (jj_2_722(3)) {
      jj_consume_token(570);
      numeric_value_expression();
      jj_consume_token(570);
      numeric_value_expression();
    } else {
      ;
    }
    jj_consume_token(rparen);
}


void SqlParser::string_value_expression() {
    if (jj_2_723(3)) {
      character_value_expression();
    } else if (jj_2_724(3)) {
      binary_value_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::character_value_expression() {/*@bgen(jjtree) #Concatenation(> 1) */
  Concatenation *jjtn000 = new Concatenation(JJTCONCATENATION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      character_factor();
      while (!hasError) {
        if (jj_2_725(3)) {
          ;
        } else {
          goto end_label_18;
        }
        concatenation();
      }
      end_label_18: ;
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::concatenation() {
    jj_consume_token(576);
    character_factor();
}


void SqlParser::character_factor() {
    character_primary();
    if (jj_2_726(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
                            bool jjtc001 = true;
                            jjtree.openNodeScope(jjtn001);
                            jjtreeOpenNodeScope(jjtn001);
      try {
        collate_clause();
      } catch ( ...) {
if (jjtc001) {
                              jjtree.clearNodeScope(jjtn001);
                              jjtc001 = false;
                            } else {
                              jjtree.popNode();
                            }
      }
if (jjtc001) {
                              jjtree.closeNodeScope(jjtn001, true);
                              if (jjtree.nodeCreated()) {
                               jjtreeCloseNodeScope(jjtn001);
                              }
                            }
    } else {
      ;
    }
}


void SqlParser::character_primary() {
    if (jj_2_727(3)) {
      string_value_function();
    } else if (jj_2_728(3)) {
      binary_value_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::binary_value_expression() {/*@bgen(jjtree) #Concatenation(> 1) */
  Concatenation *jjtn000 = new Concatenation(JJTCONCATENATION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      binary_primary();
      while (!hasError) {
        if (jj_2_729(3)) {
          ;
        } else {
          goto end_label_19;
        }
        binary_concatenation();
      }
      end_label_19: ;
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::binary_primary() {
    if (jj_2_730(3)) {
      string_value_function();
    } else if (jj_2_731(3)) {
      datetime_value_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::binary_concatenation() {
    jj_consume_token(576);
    binary_primary();
}


void SqlParser::string_value_function() {
    if (jj_2_732(3)) {
      character_value_function();
    } else if (jj_2_733(3)) {
      binary_value_function();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::character_value_function() {/*@bgen(jjtree) BuiltinFunctionCall */
  BuiltinFunctionCall *jjtn000 = new BuiltinFunctionCall(JJTBUILTINFUNCTIONCALL);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
ArgumentList *jjtn001 = new ArgumentList(JJTARGUMENTLIST);
    bool jjtc001 = true;
    jjtree.openNodeScope(jjtn001);
    jjtreeOpenNodeScope(jjtn001);
      try {
        if (jj_2_734(3)) {
          character_substring_function();
        } else if (jj_2_735(3)) {
          regular_expression_substring_function();
        } else if (jj_2_736(3)) {
          regex_substring_function();
        } else if (jj_2_737(3)) {
          fold();
        } else if (jj_2_738(3)) {
          transcoding();
        } else if (jj_2_739(3)) {
          character_transliteration();
        } else if (jj_2_740(3)) {
          regex_transliteration();
        } else if (jj_2_741(3)) {
          trim_function();
        } else if (jj_2_742(3)) {
          character_overlay_function();
        } else if (jj_2_743(3)) {
          normalize_function();
        } else if (jj_2_744(3)) {
          specific_type_method();
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } catch ( ...) {
if (jjtc001) {
      jjtree.clearNodeScope(jjtn001);
      jjtc001 = false;
    } else {
      jjtree.popNode();
    }
      }
if (jjtc001) {
      jjtree.closeNodeScope(jjtn001, jjtree.nodeArity() > 0);
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn001);
      }
    }
    } catch ( ...) {
if (jjtc000) {
      jjtree.clearNodeScope(jjtn000);
      jjtc000 = false;
    } else {
      jjtree.popNode();
    }
    }
if (jjtc000) {
      jjtree.closeNodeScope(jjtn000, true);
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn000);
      }
    }
}


void SqlParser::character_substring_function() {
    jj_consume_token(SUBSTRING);
    jj_consume_token(lparen);
    character_value_expression();
    jj_consume_token(FROM);
    numeric_value_expression();
    if (jj_2_745(3)) {
      jj_consume_token(FOR);
      numeric_value_expression();
    } else {
      ;
    }
    if (jj_2_746(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
        bool jjtc001 = true;
        jjtree.openNodeScope(jjtn001);
        jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(USING);
        char_length_units();
      } catch ( ...) {
if (jjtc001) {
          jjtree.clearNodeScope(jjtn001);
          jjtc001 = false;
        } else {
          jjtree.popNode();
        }
      }
if (jjtc001) {
          jjtree.closeNodeScope(jjtn001, true);
          if (jjtree.nodeCreated()) {
           jjtreeCloseNodeScope(jjtn001);
          }
        }
    } else {
      ;
    }
    jj_consume_token(rparen);
}


void SqlParser::regular_expression_substring_function() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(SUBSTRING);
      jj_consume_token(lparen);
      character_value_expression();
      jj_consume_token(SIMILAR);
      character_value_expression();
      jj_consume_token(ESCAPE);
      character_value_expression();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::regex_substring_function() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(SUBSTRING_REGEX);
      jj_consume_token(lparen);
      character_value_expression();
      if (jj_2_747(3)) {
        jj_consume_token(FLAG);
        character_value_expression();
      } else {
        ;
      }
      jj_consume_token(IN);
      character_value_expression();
      if (jj_2_748(3)) {
        jj_consume_token(FROM);
        numeric_value_expression();
      } else {
        ;
      }
      if (jj_2_749(3)) {
        jj_consume_token(USING);
        char_length_units();
      } else {
        ;
      }
      if (jj_2_750(3)) {
        jj_consume_token(OCCURRENCE);
        numeric_value_expression();
      } else {
        ;
      }
      if (jj_2_751(3)) {
        jj_consume_token(GROUP);
        numeric_value_expression();
      } else {
        ;
      }
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::fold() {
    if (jj_2_752(3)) {
      jj_consume_token(UPPER);
    } else if (jj_2_753(3)) {
      jj_consume_token(LOWER);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
    jj_consume_token(lparen);
    character_value_expression();
    jj_consume_token(rparen);
}


void SqlParser::transcoding() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(CONVERT);
      jj_consume_token(lparen);
      character_value_expression();
      jj_consume_token(USING);
      schema_qualified_name();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::character_transliteration() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(TRANSLATE);
      jj_consume_token(lparen);
      character_value_expression();
      jj_consume_token(USING);
      schema_qualified_name();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::regex_transliteration() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(TRANSLATE_REGEX);
      jj_consume_token(lparen);
      character_value_expression();
      if (jj_2_754(3)) {
        jj_consume_token(FLAG);
        character_value_expression();
      } else {
        ;
      }
      jj_consume_token(IN);
      character_value_expression();
      if (jj_2_755(3)) {
        jj_consume_token(WITH);
        character_value_expression();
      } else {
        ;
      }
      if (jj_2_756(3)) {
        jj_consume_token(FROM);
        numeric_value_expression();
      } else {
        ;
      }
      if (jj_2_757(3)) {
        jj_consume_token(USING);
        char_length_units();
      } else {
        ;
      }
      if (jj_2_758(3)) {
        jj_consume_token(OCCURRENCE);
        regex_transliteration_occurrence();
      } else {
        ;
      }
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::regex_transliteration_occurrence() {
    if (jj_2_759(3)) {
      jj_consume_token(ALL);
    } else if (jj_2_760(3)) {
      numeric_value_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::trim_function() {
    jj_consume_token(TRIM);
    jj_consume_token(lparen);
    trim_operands();
    jj_consume_token(rparen);
}


void SqlParser::trim_operands() {
    if (jj_2_761(3)) {
      trim_specification();
    } else {
      ;
    }
    character_value_expression();
    if (jj_2_764(3)) {
      if (jj_2_762(3)) {
        jj_consume_token(570);
      } else if (jj_2_763(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
               bool jjtc001 = true;
               jjtree.openNodeScope(jjtn001);
               jjtreeOpenNodeScope(jjtn001);
        try {
          jj_consume_token(FROM);
        } catch ( ...) {
if (jjtc001) {
                 jjtree.clearNodeScope(jjtn001);
                 jjtc001 = false;
               } else {
                 jjtree.popNode();
               }
        }
if (jjtc001) {
                 jjtree.closeNodeScope(jjtn001, true);
                 if (jjtree.nodeCreated()) {
                  jjtreeCloseNodeScope(jjtn001);
                 }
               }
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
      character_value_expression();
    } else {
      ;
    }
}


void SqlParser::trim_specification() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_765(3)) {
        jj_consume_token(LEADING);
      } else if (jj_2_766(3)) {
        jj_consume_token(TRAILING);
      } else if (jj_2_767(3)) {
        jj_consume_token(BOTH);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::character_overlay_function() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(OVERLAY);
      jj_consume_token(lparen);
      character_value_expression();
      jj_consume_token(PLACING);
      character_value_expression();
      jj_consume_token(FROM);
      numeric_value_expression();
      if (jj_2_768(3)) {
        jj_consume_token(FOR);
        numeric_value_expression();
      } else {
        ;
      }
      if (jj_2_769(3)) {
        jj_consume_token(USING);
        char_length_units();
      } else {
        ;
      }
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::normalize_function() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(NORMALIZE);
      jj_consume_token(lparen);
      character_value_expression();
      if (jj_2_771(3)) {
        jj_consume_token(570);
        normal_form();
        if (jj_2_770(3)) {
          jj_consume_token(570);
          normalize_function_result_length();
        } else {
          ;
        }
      } else {
        ;
      }
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::normal_form() {
    if (jj_2_772(3)) {
      jj_consume_token(NFC);
    } else if (jj_2_773(3)) {
      jj_consume_token(NFD);
    } else if (jj_2_774(3)) {
      jj_consume_token(NFKC);
    } else if (jj_2_775(3)) {
      jj_consume_token(NFKD);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::normalize_function_result_length() {
    if (jj_2_776(3)) {
      character_length();
    } else if (jj_2_777(3)) {
      character_large_object_length();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::specific_type_method() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(569);
      jj_consume_token(SPECIFICTYPE);
      if (jj_2_778(3)) {
        jj_consume_token(lparen);
        jj_consume_token(rparen);
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::binary_value_function() {
    if (jj_2_779(3)) {
      binary_substring_function();
    } else if (jj_2_780(3)) {
      binary_trim_function();
    } else if (jj_2_781(3)) {
      binary_overlay_function();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::binary_substring_function() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(SUBSTRING);
      jj_consume_token(lparen);
      binary_value_expression();
      jj_consume_token(FROM);
      numeric_value_expression();
      if (jj_2_782(3)) {
        jj_consume_token(FOR);
        numeric_value_expression();
      } else {
        ;
      }
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::binary_trim_function() {
    jj_consume_token(TRIM);
    jj_consume_token(lparen);
    binary_trim_operands();
    jj_consume_token(rparen);
}


void SqlParser::binary_trim_operands() {
    if (jj_2_783(3)) {
      trim_specification();
    } else {
      ;
    }
    binary_value_expression();
    if (jj_2_786(3)) {
      if (jj_2_784(3)) {
        jj_consume_token(570);
      } else if (jj_2_785(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
               bool jjtc001 = true;
               jjtree.openNodeScope(jjtn001);
               jjtreeOpenNodeScope(jjtn001);
        try {
          jj_consume_token(FROM);
        } catch ( ...) {
if (jjtc001) {
                 jjtree.clearNodeScope(jjtn001);
                 jjtc001 = false;
               } else {
                 jjtree.popNode();
               }
        }
if (jjtc001) {
                 jjtree.closeNodeScope(jjtn001, true);
                 if (jjtree.nodeCreated()) {
                  jjtreeCloseNodeScope(jjtn001);
                 }
               }
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
      binary_value_expression();
    } else {
      ;
    }
}


void SqlParser::binary_overlay_function() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(OVERLAY);
      jj_consume_token(lparen);
      binary_value_expression();
      jj_consume_token(PLACING);
      binary_value_expression();
      jj_consume_token(FROM);
      numeric_value_expression();
      if (jj_2_787(3)) {
        jj_consume_token(FOR);
        numeric_value_expression();
      } else {
        ;
      }
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::datetime_value_expression() {
    if (jj_2_788(3)) {
      datetime_term();
    } else if (jj_2_789(3)) {
      interval_value_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::datetime_term() {
    datetime_factor();
}


void SqlParser::datetime_factor() {
    datetime_primary();
    if (jj_2_790(3)) {
      time_zone();
    } else {
      ;
    }
}


void SqlParser::datetime_primary() {
    if (jj_2_791(3)) {
      datetime_value_function();
    } else if (jj_2_792(3)) {
      interval_value_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::time_zone() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(AT);
      time_zone_specifier();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::time_zone_specifier() {
    if (jj_2_793(3)) {
      jj_consume_token(LOCAL);
    } else if (jj_2_794(3)) {
      jj_consume_token(TIME);
      jj_consume_token(ZONE);
      interval_primary();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::datetime_value_function() {/*@bgen(jjtree) BuiltinFunctionCall */
  BuiltinFunctionCall *jjtn000 = new BuiltinFunctionCall(JJTBUILTINFUNCTIONCALL);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
ArgumentList *jjtn001 = new ArgumentList(JJTARGUMENTLIST);
    bool jjtc001 = true;
    jjtree.openNodeScope(jjtn001);
    jjtreeOpenNodeScope(jjtn001);
      try {
        if (jj_2_795(3)) {
          current_date_value_function();
        } else if (jj_2_796(3)) {
          current_time_value_function();
        } else if (jj_2_797(3)) {
          current_timestamp_value_function();
        } else if (jj_2_798(3)) {
          current_local_time_value_function();
        } else if (jj_2_799(3)) {
          current_local_timestamp_value_function();
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } catch ( ...) {
if (jjtc001) {
      jjtree.clearNodeScope(jjtn001);
      jjtc001 = false;
    } else {
      jjtree.popNode();
    }
      }
if (jjtc001) {
      jjtree.closeNodeScope(jjtn001, jjtree.nodeArity() > 0);
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn001);
      }
    }
    } catch ( ...) {
if (jjtc000) {
      jjtree.clearNodeScope(jjtn000);
      jjtc000 = false;
    } else {
      jjtree.popNode();
    }
    }
if (jjtc000) {
      jjtree.closeNodeScope(jjtn000, true);
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn000);
      }
    }
}


void SqlParser::current_date_value_function() {
    jj_consume_token(CURRENT_DATE);
}


void SqlParser::current_time_value_function() {
    jj_consume_token(CURRENT_TIME);
    if (jj_2_800(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
                       bool jjtc001 = true;
                       jjtree.openNodeScope(jjtn001);
                       jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(lparen);
        jj_consume_token(unsigned_integer);
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc001) {
                         jjtree.clearNodeScope(jjtn001);
                         jjtc001 = false;
                       } else {
                         jjtree.popNode();
                       }
      }
if (jjtc001) {
                         jjtree.closeNodeScope(jjtn001, true);
                         if (jjtree.nodeCreated()) {
                          jjtreeCloseNodeScope(jjtn001);
                         }
                       }
    } else {
      ;
    }
}


void SqlParser::current_local_time_value_function() {
    jj_consume_token(LOCALTIME);
    if (jj_2_801(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
                    bool jjtc001 = true;
                    jjtree.openNodeScope(jjtn001);
                    jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(lparen);
        jj_consume_token(unsigned_integer);
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc001) {
                      jjtree.clearNodeScope(jjtn001);
                      jjtc001 = false;
                    } else {
                      jjtree.popNode();
                    }
      }
if (jjtc001) {
                      jjtree.closeNodeScope(jjtn001, true);
                      if (jjtree.nodeCreated()) {
                       jjtreeCloseNodeScope(jjtn001);
                      }
                    }
    } else {
      ;
    }
}


void SqlParser::current_timestamp_value_function() {
    jj_consume_token(CURRENT_TIMESTAMP);
    if (jj_2_802(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
                            bool jjtc001 = true;
                            jjtree.openNodeScope(jjtn001);
                            jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(lparen);
        jj_consume_token(unsigned_integer);
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc001) {
                              jjtree.clearNodeScope(jjtn001);
                              jjtc001 = false;
                            } else {
                              jjtree.popNode();
                            }
      }
if (jjtc001) {
                              jjtree.closeNodeScope(jjtn001, true);
                              if (jjtree.nodeCreated()) {
                               jjtreeCloseNodeScope(jjtn001);
                              }
                            }
    } else {
      ;
    }
}


void SqlParser::current_local_timestamp_value_function() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(LOCALTIMESTAMP);
      if (jj_2_803(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
                         bool jjtc001 = true;
                         jjtree.openNodeScope(jjtn001);
                         jjtreeOpenNodeScope(jjtn001);
        try {
          jj_consume_token(lparen);
          jj_consume_token(unsigned_integer);
          jj_consume_token(rparen);
        } catch ( ...) {
if (jjtc001) {
                           jjtree.clearNodeScope(jjtn001);
                           jjtc001 = false;
                         } else {
                           jjtree.popNode();
                         }
        }
if (jjtc001) {
                           jjtree.closeNodeScope(jjtn001, true);
                           if (jjtree.nodeCreated()) {
                            jjtreeCloseNodeScope(jjtn001);
                           }
                         }
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::interval_value_expression() {
    if (jj_2_804(3)) {
      interval_term();
    } else if (jj_2_805(2147483647)) {
AdditiveExpression *jjtn001 = new AdditiveExpression(JJTADDITIVEEXPRESSION);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(lparen);
        datetime_value_expression();
        jj_consume_token(MINUS);
        datetime_term();
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
      interval_qualifier();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::interval_term() {/*@bgen(jjtree) #MultiplicativeExpression(> 1) */
  MultiplicativeExpression *jjtn000 = new MultiplicativeExpression(JJTMULTIPLICATIVEEXPRESSION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      interval_factor();
      if (jj_2_808(3)) {
        if (jj_2_806(3)) {
          jj_consume_token(STAR);
        } else if (jj_2_807(3)) {
          jj_consume_token(DIV);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
        factor();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::interval_factor() {
    if (jj_2_811(3)) {
UnaryExpression *jjtn001 = new UnaryExpression(JJTUNARYEXPRESSION);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        if (jj_2_809(3)) {
          jj_consume_token(PLUS);
        } else if (jj_2_810(3)) {
          jj_consume_token(MINUS);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
        interval_primary();
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001,  1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else if (jj_2_812(3)) {
      interval_primary();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::interval_primary() {
    if (jj_2_814(3)) {
      interval_value_function();
    } else if (jj_2_815(3)) {
      array_value_expression();
      if (jj_2_813(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
        try {
          interval_qualifier();
        } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
        }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
      } else {
        ;
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::interval_value_function() {/*@bgen(jjtree) BuiltinFunctionCall */
  BuiltinFunctionCall *jjtn000 = new BuiltinFunctionCall(JJTBUILTINFUNCTIONCALL);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
ArgumentList *jjtn001 = new ArgumentList(JJTARGUMENTLIST);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        interval_absolute_value_function();
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, jjtree.nodeArity() > 0);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::interval_absolute_value_function() {
    jj_consume_token(ABS);
    jj_consume_token(lparen);
    interval_value_expression();
    jj_consume_token(rparen);
}


void SqlParser::boolean_value_expression() {/*@bgen(jjtree) #OrExpression(> 1) */
  OrExpression *jjtn000 = new OrExpression(JJTOREXPRESSION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      boolean_term();
      while (!hasError) {
        if (jj_2_816(3)) {
          ;
        } else {
          goto end_label_20;
        }
        jj_consume_token(OR);
        boolean_term();
      }
      end_label_20: ;
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::boolean_term() {/*@bgen(jjtree) #AndExpression(> 1) */
  AndExpression *jjtn000 = new AndExpression(JJTANDEXPRESSION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      boolean_factor();
      while (!hasError) {
        if (jj_2_817(3)) {
          ;
        } else {
          goto end_label_21;
        }
        jj_consume_token(AND);
        boolean_factor();
      }
      end_label_21: ;
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::boolean_factor() {
    if (jj_2_818(3)) {
NotExpression *jjtn001 = new NotExpression(JJTNOTEXPRESSION);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(NOT);
        boolean_test();
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else if (jj_2_819(3)) {
      boolean_test();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::boolean_test() {/*@bgen(jjtree) #IsExpression(> 1) */
  IsExpression *jjtn000 = new IsExpression(JJTISEXPRESSION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      boolean_primary();
      if (jj_2_821(3)) {
        jj_consume_token(IS);
        if (jj_2_820(3)) {
          jj_consume_token(NOT);
        } else {
          ;
        }
        truth_value();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::truth_value() {
    if (jj_2_822(3)) {
      jj_consume_token(TRUE);
    } else if (jj_2_823(3)) {
      jj_consume_token(FALSE);
    } else if (jj_2_824(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(UNKNOWN);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::boolean_primary() {
    if (jj_2_825(3)) {
      predicate();
    } else if (jj_2_826(3)) {
      boolean_predicand();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::boolean_predicand() {
    if (jj_2_827(3)) {
      parenthesized_boolean_value_expression();
    } else if (jj_2_828(3)) {
      numeric_value_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::parenthesized_boolean_value_expression() {/*@bgen(jjtree) ParenthesizedExpression */
  ParenthesizedExpression *jjtn000 = new ParenthesizedExpression(JJTPARENTHESIZEDEXPRESSION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(lparen);
      boolean_value_expression();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
         jjtree.clearNodeScope(jjtn000);
         jjtc000 = false;
       } else {
         jjtree.popNode();
       }
    }
if (jjtc000) {
         jjtree.closeNodeScope(jjtn000, true);
         if (jjtree.nodeCreated()) {
          jjtreeCloseNodeScope(jjtn000);
         }
       }
}


void SqlParser::array_value_expression() {/*@bgen(jjtree) #Concatenation(> 1) */
  Concatenation *jjtn000 = new Concatenation(JJTCONCATENATION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      array_primary();
      while (!hasError) {
        if (jj_2_829(3)) {
          ;
        } else {
          goto end_label_22;
        }
        jj_consume_token(576);
        array_primary();
      }
      end_label_22: ;
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::array_value_expression_1() {
    array_value_expression();
}


void SqlParser::array_primary() {
    if (jj_2_830(3)) {
      array_value_function();
    } else if (jj_2_831(3)) {
      multiset_value_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::array_value_function() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      trim_array_function();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::trim_array_function() {
    jj_consume_token(TRIM_ARRAY);
    jj_consume_token(lparen);
    array_value_expression();
    jj_consume_token(570);
    numeric_value_expression();
    jj_consume_token(rparen);
}


void SqlParser::array_value_constructor() {
    if (jj_2_832(3)) {
      array_value_constructor_by_enumeration();
    } else if (jj_2_833(3)) {
      array_value_constructor_by_query();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::array_value_constructor_by_enumeration() {/*@bgen(jjtree) ArrayLiteral */
  ArrayLiteral *jjtn000 = new ArrayLiteral(JJTARRAYLITERAL);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(ARRAY);
      left_bracket_or_trigraph();
      if (jj_2_834(3)) {
        array_element_list();
      } else {
        ;
      }
      right_bracket_or_trigraph();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::array_element_list() {
    array_element();
    while (!hasError) {
      if (jj_2_835(3)) {
        ;
      } else {
        goto end_label_23;
      }
      jj_consume_token(570);
      array_element();
    }
    end_label_23: ;
}


void SqlParser::array_element() {
    value_expression();
}


void SqlParser::array_value_constructor_by_query() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(ARRAY);
      subquery();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::multiset_value_expression() {
    multiset_term();
    if (jj_2_844(3)) {
      if (jj_2_842(3)) {
        jj_consume_token(MULTISET);
        jj_consume_token(UNION);
        if (jj_2_838(3)) {
          if (jj_2_836(3)) {
            jj_consume_token(ALL);
          } else if (jj_2_837(3)) {
            jj_consume_token(DISTINCT);
          } else {
            jj_consume_token(-1);
            errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
          }
        } else {
          ;
        }
      } else if (jj_2_843(3)) {
        jj_consume_token(MULTISET);
        jj_consume_token(EXCEPT);
        if (jj_2_841(3)) {
          if (jj_2_839(3)) {
            jj_consume_token(ALL);
          } else if (jj_2_840(3)) {
            jj_consume_token(DISTINCT);
          } else {
            jj_consume_token(-1);
            errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
          }
        } else {
          ;
        }
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
                                                                                                                   bool jjtc001 = true;
                                                                                                                   jjtree.openNodeScope(jjtn001);
                                                                                                                   jjtreeOpenNodeScope(jjtn001);
      try {
        multiset_term();
      } catch ( ...) {
if (jjtc001) {
                                                                                                                     jjtree.clearNodeScope(jjtn001);
                                                                                                                     jjtc001 = false;
                                                                                                                   } else {
                                                                                                                     jjtree.popNode();
                                                                                                                   }
      }
if (jjtc001) {
                                                                                                                     jjtree.closeNodeScope(jjtn001,  2);
                                                                                                                     if (jjtree.nodeCreated()) {
                                                                                                                      jjtreeCloseNodeScope(jjtn001);
                                                                                                                     }
                                                                                                                   }
    } else {
      ;
    }
}


void SqlParser::multiset_term() {
    multiset_primary();
    if (jj_2_848(3)) {
      jj_consume_token(MULTISET);
      jj_consume_token(INTERSECT);
      if (jj_2_847(3)) {
        if (jj_2_845(3)) {
          jj_consume_token(ALL);
        } else if (jj_2_846(3)) {
          jj_consume_token(DISTINCT);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } else {
        ;
      }
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
                                                                         bool jjtc001 = true;
                                                                         jjtree.openNodeScope(jjtn001);
                                                                         jjtreeOpenNodeScope(jjtn001);
      try {
        multiset_primary();
      } catch ( ...) {
if (jjtc001) {
                                                                           jjtree.clearNodeScope(jjtn001);
                                                                           jjtc001 = false;
                                                                         } else {
                                                                           jjtree.popNode();
                                                                         }
      }
if (jjtc001) {
                                                                           jjtree.closeNodeScope(jjtn001,  2);
                                                                           if (jjtree.nodeCreated()) {
                                                                            jjtreeCloseNodeScope(jjtn001);
                                                                           }
                                                                         }
    } else {
      ;
    }
}


void SqlParser::multiset_primary() {
    if (jj_2_849(3)) {
      multiset_set_function();
    } else if (jj_2_850(3)) {
      value_expression_primary();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::multiset_set_function() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(SET);
      jj_consume_token(lparen);
      multiset_value_expression();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::multiset_value_constructor() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_851(3)) {
        multiset_value_constructor_by_enumeration();
      } else if (jj_2_852(3)) {
        multiset_value_constructor_by_query();
      } else if (jj_2_853(3)) {
        table_value_constructor_by_query();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::multiset_value_constructor_by_enumeration() {
    jj_consume_token(MULTISET);
    left_bracket_or_trigraph();
    multiset_element_list();
    right_bracket_or_trigraph();
}


void SqlParser::multiset_element_list() {
    multiset_element();
    while (!hasError) {
      if (jj_2_854(3)) {
        ;
      } else {
        goto end_label_24;
      }
      jj_consume_token(570);
      multiset_element();
    }
    end_label_24: ;
}


void SqlParser::multiset_element() {
    value_expression();
}


void SqlParser::multiset_value_constructor_by_query() {
    jj_consume_token(MULTISET);
    subquery();
}


void SqlParser::table_value_constructor_by_query() {
    jj_consume_token(TABLE);
    subquery();
}


void SqlParser::row_value_constructor() {
    if (jj_2_855(3)) {
      explicit_row_value_constructor();
    } else if (jj_2_856(3)) {
      common_value_expression();
    } else if (jj_2_857(3)) {
      boolean_value_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::explicit_row_value_constructor() {
    if (jj_2_859(3)) {
RowExpression *jjtn001 = new RowExpression(JJTROWEXPRESSION);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(ROW);
        jj_consume_token(lparen);
        row_value_constructor_element_list();
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else if (jj_2_860(3)) {
      subquery();
    } else if (jj_2_861(3)) {
RowExpression *jjtn002 = new RowExpression(JJTROWEXPRESSION);
      bool jjtc002 = true;
      jjtree.openNodeScope(jjtn002);
      jjtreeOpenNodeScope(jjtn002);
      try {
        jj_consume_token(lparen);
        row_value_constructor_element();
        if (jj_2_858(3)) {
          jj_consume_token(570);
          row_value_constructor_element_list();
        } else {
          ;
        }
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc002) {
        jjtree.clearNodeScope(jjtn002);
        jjtc002 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc002) {
        jjtree.closeNodeScope(jjtn002, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn002);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::row_value_constructor_element_list() {
    row_value_constructor_element();
    while (!hasError) {
      if (jj_2_862(3)) {
        ;
      } else {
        goto end_label_25;
      }
      jj_consume_token(570);
      row_value_constructor_element();
    }
    end_label_25: ;
}


void SqlParser::row_value_constructor_element() {
    value_expression();
}


void SqlParser::contextually_typed_row_value_constructor() {
    if (jj_2_863(3)) {
      common_value_expression();
    } else if (jj_2_864(3)) {
      boolean_value_expression();
    } else if (jj_2_865(3)) {
      contextually_typed_value_specification();
    } else if (jj_2_866(3)) {
ParenthesizedExpression *jjtn001 = new ParenthesizedExpression(JJTPARENTHESIZEDEXPRESSION);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(lparen);
        contextually_typed_value_specification();
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else if (jj_2_867(3)) {
ParenthesizedExpression *jjtn002 = new ParenthesizedExpression(JJTPARENTHESIZEDEXPRESSION);
      bool jjtc002 = true;
      jjtree.openNodeScope(jjtn002);
      jjtreeOpenNodeScope(jjtn002);
      try {
        jj_consume_token(lparen);
        contextually_typed_row_value_constructor_element();
        jj_consume_token(570);
        contextually_typed_row_value_constructor_element_list();
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc002) {
        jjtree.clearNodeScope(jjtn002);
        jjtc002 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc002) {
        jjtree.closeNodeScope(jjtn002, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn002);
        }
      }
    } else if (jj_2_868(3)) {
RowExression *jjtn003 = new RowExression(JJTROWEXRESSION);
      bool jjtc003 = true;
      jjtree.openNodeScope(jjtn003);
      jjtreeOpenNodeScope(jjtn003);
      try {
        jj_consume_token(ROW);
        jj_consume_token(lparen);
        contextually_typed_row_value_constructor_element_list();
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc003) {
        jjtree.clearNodeScope(jjtn003);
        jjtc003 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc003) {
        jjtree.closeNodeScope(jjtn003, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn003);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::contextually_typed_row_value_constructor_element_list() {
    contextually_typed_row_value_constructor_element();
    while (!hasError) {
      if (jj_2_869(3)) {
        ;
      } else {
        goto end_label_26;
      }
      jj_consume_token(570);
      contextually_typed_row_value_constructor_element();
    }
    end_label_26: ;
}


void SqlParser::contextually_typed_row_value_constructor_element() {
    contextually_typed_value_specification();
}


void SqlParser::row_value_constructor_predicand() {
    if (jj_2_870(3)) {
      common_value_expression();
    } else if (jj_2_871(3)) {
      explicit_row_value_constructor();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::row_value_expression() {
    if (jj_2_872(3)) {
      explicit_row_value_constructor();
    } else if (jj_2_873(3)) {
      row_value_special_case();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::table_row_value_expression() {
    if (jj_2_874(3)) {
      row_value_constructor();
    } else if (jj_2_875(3)) {
      row_value_special_case();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::contextually_typed_row_value_expression() {
    if (jj_2_876(3)) {
      contextually_typed_row_value_constructor();
    } else if (jj_2_877(3)) {
      row_value_special_case();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::row_value_predicand() {
    if (jj_2_878(3)) {
      row_value_constructor_predicand();
    } else if (jj_2_879(3)) {
      row_value_special_case();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::row_value_special_case() {
    if (jj_2_880(3)) {
      common_value_expression();
    } else if (jj_2_881(3)) {
      nonparenthesized_value_expression_primary();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::table_value_constructor() {/*@bgen(jjtree) Values */
  Values *jjtn000 = new Values(JJTVALUES);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(VALUES);
      row_value_expression_list();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::row_value_expression_list() {
    table_row_value_expression();
    while (!hasError) {
      if (jj_2_882(3)) {
        ;
      } else {
        goto end_label_27;
      }
      jj_consume_token(570);
      table_row_value_expression();
    }
    end_label_27: ;
}


void SqlParser::contextually_typed_table_value_constructor() {/*@bgen(jjtree) Values */
  Values *jjtn000 = new Values(JJTVALUES);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(VALUES);
      contextually_typed_row_value_expression_list();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::contextually_typed_row_value_expression_list() {
    contextually_typed_row_value_expression();
    while (!hasError) {
      if (jj_2_883(3)) {
        ;
      } else {
        goto end_label_28;
      }
      jj_consume_token(570);
      contextually_typed_row_value_expression();
    }
    end_label_28: ;
}


void SqlParser::table_expression() {/*@bgen(jjtree) TableExpression */
  TableExpression *jjtn000 = new TableExpression(JJTTABLEEXPRESSION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      from_clause();
      if (jj_2_884(3)) {
        where_clause();
      } else {
        ;
      }
      if (jj_2_885(3)) {
        group_by_clause();
      } else {
        ;
      }
      if (jj_2_886(3)) {
        having_clause();
      } else {
        ;
      }
      if (jj_2_887(3)) {
        window_clause();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::from_clause() {/*@bgen(jjtree) FromClause */
  FromClause *jjtn000 = new FromClause(JJTFROMCLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(FROM);
      table_reference_list();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::table_reference_list() {/*@bgen(jjtree) #CommaJoin(> 1) */
  CommaJoin *jjtn000 = new CommaJoin(JJTCOMMAJOIN);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      table_reference();
      while (!hasError) {
        if (jj_2_888(3)) {
          ;
        } else {
          goto end_label_29;
        }
        jj_consume_token(570);
        table_reference();
      }
      end_label_29: ;
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::table_reference() {/*@bgen(jjtree) #Join(> 1) */
  Join *jjtn000 = new Join(JJTJOIN);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      table_factor();
      while (!hasError) {
        if (jj_2_889(3)) {
          ;
        } else {
          goto end_label_30;
        }
        joined_table();
      }
      end_label_30: ;
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::table_factor() {/*@bgen(jjtree) #TableSample(> 1) */
  TableSample *jjtn000 = new TableSample(JJTTABLESAMPLE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      table_primary();
      if (jj_2_890(3)) {
        sample_clause();
      } else {
        ;
      }
      if (jj_2_891(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
        bool jjtc001 = true;
        jjtree.openNodeScope(jjtn001);
        jjtreeOpenNodeScope(jjtn001);
        try {
          partitioned_join_table();
        } catch ( ...) {
if (jjtc001) {
          jjtree.clearNodeScope(jjtn001);
          jjtc001 = false;
        } else {
          jjtree.popNode();
        }
        }
if (jjtc001) {
          jjtree.closeNodeScope(jjtn001, true);
          if (jjtree.nodeCreated()) {
           jjtreeCloseNodeScope(jjtn001);
          }
        }
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::sample_clause() {
    jj_consume_token(TABLESAMPLE);
    sample_method();
    jj_consume_token(lparen);
    sample_percentage();
    jj_consume_token(rparen);
    if (jj_2_892(3)) {
      repeatable_clause();
    } else {
      ;
    }
}


void SqlParser::sample_method() {
    if (jj_2_893(3)) {
      jj_consume_token(BERNOULLI);
    } else if (jj_2_894(3)) {
      jj_consume_token(SYSTEM);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::repeatable_clause() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(REPEATABLE);
      jj_consume_token(lparen);
      repeat_argument();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::sample_percentage() {
    numeric_value_expression();
}


void SqlParser::repeat_argument() {
    numeric_value_expression();
}


void SqlParser::table_primary() {/*@bgen(jjtree) #AliasedTable(> 1) */
  AliasedTable *jjtn000 = new AliasedTable(JJTALIASEDTABLE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_895(3)) {
        table_or_query_name();
      } else if (jj_2_896(2147483647)) {
        derived_table();
      } else if (jj_2_897(3)) {
        parenthesized_joined_table();
      } else if (jj_2_898(3)) {
        lateral_derived_table();
      } else if (jj_2_899(3)) {
        collection_derived_table();
      } else if (jj_2_900(3)) {
        table_function_derived_table();
      } else if (jj_2_901(3)) {
        only_spec();
      } else if (jj_2_902(3)) {
        data_change_delta_table();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
      if (jj_2_903(3)) {
        alias();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
      jjtree.clearNodeScope(jjtn000);
      jjtc000 = false;
    } else {
      jjtree.popNode();
    }
    }
if (jjtc000) {
      jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn000);
      }
    }
}


void SqlParser::alias() {/*@bgen(jjtree) Alias */
  Alias *jjtn000 = new Alias(JJTALIAS);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_904(3)) {
        jj_consume_token(AS);
      } else {
        ;
      }
      identifier();
      if (jj_2_905(3)) {
        identifier_suffix_chain();
      } else {
        ;
      }
      if (jj_2_906(3)) {
        jj_consume_token(lparen);
        column_name_list();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::system_version_specification() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_910(3)) {
        jj_consume_token(AS);
        jj_consume_token(OF);
        jj_consume_token(SYSTEM);
        jj_consume_token(TIME);
        datetime_value_expression();
      } else if (jj_2_911(3)) {
        jj_consume_token(VERSIONS);
        jj_consume_token(BEFORE);
        jj_consume_token(SYSTEM);
        jj_consume_token(TIME);
        datetime_value_expression();
      } else if (jj_2_912(3)) {
        jj_consume_token(VERSIONS);
        jj_consume_token(AFTER);
        jj_consume_token(SYSTEM);
        jj_consume_token(TIME);
        datetime_value_expression();
      } else if (jj_2_913(3)) {
        jj_consume_token(VERSIONS);
        jj_consume_token(BETWEEN);
        if (jj_2_909(3)) {
          if (jj_2_907(3)) {
            jj_consume_token(ASYMMETRIC);
          } else if (jj_2_908(3)) {
            jj_consume_token(SYMMETRIC);
          } else {
            jj_consume_token(-1);
            errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
          }
        } else {
          ;
        }
        jj_consume_token(SYSTEM);
        jj_consume_token(TIME);
        datetime_value_expression();
        jj_consume_token(AND);
        datetime_value_expression();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::only_spec() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(ONLY);
      jj_consume_token(lparen);
      table_or_query_name();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::lateral_derived_table() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(LATERAL);
      subquery();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::collection_derived_table() {/*@bgen(jjtree) Unnest */
  Unnest *jjtn000 = new Unnest(JJTUNNEST);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(UNNEST);
      jj_consume_token(lparen);
      collection_value_expression();
      while (!hasError) {
        if (jj_2_914(3)) {
          ;
        } else {
          goto end_label_31;
        }
        jj_consume_token(570);
        collection_value_expression();
      }
      end_label_31: ;
      jj_consume_token(rparen);
      if (jj_2_915(3)) {
        jj_consume_token(WITH);
        jj_consume_token(ORDINALITY);
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::table_function_derived_table() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(TABLE);
      jj_consume_token(lparen);
      collection_value_expression();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::derived_table() {
    if (jj_2_916(2147483647)) {
      query_expression();
    } else if (jj_2_917(3)) {
      subquery();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::table_or_query_name() {
    if (jj_2_918(3)) {
      table_name();
    } else if (jj_2_919(3)) {
      identifier();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::column_name_list() {/*@bgen(jjtree) ColumnNames */
  ColumnNames *jjtn000 = new ColumnNames(JJTCOLUMNNAMES);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      identifier();
      while (!hasError) {
        if (jj_2_920(3)) {
          ;
        } else {
          goto end_label_32;
        }
        jj_consume_token(570);
        identifier();
      }
      end_label_32: ;
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::data_change_delta_table() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      result_option();
      jj_consume_token(TABLE);
      jj_consume_token(lparen);
      data_change_statement();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::data_change_statement() {
    if (jj_2_921(3)) {
      delete_statement_searched();
    } else if (jj_2_922(3)) {
      insert_statement();
    } else if (jj_2_923(3)) {
      merge_statement();
    } else if (jj_2_924(3)) {
      update_statement_searched();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::result_option() {
    if (jj_2_925(3)) {
      jj_consume_token(FINAL);
    } else if (jj_2_926(3)) {
      jj_consume_token(NEW);
    } else if (jj_2_927(3)) {
      jj_consume_token(OLD);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::parenthesized_joined_table() {/*@bgen(jjtree) #Join(> 1) */
  Join *jjtn000 = new Join(JJTJOIN);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(lparen);
      if (jj_2_928(2147483647)) {
        table_primary();
      } else if (jj_2_929(3)) {
        table_reference();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
      while (!hasError) {
        if (jj_2_930(3)) {
          ;
        } else {
          goto end_label_33;
        }
        joined_table();
      }
      end_label_33: ;
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::joined_table() {
    if (jj_2_931(3)) {
      cross_join();
    } else if (jj_2_932(3)) {
      qualified_join();
    } else if (jj_2_933(3)) {
      natural_join();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::cross_join() {
    jj_consume_token(CROSS);
    jj_consume_token(JOIN);
    table_factor();
}


void SqlParser::qualified_join() {
    if (jj_2_934(3)) {
      join_type();
    } else {
      ;
    }
    jj_consume_token(JOIN);
    if (jj_2_935(3)) {
      table_reference();
    } else if (jj_2_936(3)) {
      partitioned_join_table();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
    join_specification();
}


void SqlParser::partitioned_join_table() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(PARTITION);
      jj_consume_token(BY);
      partitioned_join_column_reference_list();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::partitioned_join_column_reference_list() {
    jj_consume_token(lparen);
    partitioned_join_column_reference();
    while (!hasError) {
      if (jj_2_937(3)) {
        ;
      } else {
        goto end_label_34;
      }
      jj_consume_token(570);
      partitioned_join_column_reference();
    }
    end_label_34: ;
    jj_consume_token(rparen);
}


void SqlParser::partitioned_join_column_reference() {
    column_reference();
}


void SqlParser::natural_join() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(NATURAL);
      if (jj_2_938(3)) {
        join_type();
      } else {
        ;
      }
      jj_consume_token(JOIN);
      if (jj_2_939(3)) {
        table_factor();
      } else if (jj_2_940(3)) {
        partitioned_join_table();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::join_specification() {
    if (jj_2_941(3)) {
      join_condition();
    } else if (jj_2_942(3)) {
      named_columns_join();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::join_condition() {/*@bgen(jjtree) OnClause */
  OnClause *jjtn000 = new OnClause(JJTONCLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(ON);
      search_condition();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::named_columns_join() {/*@bgen(jjtree) UsingClause */
  UsingClause *jjtn000 = new UsingClause(JJTUSINGCLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(USING);
      jj_consume_token(lparen);
      join_column_list();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::join_type() {
    if (jj_2_944(3)) {
      jj_consume_token(INNER);
    } else if (jj_2_945(3)) {
      outer_join_type();
      if (jj_2_943(3)) {
        jj_consume_token(OUTER);
      } else {
        ;
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::outer_join_type() {
    if (jj_2_946(3)) {
      jj_consume_token(LEFT);
    } else if (jj_2_947(3)) {
      jj_consume_token(RIGHT);
    } else if (jj_2_948(3)) {
      jj_consume_token(FULL);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::join_column_list() {
    column_name_list();
}


void SqlParser::where_clause() {/*@bgen(jjtree) WhereClause */
  WhereClause *jjtn000 = new WhereClause(JJTWHERECLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(WHERE);
      search_condition();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::group_by_clause() {/*@bgen(jjtree) GroupbyClause */
  GroupbyClause *jjtn000 = new GroupbyClause(JJTGROUPBYCLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(GROUP);
      jj_consume_token(BY);
      if (jj_2_949(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
        bool jjtc001 = true;
        jjtree.openNodeScope(jjtn001);
        jjtreeOpenNodeScope(jjtn001);
        try {
          set_quantifier();
        } catch ( ...) {
if (jjtc001) {
          jjtree.clearNodeScope(jjtn001);
          jjtc001 = false;
        } else {
          jjtree.popNode();
        }
        }
if (jjtc001) {
          jjtree.closeNodeScope(jjtn001, true);
          if (jjtree.nodeCreated()) {
           jjtreeCloseNodeScope(jjtn001);
          }
        }
      } else {
        ;
      }
      grouping_element_list();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::grouping_element_list() {
    grouping_element();
    while (!hasError) {
      if (jj_2_950(3)) {
        ;
      } else {
        goto end_label_35;
      }
      jj_consume_token(570);
      grouping_element();
    }
    end_label_35: ;
}


void SqlParser::grouping_element() {
    if (jj_2_951(3)) {
      rollup_list();
    } else if (jj_2_952(3)) {
      cube_list();
    } else if (jj_2_953(3)) {
      grouping_sets_specification();
    } else if (jj_2_954(3)) {
      empty_grouping_set();
    } else if (jj_2_955(3)) {
      ordinary_grouping_set();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::ordinary_grouping_set() {
    if (jj_2_956(3)) {
      grouping_column_reference();
    } else if (jj_2_957(3)) {
      jj_consume_token(lparen);
      grouping_column_reference_list();
      jj_consume_token(rparen);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::grouping_column_reference() {
    if (jj_2_958(3)) {
      grouping_expression();
    } else if (jj_2_959(3)) {
      column_reference();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
    if (jj_2_960(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        collate_clause();
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else {
      ;
    }
}


void SqlParser::grouping_column_reference_list() {
    grouping_column_reference();
    while (!hasError) {
      if (jj_2_961(3)) {
        ;
      } else {
        goto end_label_36;
      }
      jj_consume_token(570);
      grouping_column_reference();
    }
    end_label_36: ;
}


void SqlParser::rollup_list() {/*@bgen(jjtree) Rollup */
  Rollup *jjtn000 = new Rollup(JJTROLLUP);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(ROLLUP);
      jj_consume_token(lparen);
      ordinary_grouping_set_list();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::ordinary_grouping_set_list() {
    ordinary_grouping_set();
    while (!hasError) {
      if (jj_2_962(3)) {
        ;
      } else {
        goto end_label_37;
      }
      jj_consume_token(570);
      ordinary_grouping_set();
    }
    end_label_37: ;
}


void SqlParser::cube_list() {/*@bgen(jjtree) Cube */
  Cube *jjtn000 = new Cube(JJTCUBE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(CUBE);
      jj_consume_token(lparen);
      ordinary_grouping_set_list();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::grouping_sets_specification() {/*@bgen(jjtree) GroupingSets */
  GroupingSets *jjtn000 = new GroupingSets(JJTGROUPINGSETS);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(GROUPING);
      jj_consume_token(SETS);
      jj_consume_token(lparen);
      grouping_set_list();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::grouping_set_list() {
    grouping_set();
    while (!hasError) {
      if (jj_2_963(3)) {
        ;
      } else {
        goto end_label_38;
      }
      jj_consume_token(570);
      grouping_set();
    }
    end_label_38: ;
}


void SqlParser::grouping_set() {
    if (jj_2_964(3)) {
      rollup_list();
    } else if (jj_2_965(3)) {
      cube_list();
    } else if (jj_2_966(3)) {
      grouping_sets_specification();
    } else if (jj_2_967(3)) {
      empty_grouping_set();
    } else if (jj_2_968(3)) {
      ordinary_grouping_set();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::empty_grouping_set() {
    jj_consume_token(lparen);
    jj_consume_token(rparen);
}


void SqlParser::having_clause() {/*@bgen(jjtree) HavingClause */
  HavingClause *jjtn000 = new HavingClause(JJTHAVINGCLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(HAVING);
      search_condition();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::window_clause() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(WINDOW);
      window_definition_list();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::window_definition_list() {
    window_definition();
    while (!hasError) {
      if (jj_2_969(3)) {
        ;
      } else {
        goto end_label_39;
      }
      jj_consume_token(570);
      window_definition();
    }
    end_label_39: ;
}


void SqlParser::window_definition() {
    identifier();
    jj_consume_token(AS);
    window_specification();
}


void SqlParser::window_specification() {/*@bgen(jjtree) WindowSpecification */
  WindowSpecification *jjtn000 = new WindowSpecification(JJTWINDOWSPECIFICATION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(lparen);
      if (jj_2_970(3)) {
        window_specification_details();
      } else {
        ;
      }
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
         jjtree.clearNodeScope(jjtn000);
         jjtc000 = false;
       } else {
         jjtree.popNode();
       }
    }
if (jjtc000) {
         jjtree.closeNodeScope(jjtn000, true);
         if (jjtree.nodeCreated()) {
          jjtreeCloseNodeScope(jjtn000);
         }
       }
}


void SqlParser::window_specification_details() {
    while (!hasError) {
      if (jj_2_971(3)) {
        window_partition_clause();
      } else if (jj_2_972(3)) {
        window_order_clause();
      } else if (jj_2_973(3)) {
        window_frame_clause();
      } else if (jj_2_974(3)) {
        existing_identifier();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
      if (jj_2_975(3)) {
        ;
      } else {
        goto end_label_40;
      }
    }
    end_label_40: ;
}


void SqlParser::existing_identifier() {
    identifier();
}


void SqlParser::window_partition_clause() {/*@bgen(jjtree) PartitionByClause */
  PartitionByClause *jjtn000 = new PartitionByClause(JJTPARTITIONBYCLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(PARTITION);
      jj_consume_token(BY);
      window_partition_column_reference_list();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::window_partition_column_reference_list() {
    window_partition_column_reference();
    while (!hasError) {
      if (jj_2_976(3)) {
        ;
      } else {
        goto end_label_41;
      }
      jj_consume_token(570);
      window_partition_column_reference();
    }
    end_label_41: ;
}


void SqlParser::window_partition_column_reference() {
    if (jj_2_977(3)) {
      value_expression();
    } else if (jj_2_978(3)) {
      column_reference();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
    if (jj_2_979(3)) {
      collate_clause();
    } else {
      ;
    }
}


void SqlParser::window_order_clause() {/*@bgen(jjtree) OrderByClause */
  OrderByClause *jjtn000 = new OrderByClause(JJTORDERBYCLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(ORDER);
      jj_consume_token(BY);
      sort_specification_list();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::window_frame_clause() {
    window_frame_units();
    window_frame_extent();
    if (jj_2_980(3)) {
      window_frame_exclusion();
    } else {
      ;
    }
}


void SqlParser::window_frame_units() {/*@bgen(jjtree) WindowFrameUnits */
  WindowFrameUnits *jjtn000 = new WindowFrameUnits(JJTWINDOWFRAMEUNITS);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_981(3)) {
        jj_consume_token(ROWS);
      } else if (jj_2_982(3)) {
        jj_consume_token(RANGE);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::window_frame_extent() {/*@bgen(jjtree) WindowFrameExtent */
  WindowFrameExtent *jjtn000 = new WindowFrameExtent(JJTWINDOWFRAMEEXTENT);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_983(3)) {
        window_frame_start();
      } else if (jj_2_984(3)) {
        window_frame_between();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::window_frame_start() {
    if (jj_2_985(3)) {
UnboundedPreceding *jjtn001 = new UnboundedPreceding(JJTUNBOUNDEDPRECEDING);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(UNBOUNDED);
        jj_consume_token(PRECEDING);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else if (jj_2_986(3)) {
CurrentRow *jjtn002 = new CurrentRow(JJTCURRENTROW);
      bool jjtc002 = true;
      jjtree.openNodeScope(jjtn002);
      jjtreeOpenNodeScope(jjtn002);
      try {
        jj_consume_token(CURRENT);
        jj_consume_token(ROW);
      } catch ( ...) {
if (jjtc002) {
        jjtree.clearNodeScope(jjtn002);
        jjtc002 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc002) {
        jjtree.closeNodeScope(jjtn002, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn002);
        }
      }
    } else if (jj_2_987(3)) {
      window_frame_preceding();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::window_frame_preceding() {/*@bgen(jjtree) WindowFramePreceding */
  WindowFramePreceding *jjtn000 = new WindowFramePreceding(JJTWINDOWFRAMEPRECEDING);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      value_expression();
      jj_consume_token(PRECEDING);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::window_frame_between() {/*@bgen(jjtree) WindowFrameBetween */
  WindowFrameBetween *jjtn000 = new WindowFrameBetween(JJTWINDOWFRAMEBETWEEN);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(BETWEEN);
      window_frame_bound();
      jj_consume_token(AND);
      window_frame_bound();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::window_frame_bound() {
    if (jj_2_988(3)) {
      window_frame_start();
    } else if (jj_2_989(3)) {
UnboundedFollowing *jjtn001 = new UnboundedFollowing(JJTUNBOUNDEDFOLLOWING);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(UNBOUNDED);
        jj_consume_token(FOLLOWING);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else if (jj_2_990(3)) {
      window_frame_following();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::window_frame_following() {/*@bgen(jjtree) WindowFrameFollowing */
  WindowFrameFollowing *jjtn000 = new WindowFrameFollowing(JJTWINDOWFRAMEFOLLOWING);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      value_expression();
      jj_consume_token(FOLLOWING);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::window_frame_exclusion() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_991(3)) {
        jj_consume_token(EXCLUDE);
        jj_consume_token(CURRENT);
        jj_consume_token(ROW);
      } else if (jj_2_992(3)) {
        jj_consume_token(EXCLUDE);
        jj_consume_token(GROUP);
      } else if (jj_2_993(3)) {
        jj_consume_token(EXCLUDE);
        jj_consume_token(TIES);
      } else if (jj_2_994(3)) {
        jj_consume_token(EXCLUDE);
        jj_consume_token(NO);
        jj_consume_token(OTHERS);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::query_specification() {/*@bgen(jjtree) Select */
  Select *jjtn000 = new Select(JJTSELECT);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(SELECT);
      if (jj_2_995(3)) {
        set_quantifier();
      } else {
        ;
      }
      select_list();
      if (jj_2_996(3)) {
        table_expression();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::select_list() {/*@bgen(jjtree) SelectList */
  SelectList *jjtn000 = new SelectList(JJTSELECTLIST);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1001(3)) {
        star();
        while (!hasError) {
          if (jj_2_997(3)) {
            ;
          } else {
            goto end_label_42;
          }
          jj_consume_token(570);
          select_sublist();
        }
        end_label_42: ;
      } else if (jj_2_1002(3)) {
        select_sublist();
        while (!hasError) {
          if (jj_2_998(3)) {
            ;
          } else {
            goto end_label_43;
          }
          jj_consume_token(570);
          select_sublist();
        }
        end_label_43: ;
        if (jj_2_999(3)) {
          jj_consume_token(570);
          star();
        } else {
          ;
        }
        while (!hasError) {
          if (jj_2_1000(3)) {
            ;
          } else {
            goto end_label_44;
          }
          jj_consume_token(570);
          select_sublist();
        }
        end_label_44: ;
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::star() {/*@bgen(jjtree) SelectItem */
  SelectItem *jjtn000 = new SelectItem(JJTSELECTITEM);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
Star *jjtn001 = new Star(JJTSTAR);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(STAR);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::select_sublist() {/*@bgen(jjtree) SelectItem */
  SelectItem *jjtn000 = new SelectItem(JJTSELECTITEM);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      derived_column();
      if (jj_2_1006(3)) {
        if (jj_2_1004(3)) {
          jj_consume_token(569);
Star *jjtn001 = new Star(JJTSTAR);
            bool jjtc001 = true;
            jjtree.openNodeScope(jjtn001);
            jjtreeOpenNodeScope(jjtn001);
          try {
            jj_consume_token(STAR);
          } catch ( ...) {
if (jjtc001) {
              jjtree.clearNodeScope(jjtn001);
              jjtc001 = false;
            } else {
              jjtree.popNode();
            }
          }
if (jjtc001) {
              jjtree.closeNodeScope(jjtn001, true);
              if (jjtree.nodeCreated()) {
               jjtreeCloseNodeScope(jjtn001);
              }
            }
          if (jj_2_1003(3)) {
Unsupported *jjtn002 = new Unsupported(JJTUNSUPPORTED);
          bool jjtc002 = true;
          jjtree.openNodeScope(jjtn002);
          jjtreeOpenNodeScope(jjtn002);
            try {
              jj_consume_token(AS);
              jj_consume_token(lparen);
              all_fields_column_name_list();
              jj_consume_token(rparen);
            } catch ( ...) {
if (jjtc002) {
            jjtree.clearNodeScope(jjtn002);
            jjtc002 = false;
          } else {
            jjtree.popNode();
          }
            }
if (jjtc002) {
            jjtree.closeNodeScope(jjtn002, true);
            if (jjtree.nodeCreated()) {
             jjtreeCloseNodeScope(jjtn002);
            }
          }
          } else {
            ;
          }
        } else if (jj_2_1005(3)) {
          as_clause();
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::qualified_asterisk() {
    if (jj_2_1007(3)) {
Star *jjtn001 = new Star(JJTSTAR);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        asterisked_identifier_chain();
        jj_consume_token(569);
        jj_consume_token(STAR);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else if (jj_2_1008(3)) {
      all_fields_reference();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::asterisked_identifier_chain() {/*@bgen(jjtree) #QualifiedName(> 1) */
  QualifiedName *jjtn000 = new QualifiedName(JJTQUALIFIEDNAME);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      identifier();
      while (!hasError) {
        if (jj_2_1009(3)) {
          ;
        } else {
          goto end_label_45;
        }
        jj_consume_token(569);
        identifier();
      }
      end_label_45: ;
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::derived_column() {
    value_expression();
    if (jj_2_1010(3)) {
      as_clause();
    } else {
      ;
    }
}


void SqlParser::as_clause() {/*@bgen(jjtree) Alias */
  Alias *jjtn000 = new Alias(JJTALIAS);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1011(3)) {
        jj_consume_token(AS);
      } else {
        ;
      }
      identifier();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::all_fields_reference() {
    value_expression_primary();
    jj_consume_token(569);
    jj_consume_token(STAR);
    if (jj_2_1012(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
        bool jjtc001 = true;
        jjtree.openNodeScope(jjtn001);
        jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(AS);
        jj_consume_token(lparen);
        all_fields_column_name_list();
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc001) {
          jjtree.clearNodeScope(jjtn001);
          jjtc001 = false;
        } else {
          jjtree.popNode();
        }
      }
if (jjtc001) {
          jjtree.closeNodeScope(jjtn001, true);
          if (jjtree.nodeCreated()) {
           jjtreeCloseNodeScope(jjtn001);
          }
        }
    } else {
      ;
    }
}


void SqlParser::all_fields_column_name_list() {
    column_name_list();
}


void SqlParser::query_expression() {/*@bgen(jjtree) #QuerySpecification(> 1) */
  QuerySpecification *jjtn000 = new QuerySpecification(JJTQUERYSPECIFICATION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1013(3)) {
        with_clause();
      } else {
        ;
      }
      query_expression_body();
      if (jj_2_1014(3)) {
        order_by_clause();
      } else {
        ;
      }
      if (jj_2_1019(3)) {
        if (jj_2_1016(3)) {
          limit_clause();
        } else if (jj_2_1017(3)) {
          result_offset_clause();
          if (jj_2_1015(3)) {
            fetch_first_clause();
          } else {
            ;
          }
        } else if (jj_2_1018(3)) {
          fetch_first_clause();
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::with_clause() {/*@bgen(jjtree) WithClause */
  WithClause *jjtn000 = new WithClause(JJTWITHCLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(WITH);
      if (jj_2_1020(3)) {
        jj_consume_token(RECURSIVE);
      } else {
        ;
      }
      with_list();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::with_list() {
    with_list_element();
    while (!hasError) {
      if (jj_2_1021(3)) {
        ;
      } else {
        goto end_label_46;
      }
      jj_consume_token(570);
      with_list_element();
    }
    end_label_46: ;
}


void SqlParser::with_list_element() {/*@bgen(jjtree) Cte */
  Cte *jjtn000 = new Cte(JJTCTE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      identifier();
      if (jj_2_1022(3)) {
        jj_consume_token(lparen);
        column_name_list();
        jj_consume_token(rparen);
      } else {
        ;
      }
      jj_consume_token(AS);
      subquery();
      if (jj_2_1023(3)) {
        search_or_cycle_clause();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::query_expression_body() {/*@bgen(jjtree) #SetOperation(> 1) */
  SetOperation *jjtn000 = new SetOperation(JJTSETOPERATION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      query_term();
      while (!hasError) {
        if (jj_2_1024(3)) {
          ;
        } else {
          goto end_label_47;
        }
        if (jj_2_1033(3)) {
          jj_consume_token(UNION);
          if (jj_2_1027(3)) {
            if (jj_2_1025(3)) {
              jj_consume_token(ALL);
            } else if (jj_2_1026(3)) {
              jj_consume_token(DISTINCT);
            } else {
              jj_consume_token(-1);
              errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
            }
          } else {
            ;
          }
          if (jj_2_1028(3)) {
            corresponding_spec();
          } else {
            ;
          }
          query_term();
        } else if (jj_2_1034(3)) {
          jj_consume_token(EXCEPT);
          if (jj_2_1031(3)) {
            if (jj_2_1029(3)) {
              jj_consume_token(ALL);
            } else if (jj_2_1030(3)) {
              jj_consume_token(DISTINCT);
            } else {
              jj_consume_token(-1);
              errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
            }
          } else {
            ;
          }
          if (jj_2_1032(3)) {
            corresponding_spec();
          } else {
            ;
          }
          query_term();
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      }
      end_label_47: ;
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::query_term() {/*@bgen(jjtree) #SetOperation(> 1) */
  SetOperation *jjtn000 = new SetOperation(JJTSETOPERATION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      query_primary();
      while (!hasError) {
        if (jj_2_1035(3)) {
          ;
        } else {
          goto end_label_48;
        }
        jj_consume_token(INTERSECT);
        if (jj_2_1038(3)) {
          if (jj_2_1036(3)) {
            jj_consume_token(ALL);
          } else if (jj_2_1037(3)) {
            jj_consume_token(DISTINCT);
          } else {
            jj_consume_token(-1);
            errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
          }
        } else {
          ;
        }
        if (jj_2_1039(3)) {
          corresponding_spec();
        } else {
          ;
        }
        query_primary();
      }
      end_label_48: ;
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, jjtree.nodeArity() > 1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::query_primary() {
    if (jj_2_1044(3)) {
Subquery *jjtn001 = new Subquery(JJTSUBQUERY);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(lparen);
        query_expression_body();
        if (jj_2_1040(3)) {
          order_by_clause();
        } else {
          ;
        }
        if (jj_2_1041(3)) {
          limit_clause();
        } else {
          ;
        }
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else if (jj_2_1045(3)) {
      simple_table();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::simple_table() {
    if (jj_2_1046(3)) {
      table_value_constructor();
    } else if (jj_2_1047(3)) {
      explicit_table();
    } else if (jj_2_1048(3)) {
      query_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::explicit_table() {
    jj_consume_token(TABLE);
    table_or_query_name();
}


void SqlParser::corresponding_spec() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(CORRESPONDING);
      if (jj_2_1049(3)) {
        jj_consume_token(BY);
        jj_consume_token(lparen);
        column_name_list();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::order_by_clause() {/*@bgen(jjtree) OrderByClause */
  OrderByClause *jjtn000 = new OrderByClause(JJTORDERBYCLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(ORDER);
      jj_consume_token(BY);
      sort_specification_list();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::result_offset_clause() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(OFFSET);
      simple_value_specification();
      if (jj_2_1050(3)) {
        jj_consume_token(ROW);
      } else if (jj_2_1051(3)) {
        jj_consume_token(ROWS);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::fetch_first_clause() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(FETCH);
      if (jj_2_1052(3)) {
        jj_consume_token(FIRST);
      } else if (jj_2_1053(3)) {
        jj_consume_token(NEXT);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
      if (jj_2_1054(3)) {
        simple_value_specification();
      } else {
        ;
      }
      if (jj_2_1055(3)) {
        jj_consume_token(ROW);
      } else if (jj_2_1056(3)) {
        jj_consume_token(ROWS);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
      jj_consume_token(ONLY);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::search_or_cycle_clause() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1057(3)) {
        search_clause();
      } else if (jj_2_1058(3)) {
        cycle_clause();
      } else if (jj_2_1059(3)) {
        search_clause();
        cycle_clause();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::search_clause() {
    jj_consume_token(SEARCH);
    recursive_search_order();
    jj_consume_token(SET);
    identifier();
}


void SqlParser::recursive_search_order() {
    if (jj_2_1060(3)) {
      jj_consume_token(DEPTH);
      jj_consume_token(FIRST);
      jj_consume_token(BY);
      column_name_list();
    } else if (jj_2_1061(3)) {
      jj_consume_token(BREADTH);
      jj_consume_token(FIRST);
      jj_consume_token(BY);
      column_name_list();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::cycle_clause() {
    jj_consume_token(CYCLE);
    cycle_column_list();
    jj_consume_token(SET);
    identifier();
    jj_consume_token(TO);
    value_expression();
    jj_consume_token(DEFAULT_);
    value_expression();
    jj_consume_token(USING);
    identifier();
}


void SqlParser::cycle_column_list() {
    identifier();
    while (!hasError) {
      if (jj_2_1062(3)) {
        ;
      } else {
        goto end_label_49;
      }
      jj_consume_token(570);
      identifier();
    }
    end_label_49: ;
}


void SqlParser::subquery() {/*@bgen(jjtree) Subquery */
  Subquery *jjtn000 = new Subquery(JJTSUBQUERY);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(lparen);
      query_expression();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::predicate() {
    if (jj_2_1080(3)) {
      exists_predicate();
    } else if (jj_2_1081(3)) {
      unique_predicate();
    } else if (jj_2_1082(3)) {
      row_value_predicand();
      if (jj_2_1079(3)) {
PushNode(PopNode());
        if (jj_2_1063(3)) {
          comparison_predicate();
        } else if (jj_2_1064(3)) {
          between_predicate();
        } else if (jj_2_1065(3)) {
          in_predicate();
        } else if (jj_2_1066(3)) {
          like_predicate();
        } else if (jj_2_1067(3)) {
          similar_predicate();
        } else if (jj_2_1068(3)) {
          regex_like_predicate();
        } else if (jj_2_1069(3)) {
          null_predicate();
        } else if (jj_2_1070(3)) {
          quantified_comparison_predicate();
        } else if (jj_2_1071(3)) {
          normalized_predicate();
        } else if (jj_2_1072(3)) {
          match_predicate();
        } else if (jj_2_1073(3)) {
          overlaps_predicate();
        } else if (jj_2_1074(3)) {
          distinct_predicate();
        } else if (jj_2_1075(3)) {
          member_predicate();
        } else if (jj_2_1076(3)) {
          submultiset_predicate();
        } else if (jj_2_1077(3)) {
          set_predicate();
        } else if (jj_2_1078(3)) {
          type_predicate();
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } else {
        ;
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::comparison_predicate() {
    comparison_predicate_part_2();
}


void SqlParser::comparison_predicate_part_2() {/*@bgen(jjtree) #Comparison( 2) */
  Comparison *jjtn000 = new Comparison(JJTCOMPARISON);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      comp_op();
      row_value_predicand();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000,  2);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::comp_op() {
    if (jj_2_1083(3)) {
      jj_consume_token(EQUAL);
    } else if (jj_2_1084(3)) {
      jj_consume_token(NOT_EQUAL);
    } else if (jj_2_1085(3)) {
      jj_consume_token(LESS_THAN);
    } else if (jj_2_1086(3)) {
      jj_consume_token(GREATER_THAN);
    } else if (jj_2_1087(3)) {
      jj_consume_token(LESS_THAN_OR_EQUAL);
    } else if (jj_2_1088(3)) {
      jj_consume_token(GREATER_THAN_OR_EQUAL);
    } else if (jj_2_1089(3)) {
      jj_consume_token(NOT_EQUAL_2);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::between_predicate() {
    between_predicate_part_2();
}


void SqlParser::between_predicate_part_2() {/*@bgen(jjtree) #Between( 3) */
  Between *jjtn000 = new Between(JJTBETWEEN);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1090(3)) {
        jj_consume_token(NOT);
      } else {
        ;
      }
      jj_consume_token(BETWEEN);
      if (jj_2_1093(3)) {
        if (jj_2_1091(3)) {
          jj_consume_token(ASYMMETRIC);
        } else if (jj_2_1092(3)) {
          jj_consume_token(SYMMETRIC);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } else {
        ;
      }
      row_value_predicand();
      jj_consume_token(AND);
      row_value_predicand();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000,  3);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::in_predicate() {
    in_predicate_part_2();
}


void SqlParser::in_predicate_part_2() {/*@bgen(jjtree) InPredicate */
  InPredicate *jjtn000 = new InPredicate(JJTINPREDICATE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1094(3)) {
        jj_consume_token(NOT);
      } else {
        ;
      }
      jj_consume_token(IN);
      in_predicate_value();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::in_predicate_value() {
    if (jj_2_1095(3)) {
InvalueList *jjtn001 = new InvalueList(JJTINVALUELIST);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(lparen);
        in_value_list();
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else if (jj_2_1096(3)) {
      subquery();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::in_value_list() {
    row_value_expression();
    while (!hasError) {
      if (jj_2_1097(3)) {
        ;
      } else {
        goto end_label_50;
      }
      jj_consume_token(570);
      row_value_expression();
    }
    end_label_50: ;
}


void SqlParser::like_predicate() {
    if (jj_2_1098(3)) {
      character_like_predicate();
    } else if (jj_2_1099(3)) {
      octet_like_predicate();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::character_like_predicate() {
    character_like_predicate_part_2();
}


void SqlParser::character_like_predicate_part_2() {/*@bgen(jjtree) #Like( 2) */
  Like *jjtn000 = new Like(JJTLIKE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1100(3)) {
        jj_consume_token(NOT);
      } else {
        ;
      }
      jj_consume_token(LIKE);
      character_value_expression();
      if (jj_2_1101(3)) {
        jj_consume_token(ESCAPE);
        character_value_expression();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000,  2);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::octet_like_predicate() {
    octet_like_predicate_part_2();
}


void SqlParser::octet_like_predicate_part_2() {/*@bgen(jjtree) #Like( 2) */
  Like *jjtn000 = new Like(JJTLIKE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1102(3)) {
        jj_consume_token(NOT);
      } else {
        ;
      }
      jj_consume_token(LIKE);
      binary_value_expression();
      if (jj_2_1103(3)) {
        jj_consume_token(ESCAPE);
        binary_value_expression();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000,  2);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::similar_predicate() {
    similar_predicate_part_2();
}


void SqlParser::similar_predicate_part_2() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1104(3)) {
        jj_consume_token(NOT);
      } else {
        ;
      }
      jj_consume_token(SIMILAR);
      jj_consume_token(TO);
      character_value_expression();
      if (jj_2_1105(3)) {
        jj_consume_token(ESCAPE);
        character_value_expression();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::regex_like_predicate() {
    regex_like_predicate_part_2();
}


void SqlParser::regex_like_predicate_part_2() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1106(3)) {
        jj_consume_token(NOT);
      } else {
        ;
      }
      jj_consume_token(LIKE_REGEX);
      character_value_expression();
      if (jj_2_1107(3)) {
        jj_consume_token(FLAG);
        character_value_expression();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::null_predicate() {
    null_predicate_part_2();
}


void SqlParser::null_predicate_part_2() {/*@bgen(jjtree) #IsNull( 1) */
  IsNull *jjtn000 = new IsNull(JJTISNULL);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(IS);
      if (jj_2_1108(3)) {
        jj_consume_token(NOT);
      } else {
        ;
      }
      jj_consume_token(NULL_);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000,  1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::quantified_comparison_predicate() {
    quantified_comparison_predicate_part_2();
}


void SqlParser::quantified_comparison_predicate_part_2() {/*@bgen(jjtree) #QuantifiedComparison( 2) */
  QuantifiedComparison *jjtn000 = new QuantifiedComparison(JJTQUANTIFIEDCOMPARISON);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      comp_op();
      if (jj_2_1109(3)) {
        jj_consume_token(ALL);
      } else if (jj_2_1110(3)) {
        jj_consume_token(SOME);
      } else if (jj_2_1111(3)) {
        jj_consume_token(ANY);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
      subquery();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000,  2);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::exists_predicate() {/*@bgen(jjtree) #Exists( 1) */
  Exists *jjtn000 = new Exists(JJTEXISTS);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(EXISTS);
      subquery();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000,  1);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::unique_predicate() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(UNIQUE);
      subquery();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::normalized_predicate() {
    normalized_predicate_part_2();
}


void SqlParser::normalized_predicate_part_2() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(IS);
      if (jj_2_1112(3)) {
        jj_consume_token(NOT);
      } else {
        ;
      }
      if (jj_2_1113(3)) {
        normal_form();
      } else {
        ;
      }
      jj_consume_token(NORMALIZED);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::match_predicate() {
    match_predicate_part_2();
}


void SqlParser::match_predicate_part_2() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(MATCH);
      if (jj_2_1114(3)) {
        jj_consume_token(UNIQUE);
      } else {
        ;
      }
      if (jj_2_1118(3)) {
        if (jj_2_1115(3)) {
          jj_consume_token(SIMPLE);
        } else if (jj_2_1116(3)) {
          jj_consume_token(PARTIAL);
        } else if (jj_2_1117(3)) {
          jj_consume_token(FULL);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } else {
        ;
      }
      subquery();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::overlaps_predicate() {
    overlaps_predicate_part_2();
}


void SqlParser::overlaps_predicate_part_1() {
    row_value_predicand_1();
}


void SqlParser::overlaps_predicate_part_2() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(OVERLAPS);
      row_value_predicand_2();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::row_value_predicand_1() {
    row_value_predicand();
}


void SqlParser::row_value_predicand_2() {
    row_value_predicand();
}


void SqlParser::distinct_predicate() {
    distinct_predicate_part_2();
}


void SqlParser::distinct_predicate_part_2() {/*@bgen(jjtree) #IsDistinct( 2) */
  IsDistinct *jjtn000 = new IsDistinct(JJTISDISTINCT);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(IS);
      if (jj_2_1119(3)) {
        jj_consume_token(NOT);
      } else {
        ;
      }
      jj_consume_token(DISTINCT);
      jj_consume_token(FROM);
      row_value_predicand_4();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000,  2);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::row_value_predicand_3() {
    row_value_predicand();
}


void SqlParser::row_value_predicand_4() {
    row_value_predicand();
}


void SqlParser::member_predicate() {
    member_predicate_part_2();
}


void SqlParser::member_predicate_part_2() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1120(3)) {
        jj_consume_token(NOT);
      } else {
        ;
      }
      jj_consume_token(MEMBER);
      if (jj_2_1121(3)) {
        jj_consume_token(OF);
      } else {
        ;
      }
      multiset_value_expression();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::submultiset_predicate() {
    submultiset_predicate_part_2();
}


void SqlParser::submultiset_predicate_part_2() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1122(3)) {
        jj_consume_token(NOT);
      } else {
        ;
      }
      jj_consume_token(SUBMULTISET);
      if (jj_2_1123(3)) {
        jj_consume_token(OF);
      } else {
        ;
      }
      multiset_value_expression();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::set_predicate() {
    set_predicate_part_2();
}


void SqlParser::set_predicate_part_2() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(IS);
      if (jj_2_1124(3)) {
        jj_consume_token(NOT);
      } else {
        ;
      }
      jj_consume_token(A);
      jj_consume_token(SET);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::type_predicate() {
    type_predicate_part_2();
}


void SqlParser::type_predicate_part_2() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(IS);
      if (jj_2_1125(3)) {
        jj_consume_token(NOT);
      } else {
        ;
      }
      jj_consume_token(OF);
      jj_consume_token(lparen);
      type_list();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::type_list() {
    user_defined_type_specification();
    while (!hasError) {
      if (jj_2_1126(3)) {
        ;
      } else {
        goto end_label_51;
      }
      jj_consume_token(570);
      user_defined_type_specification();
    }
    end_label_51: ;
}


void SqlParser::user_defined_type_specification() {
    if (jj_2_1127(3)) {
      exclusive_user_defined_type_specification();
    } else if (jj_2_1128(3)) {
      inclusive_user_defined_type_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::inclusive_user_defined_type_specification() {
    path_resolved_user_defined_type_name();
}


void SqlParser::exclusive_user_defined_type_specification() {
    jj_consume_token(ONLY);
    path_resolved_user_defined_type_name();
}


void SqlParser::search_condition() {
    boolean_value_expression();
}


void SqlParser::interval_qualifier() {/*@bgen(jjtree) InvervalQualifier */
  InvervalQualifier *jjtn000 = new InvervalQualifier(JJTINVERVALQUALIFIER);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1129(3)) {
        start_field();
        jj_consume_token(TO);
        end_field();
      } else if (jj_2_1130(3)) {
        single_datetime_field();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::start_field() {/*@bgen(jjtree) NonSecondField */
  NonSecondField *jjtn000 = new NonSecondField(JJTNONSECONDFIELD);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      non_second_primary_datetime_field();
      if (jj_2_1131(3)) {
        jj_consume_token(lparen);
        interval_leading_field_precision();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::end_field() {
    if (jj_2_1133(3)) {
      non_second_primary_datetime_field();
    } else if (jj_2_1134(3)) {
SecondField *jjtn001 = new SecondField(JJTSECONDFIELD);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(SECOND);
        if (jj_2_1132(3)) {
          jj_consume_token(lparen);
          interval_fractional_seconds_precision();
          jj_consume_token(rparen);
        } else {
          ;
        }
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::single_datetime_field() {
    if (jj_2_1137(3)) {
      start_field();
    } else if (jj_2_1138(3)) {
SecondField *jjtn001 = new SecondField(JJTSECONDFIELD);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(SECOND);
        if (jj_2_1136(3)) {
          jj_consume_token(lparen);
          interval_leading_field_precision();
          if (jj_2_1135(3)) {
            jj_consume_token(570);
            interval_fractional_seconds_precision();
          } else {
            ;
          }
          jj_consume_token(rparen);
        } else {
          ;
        }
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::primary_datetime_field() {
    if (jj_2_1139(3)) {
      non_second_primary_datetime_field();
    } else if (jj_2_1140(3)) {
      jj_consume_token(SECOND);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::non_second_primary_datetime_field() {/*@bgen(jjtree) NonSecondDateTimeField */
  NonSecondDateTimeField *jjtn000 = new NonSecondDateTimeField(JJTNONSECONDDATETIMEFIELD);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1141(3)) {
        jj_consume_token(YEAR);
      } else if (jj_2_1142(3)) {
        jj_consume_token(MONTH);
      } else if (jj_2_1143(3)) {
        jj_consume_token(DAY);
      } else if (jj_2_1144(3)) {
        jj_consume_token(HOUR);
      } else if (jj_2_1145(3)) {
        jj_consume_token(MINUTE);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::interval_fractional_seconds_precision() {
    jj_consume_token(unsigned_integer);
}


void SqlParser::interval_leading_field_precision() {
    jj_consume_token(unsigned_integer);
}


void SqlParser::language_clause() {/*@bgen(jjtree) LanguageClause */
  LanguageClause *jjtn000 = new LanguageClause(JJTLANGUAGECLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(LANGUAGE);
      language_name();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::language_name() {
    if (jj_2_1146(3)) {
      jj_consume_token(ADA);
    } else if (jj_2_1147(3)) {
      jj_consume_token(C);
    } else if (jj_2_1148(3)) {
      jj_consume_token(COBOL);
    } else if (jj_2_1149(3)) {
      jj_consume_token(FORTRAN);
    } else if (jj_2_1150(3)) {
      jj_consume_token(M);
    } else if (jj_2_1151(3)) {
      jj_consume_token(MUMPS);
    } else if (jj_2_1152(3)) {
      jj_consume_token(PASCAL);
    } else if (jj_2_1153(3)) {
      jj_consume_token(PLI);
    } else if (jj_2_1154(3)) {
      jj_consume_token(SQL);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::path_specification() {
    jj_consume_token(PATH);
    schema_name_list();
}


void SqlParser::schema_name_list() {
    schema_name();
    while (!hasError) {
      if (jj_2_1155(3)) {
        ;
      } else {
        goto end_label_52;
      }
      jj_consume_token(570);
      schema_name();
    }
    end_label_52: ;
}


void SqlParser::routine_invocation() {
    routine_name();
    SQL_argument_list();
}


void SqlParser::routine_name() {
    if (jj_2_1156(3)) {
      schema_name();
      jj_consume_token(569);
    } else {
      ;
    }
    identifier();
}


void SqlParser::SQL_argument_list() {/*@bgen(jjtree) ArgumentList */
  ArgumentList *jjtn000 = new ArgumentList(JJTARGUMENTLIST);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(lparen);
      if (jj_2_1158(3)) {
        SQL_argument();
        while (!hasError) {
          if (jj_2_1157(3)) {
            ;
          } else {
            goto end_label_53;
          }
          jj_consume_token(570);
          SQL_argument();
        }
        end_label_53: ;
      } else {
        ;
      }
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
         jjtree.clearNodeScope(jjtn000);
         jjtc000 = false;
       } else {
         jjtree.popNode();
       }
    }
if (jjtc000) {
         jjtree.closeNodeScope(jjtn000, true);
         if (jjtree.nodeCreated()) {
          jjtreeCloseNodeScope(jjtn000);
         }
       }
}


void SqlParser::SQL_argument() {
    if (jj_2_1160(2147483647)) {
      lambda();
    } else if (jj_2_1161(3)) {
      value_expression();
      if (jj_2_1159(3)) {
        generalized_expression();
      } else {
        ;
      }
    } else if (jj_2_1162(3)) {
      named_argument_specification();
    } else if (jj_2_1163(3)) {
      contextually_typed_value_specification();
    } else if (jj_2_1164(3)) {
      target_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::generalized_expression() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(AS);
      path_resolved_user_defined_type_name();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::named_argument_specification() {/*@bgen(jjtree) NamedArgument */
  NamedArgument *jjtn000 = new NamedArgument(JJTNAMEDARGUMENT);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      identifier();
      jj_consume_token(584);
      named_argument_SQL_argument();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::named_argument_SQL_argument() {
    if (jj_2_1165(3)) {
      value_expression();
    } else if (jj_2_1166(3)) {
      contextually_typed_value_specification();
    } else if (jj_2_1167(3)) {
      target_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::character_set_specification() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1168(3)) {
        standard_character_set_name();
      } else if (jj_2_1169(3)) {
        implementation_defined_character_set_name();
      } else if (jj_2_1170(3)) {
        user_defined_character_set_name();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::standard_character_set_name() {
    character_set_name();
}


void SqlParser::implementation_defined_character_set_name() {
    character_set_name();
}


void SqlParser::user_defined_character_set_name() {
    character_set_name();
}


void SqlParser::specific_routine_designator() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1172(3)) {
        jj_consume_token(SPECIFIC);
        routine_type();
        schema_qualified_name();
      } else if (jj_2_1173(3)) {
        routine_type();
        member_name();
        if (jj_2_1171(3)) {
          jj_consume_token(FOR);
          schema_resolved_user_defined_type_name();
        } else {
          ;
        }
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::routine_type() {
    if (jj_2_1178(3)) {
      jj_consume_token(ROUTINE);
    } else if (jj_2_1179(3)) {
      jj_consume_token(FUNCTION);
    } else if (jj_2_1180(3)) {
      jj_consume_token(PROCEDURE);
    } else if (jj_2_1181(3)) {
      if (jj_2_1177(3)) {
        if (jj_2_1174(3)) {
          jj_consume_token(INSTANCE);
        } else if (jj_2_1175(3)) {
          jj_consume_token(STATIC);
        } else if (jj_2_1176(3)) {
          jj_consume_token(CONSTRUCTOR);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } else {
        ;
      }
      jj_consume_token(METHOD);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::member_name() {
    member_name_alternatives();
    if (jj_2_1182(3)) {
      data_type_list();
    } else {
      ;
    }
}


void SqlParser::member_name_alternatives() {
    if (jj_2_1183(3)) {
      schema_qualified_name();
    } else if (jj_2_1184(3)) {
      identifier();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::data_type_list() {
    jj_consume_token(lparen);
    if (jj_2_1186(3)) {
      data_type();
      while (!hasError) {
        if (jj_2_1185(3)) {
          ;
        } else {
          goto end_label_54;
        }
        jj_consume_token(570);
        data_type();
      }
      end_label_54: ;
    } else {
      ;
    }
    jj_consume_token(rparen);
}


void SqlParser::collate_clause() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(COLLATE);
      schema_qualified_name();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::constraint_name_definition() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(CONSTRAINT);
      schema_qualified_name();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::constraint_characteristics() {
    if (jj_2_1193(3)) {
      constraint_check_time();
      if (jj_2_1188(3)) {
        if (jj_2_1187(3)) {
          jj_consume_token(NOT);
        } else {
          ;
        }
        jj_consume_token(DEFERRABLE);
      } else {
        ;
      }
      if (jj_2_1189(3)) {
        constraint_enforcement();
      } else {
        ;
      }
    } else if (jj_2_1194(3)) {
      if (jj_2_1190(3)) {
        jj_consume_token(NOT);
      } else {
        ;
      }
      jj_consume_token(DEFERRABLE);
      if (jj_2_1191(3)) {
        constraint_check_time();
      } else {
        ;
      }
      if (jj_2_1192(3)) {
        constraint_enforcement();
      } else {
        ;
      }
    } else if (jj_2_1195(3)) {
      constraint_enforcement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::constraint_check_time() {
    if (jj_2_1196(3)) {
      jj_consume_token(INITIALLY);
      jj_consume_token(DEFERRED);
    } else if (jj_2_1197(3)) {
      jj_consume_token(INITIALLY);
      jj_consume_token(IMMEDIATE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::constraint_enforcement() {
    if (jj_2_1198(3)) {
      jj_consume_token(NOT);
    } else {
      ;
    }
    jj_consume_token(ENFORCED);
}


void SqlParser::aggregate_function() {/*@bgen(jjtree) AggregationFunction */
  AggregationFunction *jjtn000 = new AggregationFunction(JJTAGGREGATIONFUNCTION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1199(3)) {
        jj_consume_token(COUNT);
        jj_consume_token(lparen);
        jj_consume_token(STAR);
        jj_consume_token(rparen);
      } else if (jj_2_1200(3)) {
        count();
      } else if (jj_2_1201(3)) {
        general_set_function();
      } else if (jj_2_1202(3)) {
        binary_set_function();
      } else if (jj_2_1203(3)) {
        ordered_set_function();
      } else if (jj_2_1204(3)) {
        array_aggregate_function();
      } else if (jj_2_1205(3)) {
        presto_aggregations();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
      if (jj_2_1206(3)) {
        filter_clause();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
      jjtree.clearNodeScope(jjtn000);
      jjtc000 = false;
    } else {
      jjtree.popNode();
    }
    }
if (jjtc000) {
      jjtree.closeNodeScope(jjtn000, true);
      if (jjtree.nodeCreated()) {
       jjtreeCloseNodeScope(jjtn000);
      }
    }
}


void SqlParser::general_set_function() {
    set_function_type();
    jj_consume_token(lparen);
    if (jj_2_1207(3)) {
      set_quantifier();
    } else {
      ;
    }
    value_expression();
    if (jj_2_1208(3)) {
      extra_args_to_agg();
    } else {
      ;
    }
    jj_consume_token(rparen);
}


void SqlParser::set_function_type() {
    computational_operation();
}


void SqlParser::computational_operation() {
    if (jj_2_1209(3)) {
      jj_consume_token(AVG);
    } else if (jj_2_1210(3)) {
      jj_consume_token(MAX);
    } else if (jj_2_1211(3)) {
      jj_consume_token(MIN);
    } else if (jj_2_1212(3)) {
      jj_consume_token(SUM);
    } else if (jj_2_1213(3)) {
      jj_consume_token(EVERY);
    } else if (jj_2_1214(3)) {
      jj_consume_token(ANY);
    } else if (jj_2_1215(3)) {
      jj_consume_token(SOME);
    } else if (jj_2_1216(3)) {
      jj_consume_token(COUNT);
    } else if (jj_2_1217(3)) {
      jj_consume_token(STDDEV_POP);
    } else if (jj_2_1218(3)) {
      jj_consume_token(STDDEV_SAMP);
    } else if (jj_2_1219(3)) {
      jj_consume_token(VAR_SAMP);
    } else if (jj_2_1220(3)) {
      jj_consume_token(VAR_POP);
    } else if (jj_2_1221(3)) {
      jj_consume_token(COLLECT);
    } else if (jj_2_1222(3)) {
      jj_consume_token(FUSION);
    } else if (jj_2_1223(3)) {
      jj_consume_token(INTERSECTION);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::set_quantifier() {/*@bgen(jjtree) SetQuantifier */
  SetQuantifier *jjtn000 = new SetQuantifier(JJTSETQUANTIFIER);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1224(3)) {
        jj_consume_token(DISTINCT);
      } else if (jj_2_1225(3)) {
        jj_consume_token(ALL);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::filter_clause() {/*@bgen(jjtree) FilterClause */
  FilterClause *jjtn000 = new FilterClause(JJTFILTERCLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(FILTER);
      jj_consume_token(lparen);
      jj_consume_token(WHERE);
      search_condition();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::binary_set_function() {
    binary_set_function_type();
    jj_consume_token(lparen);
    dependent_variable_expression();
    jj_consume_token(570);
    independent_variable_expression();
    jj_consume_token(rparen);
}


void SqlParser::binary_set_function_type() {
    if (jj_2_1226(3)) {
      jj_consume_token(COVAR_POP);
    } else if (jj_2_1227(3)) {
      jj_consume_token(COVAR_SAMP);
    } else if (jj_2_1228(3)) {
      jj_consume_token(CORR);
    } else if (jj_2_1229(3)) {
      jj_consume_token(REGR_SLOPE);
    } else if (jj_2_1230(3)) {
      jj_consume_token(REGR_INTERCEPT);
    } else if (jj_2_1231(3)) {
      jj_consume_token(REGR_COUNT);
    } else if (jj_2_1232(3)) {
      jj_consume_token(REGR_R2);
    } else if (jj_2_1233(3)) {
      jj_consume_token(REGR_AVGX);
    } else if (jj_2_1234(3)) {
      jj_consume_token(REGR_AVGY);
    } else if (jj_2_1235(3)) {
      jj_consume_token(REGR_SXX);
    } else if (jj_2_1236(3)) {
      jj_consume_token(REGR_SYY);
    } else if (jj_2_1237(3)) {
      jj_consume_token(REGR_SXY);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::dependent_variable_expression() {
    numeric_value_expression();
}


void SqlParser::independent_variable_expression() {
    numeric_value_expression();
}


void SqlParser::ordered_set_function() {
    if (jj_2_1238(3)) {
      hypothetical_set_function();
    } else if (jj_2_1239(3)) {
      inverse_distribution_function();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::hypothetical_set_function() {
    rank_function_type();
    jj_consume_token(lparen);
    hypothetical_set_function_value_expression_list();
    jj_consume_token(rparen);
    within_group_specification();
}


void SqlParser::within_group_specification() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(WITHIN);
      jj_consume_token(GROUP);
      jj_consume_token(lparen);
      jj_consume_token(ORDER);
      jj_consume_token(BY);
      sort_specification_list();
      jj_consume_token(rparen);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::hypothetical_set_function_value_expression_list() {
    value_expression();
    while (!hasError) {
      if (jj_2_1240(3)) {
        ;
      } else {
        goto end_label_55;
      }
      jj_consume_token(570);
      value_expression();
    }
    end_label_55: ;
}


void SqlParser::inverse_distribution_function() {
    inverse_distribution_function_type();
    jj_consume_token(lparen);
    inverse_distribution_function_argument();
    jj_consume_token(rparen);
    within_group_specification();
}


void SqlParser::inverse_distribution_function_argument() {
    numeric_value_expression();
}


void SqlParser::inverse_distribution_function_type() {
    if (jj_2_1241(3)) {
      jj_consume_token(PERCENTILE_CONT);
    } else if (jj_2_1242(3)) {
      jj_consume_token(PERCENTILE_DISC);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::array_aggregate_function() {
    jj_consume_token(ARRAY_AGG);
    jj_consume_token(lparen);
    if (jj_2_1243(3)) {
      distinct();
    } else {
      ;
    }
    value_expression();
    if (jj_2_1244(3)) {
      jj_consume_token(ORDER);
      jj_consume_token(BY);
      sort_specification_list();
    } else {
      ;
    }
    jj_consume_token(rparen);
}


void SqlParser::sort_specification_list() {/*@bgen(jjtree) SortSpecificationList */
  SortSpecificationList *jjtn000 = new SortSpecificationList(JJTSORTSPECIFICATIONLIST);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      sort_specification();
      while (!hasError) {
        if (jj_2_1245(3)) {
          ;
        } else {
          goto end_label_56;
        }
        jj_consume_token(570);
        sort_specification();
      }
      end_label_56: ;
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::sort_specification() {/*@bgen(jjtree) SortSpecification */
  SortSpecification *jjtn000 = new SortSpecification(JJTSORTSPECIFICATION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      sort_key();
      if (jj_2_1246(3)) {
        ordering_specification();
      } else {
        ;
      }
      if (jj_2_1247(3)) {
        null_ordering();
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::sort_key() {
    value_expression();
}


void SqlParser::ordering_specification() {/*@bgen(jjtree) OrderingDirection */
  OrderingDirection *jjtn000 = new OrderingDirection(JJTORDERINGDIRECTION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1248(3)) {
        jj_consume_token(ASC);
      } else if (jj_2_1249(3)) {
        jj_consume_token(DESC);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::null_ordering() {/*@bgen(jjtree) NullOrdering */
  NullOrdering *jjtn000 = new NullOrdering(JJTNULLORDERING);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1250(3)) {
        jj_consume_token(NULLS);
        jj_consume_token(FIRST);
      } else if (jj_2_1251(3)) {
        jj_consume_token(NULLS);
        jj_consume_token(LAST);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::schema_definition() {/*@bgen(jjtree) CreateSchema */
  CreateSchema *jjtn000 = new CreateSchema(JJTCREATESCHEMA);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(CREATE);
      jj_consume_token(SCHEMA);
      if (jj_2_1252(3)) {
        if_not_exists();
      } else {
        ;
      }
      schema_name_clause();
      if (jj_2_1253(3)) {
        schema_character_set_or_path();
      } else {
        ;
      }
      while (!hasError) {
        if (jj_2_1254(3)) {
          ;
        } else {
          goto end_label_57;
        }
        schema_element();
      }
      end_label_57: ;
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::schema_character_set_or_path() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1255(3)) {
        schema_character_set_specification();
      } else if (jj_2_1256(3)) {
        schema_path_specification();
      } else if (jj_2_1257(3)) {
        schema_character_set_specification();
        schema_path_specification();
      } else if (jj_2_1258(3)) {
        schema_path_specification();
        schema_character_set_specification();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::schema_name_clause() {
    if (jj_2_1259(3)) {
      schema_name();
    } else if (jj_2_1260(3)) {
Unsuppoerted *jjtn001 = new Unsuppoerted(JJTUNSUPPOERTED);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(AUTHORIZATION);
        identifier();
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else if (jj_2_1261(3)) {
Unsuppoerted *jjtn002 = new Unsuppoerted(JJTUNSUPPOERTED);
      bool jjtc002 = true;
      jjtree.openNodeScope(jjtn002);
      jjtreeOpenNodeScope(jjtn002);
      try {
        schema_name();
        jj_consume_token(AUTHORIZATION);
        identifier();
      } catch ( ...) {
if (jjtc002) {
        jjtree.clearNodeScope(jjtn002);
        jjtc002 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc002) {
        jjtree.closeNodeScope(jjtn002, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn002);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::schema_character_set_specification() {
    jj_consume_token(DEFAULT_);
    jj_consume_token(CHARACTER);
    jj_consume_token(SET);
    character_set_specification();
}


void SqlParser::schema_path_specification() {
    path_specification();
}


void SqlParser::schema_element() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1262(3)) {
        table_definition();
      } else if (jj_2_1263(4)) {
        view_definition();
      } else if (jj_2_1264(3)) {
        domain_definition();
      } else if (jj_2_1265(3)) {
        character_set_definition();
      } else if (jj_2_1266(3)) {
        collation_definition();
      } else if (jj_2_1267(3)) {
        transliteration_definition();
      } else if (jj_2_1268(3)) {
        assertion_definition();
      } else if (jj_2_1269(3)) {
        trigger_definition();
      } else if (jj_2_1270(3)) {
        user_defined_type_definition();
      } else if (jj_2_1271(3)) {
        user_defined_cast_definition();
      } else if (jj_2_1272(3)) {
        user_defined_ordering_definition();
      } else if (jj_2_1273(3)) {
        transform_definition();
      } else if (jj_2_1274(3)) {
        schema_routine();
      } else if (jj_2_1275(3)) {
        sequence_generator_definition();
      } else if (jj_2_1276(3)) {
        grant_statement();
      } else if (jj_2_1277(3)) {
        role_definition();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::drop_schema_statement() {
    jj_consume_token(DROP);
    jj_consume_token(SCHEMA);
    schema_name();
    drop_behavior();
}


void SqlParser::drop_behavior() {
    if (jj_2_1278(3)) {
      jj_consume_token(CASCADE);
    } else if (jj_2_1279(3)) {
      jj_consume_token(RESTRICT);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::table_definition() {/*@bgen(jjtree) TableDefinition */
  TableDefinition *jjtn000 = new TableDefinition(JJTTABLEDEFINITION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(CREATE);
      if (jj_2_1280(3)) {
        table_scope();
      } else {
        ;
      }
      jj_consume_token(TABLE);
      if (jj_2_1281(3)) {
        if_not_exists();
      } else {
        ;
      }
      table_name();
      if (jj_2_1282(3)) {
        table_description();
      } else {
        ;
      }
      if (jj_2_1290(2147483647)) {
        if (jj_2_1285(3)) {
          jj_consume_token(WITH);
          if (jj_2_1283(3)) {
            system_versioning_clause();
          } else if (jj_2_1284(3)) {
            table_attributes();
          } else {
            jj_consume_token(-1);
            errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
          }
        } else {
          ;
        }
        table_contents_source();
      } else if (jj_2_1291(3)) {
        table_contents_source();
        if (jj_2_1286(3)) {
          table_description();
        } else {
          ;
        }
        if (jj_2_1289(3)) {
          jj_consume_token(WITH);
          if (jj_2_1287(3)) {
            system_versioning_clause();
          } else if (jj_2_1288(3)) {
            table_attributes();
          } else {
            jj_consume_token(-1);
            errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
          }
        } else {
          ;
        }
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
      if (jj_2_1292(3)) {
        jj_consume_token(ON);
        jj_consume_token(COMMIT);
        table_commit_action();
        jj_consume_token(ROWS);
      } else {
        ;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::table_contents_source() {
    if (jj_2_1293(3)) {
      typed_table_clause();
    } else if (jj_2_1294(3)) {
      as_subquery_clause();
    } else if (jj_2_1295(3)) {
      table_element_list();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::table_scope() {
    global_or_local();
    jj_consume_token(TEMPORARY);
}


void SqlParser::global_or_local() {
    if (jj_2_1296(3)) {
      jj_consume_token(GLOBAL);
    } else if (jj_2_1297(3)) {
      jj_consume_token(LOCAL);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::system_versioning_clause() {
    jj_consume_token(SYSTEM);
    jj_consume_token(VERSIONING);
    if (jj_2_1298(3)) {
      retention_period_specification();
    } else {
      ;
    }
}


void SqlParser::retention_period_specification() {
    if (jj_2_1299(3)) {
      jj_consume_token(KEEP);
      jj_consume_token(VERSIONS);
      jj_consume_token(FOREVER);
    } else if (jj_2_1300(3)) {
      jj_consume_token(KEEP);
      jj_consume_token(VERSIONS);
      jj_consume_token(FOR);
      length_of_time();
      time_unit();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::length_of_time() {
    jj_consume_token(unsigned_integer);
}


void SqlParser::time_unit() {
    if (jj_2_1301(3)) {
      jj_consume_token(SECOND);
    } else if (jj_2_1302(3)) {
      jj_consume_token(SECONDS);
    } else if (jj_2_1303(3)) {
      jj_consume_token(MINUTE);
    } else if (jj_2_1304(3)) {
      jj_consume_token(MINUTES);
    } else if (jj_2_1305(3)) {
      jj_consume_token(HOUR);
    } else if (jj_2_1306(3)) {
      jj_consume_token(HOURS);
    } else if (jj_2_1307(3)) {
      jj_consume_token(DAY);
    } else if (jj_2_1308(3)) {
      jj_consume_token(DAYS);
    } else if (jj_2_1309(3)) {
      jj_consume_token(MONTH);
    } else if (jj_2_1310(3)) {
      jj_consume_token(MONTHS);
    } else if (jj_2_1311(3)) {
      jj_consume_token(YEAR);
    } else if (jj_2_1312(3)) {
      jj_consume_token(YEARS);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::table_commit_action() {
    if (jj_2_1313(3)) {
      jj_consume_token(PRESERVE);
    } else if (jj_2_1314(3)) {
      jj_consume_token(DELETE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::table_element_list() {
    jj_consume_token(lparen);
    table_element();
    while (!hasError) {
      if (jj_2_1315(3)) {
        ;
      } else {
        goto end_label_58;
      }
      jj_consume_token(570);
      table_element();
    }
    end_label_58: ;
    jj_consume_token(rparen);
}


void SqlParser::table_element() {
    if (jj_2_1316(3)) {
      column_definition();
    } else if (jj_2_1317(3)) {
      table_constraint_definition();
    } else if (jj_2_1318(3)) {
      like_clause();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::typed_table_clause() {
    jj_consume_token(OF);
    path_resolved_user_defined_type_name();
    if (jj_2_1319(3)) {
      subtable_clause();
    } else {
      ;
    }
    if (jj_2_1320(3)) {
      typed_table_element_list();
    } else {
      ;
    }
}


void SqlParser::typed_table_element_list() {
    jj_consume_token(lparen);
    typed_table_element();
    while (!hasError) {
      if (jj_2_1321(3)) {
        ;
      } else {
        goto end_label_59;
      }
      jj_consume_token(570);
      typed_table_element();
    }
    end_label_59: ;
    jj_consume_token(rparen);
}


void SqlParser::typed_table_element() {
    if (jj_2_1322(3)) {
      column_options();
    } else if (jj_2_1323(3)) {
      table_constraint_definition();
    } else if (jj_2_1324(3)) {
      self_referencing_column_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::self_referencing_column_specification() {
    jj_consume_token(REF);
    jj_consume_token(IS);
    identifier();
    if (jj_2_1325(3)) {
      reference_generation();
    } else {
      ;
    }
}


void SqlParser::reference_generation() {
    if (jj_2_1326(3)) {
      jj_consume_token(SYSTEM);
      jj_consume_token(GENERATED);
    } else if (jj_2_1327(3)) {
      jj_consume_token(USER);
      jj_consume_token(GENERATED);
    } else if (jj_2_1328(3)) {
      jj_consume_token(DERIVED);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::column_options() {
    identifier();
    jj_consume_token(WITH);
    jj_consume_token(OPTIONS);
    column_option_list();
}


void SqlParser::column_option_list() {
    if (jj_2_1329(3)) {
      scope_clause();
    } else {
      ;
    }
    if (jj_2_1330(3)) {
      default_clause();
    } else {
      ;
    }
    while (!hasError) {
      if (jj_2_1331(3)) {
        ;
      } else {
        goto end_label_60;
      }
      column_constraint_definition();
    }
    end_label_60: ;
}


void SqlParser::subtable_clause() {
    jj_consume_token(UNDER);
    supertable_clause();
}


void SqlParser::supertable_clause() {
    supertable_name();
}


void SqlParser::supertable_name() {
    table_name();
}


void SqlParser::like_clause() {
    jj_consume_token(LIKE);
    table_name();
    if (jj_2_1332(3)) {
      like_options();
    } else {
      ;
    }
}


void SqlParser::like_options() {
    while (!hasError) {
      like_option();
      if (jj_2_1333(3)) {
        ;
      } else {
        goto end_label_61;
      }
    }
    end_label_61: ;
}


void SqlParser::like_option() {
    if (jj_2_1334(3)) {
      identity_option();
    } else if (jj_2_1335(3)) {
      column_default_option();
    } else if (jj_2_1336(3)) {
      generation_option();
    } else if (jj_2_1337(3)) {
      jj_consume_token(INCLUDING);
      jj_consume_token(PROPERTIES);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::identity_option() {
    if (jj_2_1338(3)) {
      jj_consume_token(INCLUDING);
      jj_consume_token(IDENTITY);
    } else if (jj_2_1339(3)) {
      jj_consume_token(EXCLUDING);
      jj_consume_token(IDENTITY);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::column_default_option() {
    if (jj_2_1340(3)) {
      jj_consume_token(INCLUDING);
      jj_consume_token(DEFAULTS);
    } else if (jj_2_1341(3)) {
      jj_consume_token(EXCLUDING);
      jj_consume_token(DEFAULTS);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::generation_option() {
    if (jj_2_1342(3)) {
      jj_consume_token(INCLUDING);
      jj_consume_token(GENERATED);
    } else if (jj_2_1343(3)) {
      jj_consume_token(EXCLUDING);
      jj_consume_token(GENERATED);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::as_subquery_clause() {
    if (jj_2_1344(3)) {
      jj_consume_token(lparen);
      column_name_list();
      jj_consume_token(rparen);
    } else {
      ;
    }
    if (jj_2_1345(3)) {
      jj_consume_token(WITH);
      table_attributes();
    } else {
      ;
    }
    jj_consume_token(AS);
    if (jj_2_1346(3)) {
      subquery();
    } else if (jj_2_1347(3)) {
      query_expression();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
    if (jj_2_1348(3)) {
      with_or_without_data();
    } else {
      ;
    }
}


void SqlParser::with_or_without_data() {
    if (jj_2_1349(3)) {
      jj_consume_token(WITH);
      jj_consume_token(NO);
      jj_consume_token(DATA);
    } else if (jj_2_1350(3)) {
      jj_consume_token(WITH);
      jj_consume_token(DATA);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::column_definition() {
    identifier();
    if (jj_2_1351(3)) {
      data_type_or_schema_qualified_name();
    } else {
      ;
    }
    if (jj_2_1357(3)) {
      if (jj_2_1352(3)) {
        default_clause();
      } else if (jj_2_1353(3)) {
        identity_column_specification();
      } else if (jj_2_1354(3)) {
        generation_clause();
      } else if (jj_2_1355(3)) {
        system_version_start_column_specification();
      } else if (jj_2_1356(3)) {
        system_version_end_column_specification();
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } else {
      ;
    }
    while (!hasError) {
      if (jj_2_1358(3)) {
        ;
      } else {
        goto end_label_62;
      }
      column_constraint_definition();
    }
    end_label_62: ;
    if (jj_2_1359(3)) {
      collate_clause();
    } else {
      ;
    }
    if (jj_2_1360(3)) {
      column_description();
    } else {
      ;
    }
}


void SqlParser::data_type_or_schema_qualified_name() {
    if (jj_2_1361(3)) {
      data_type();
    } else if (jj_2_1362(3)) {
      schema_qualified_name();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::system_version_start_column_specification() {
    timestamp_generation_rule();
    jj_consume_token(AS);
    jj_consume_token(SYSTEM);
    jj_consume_token(VERSION);
    jj_consume_token(START);
}


void SqlParser::system_version_end_column_specification() {
    timestamp_generation_rule();
    jj_consume_token(AS);
    jj_consume_token(SYSTEM);
    jj_consume_token(VERSION);
    jj_consume_token(END);
}


void SqlParser::timestamp_generation_rule() {
    jj_consume_token(GENERATED);
    jj_consume_token(ALWAYS);
}


void SqlParser::column_constraint_definition() {
    if (jj_2_1363(3)) {
      constraint_name_definition();
    } else {
      ;
    }
    column_constraint();
    if (jj_2_1364(3)) {
      constraint_characteristics();
    } else {
      ;
    }
}


void SqlParser::column_constraint() {
    if (jj_2_1365(3)) {
      jj_consume_token(NOT);
      jj_consume_token(NULL_);
    } else if (jj_2_1366(3)) {
      unique_specification();
    } else if (jj_2_1367(3)) {
      references_specification();
    } else if (jj_2_1368(3)) {
      check_constraint_definition();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::identity_column_specification() {
    jj_consume_token(GENERATED);
    if (jj_2_1369(3)) {
      jj_consume_token(ALWAYS);
    } else if (jj_2_1370(3)) {
      jj_consume_token(BY);
      jj_consume_token(DEFAULT_);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
    jj_consume_token(AS);
    jj_consume_token(IDENTITY);
    if (jj_2_1371(3)) {
      jj_consume_token(lparen);
      common_sequence_generator_options();
      jj_consume_token(rparen);
    } else {
      ;
    }
}


void SqlParser::generation_clause() {
    generation_rule();
    jj_consume_token(AS);
    generation_expression();
}


void SqlParser::generation_rule() {
    jj_consume_token(GENERATED);
    jj_consume_token(ALWAYS);
}


void SqlParser::generation_expression() {
    jj_consume_token(lparen);
    value_expression();
    jj_consume_token(rparen);
}


void SqlParser::default_clause() {
    jj_consume_token(DEFAULT_);
    default_option();
}


void SqlParser::default_option() {
    if (jj_2_1372(3)) {
      literal();
    } else if (jj_2_1373(3)) {
      datetime_value_function();
    } else if (jj_2_1374(3)) {
      jj_consume_token(USER);
    } else if (jj_2_1375(3)) {
      jj_consume_token(CURRENT_USER);
    } else if (jj_2_1376(3)) {
      jj_consume_token(CURRENT_ROLE);
    } else if (jj_2_1377(3)) {
      jj_consume_token(SESSION_USER);
    } else if (jj_2_1378(3)) {
      jj_consume_token(SYSTEM_USER);
    } else if (jj_2_1379(3)) {
      jj_consume_token(CURRENT_CATALOG);
    } else if (jj_2_1380(3)) {
      jj_consume_token(CURRENT_SCHEMA);
    } else if (jj_2_1381(3)) {
      jj_consume_token(CURRENT_PATH);
    } else if (jj_2_1382(3)) {
      implicitly_typed_value_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::table_constraint_definition() {
    if (jj_2_1383(3)) {
      constraint_name_definition();
    } else {
      ;
    }
    table_constraint();
    if (jj_2_1384(3)) {
      constraint_characteristics();
    } else {
      ;
    }
}


void SqlParser::table_constraint() {
    if (jj_2_1385(3)) {
      unique_constraint_definition();
    } else if (jj_2_1386(3)) {
      referential_constraint_definition();
    } else if (jj_2_1387(3)) {
      check_constraint_definition();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::unique_constraint_definition() {
    if (jj_2_1388(3)) {
      unique_specification();
      jj_consume_token(lparen);
      unique_column_list();
      jj_consume_token(rparen);
    } else if (jj_2_1389(3)) {
      jj_consume_token(UNIQUE);
      jj_consume_token(VALUE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::unique_specification() {
    if (jj_2_1390(3)) {
      jj_consume_token(UNIQUE);
    } else if (jj_2_1391(3)) {
      jj_consume_token(PRIMARY);
      jj_consume_token(KEY);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::unique_column_list() {
    column_name_list();
}


void SqlParser::referential_constraint_definition() {
    jj_consume_token(FOREIGN);
    jj_consume_token(KEY);
    jj_consume_token(lparen);
    referencing_columns();
    jj_consume_token(rparen);
    references_specification();
}


void SqlParser::references_specification() {
    jj_consume_token(REFERENCES);
    referenced_table_and_columns();
    if (jj_2_1392(3)) {
      jj_consume_token(MATCH);
      match_type();
    } else {
      ;
    }
    if (jj_2_1393(3)) {
      referential_triggered_action();
    } else {
      ;
    }
}


void SqlParser::match_type() {
    if (jj_2_1394(3)) {
      jj_consume_token(FULL);
    } else if (jj_2_1395(3)) {
      jj_consume_token(PARTIAL);
    } else if (jj_2_1396(3)) {
      jj_consume_token(SIMPLE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::referencing_columns() {
    reference_column_list();
}


void SqlParser::referenced_table_and_columns() {
    table_name();
    if (jj_2_1397(3)) {
      jj_consume_token(lparen);
      reference_column_list();
      jj_consume_token(rparen);
    } else {
      ;
    }
}


void SqlParser::reference_column_list() {
    column_name_list();
}


void SqlParser::referential_triggered_action() {
    if (jj_2_1400(3)) {
      update_rule();
      if (jj_2_1398(3)) {
        delete_rule();
      } else {
        ;
      }
    } else if (jj_2_1401(3)) {
      delete_rule();
      if (jj_2_1399(3)) {
        update_rule();
      } else {
        ;
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::update_rule() {
    jj_consume_token(ON);
    jj_consume_token(UPDATE);
    referential_action();
}


void SqlParser::delete_rule() {
    jj_consume_token(ON);
    jj_consume_token(DELETE);
    referential_action();
}


void SqlParser::referential_action() {
    if (jj_2_1402(3)) {
      jj_consume_token(CASCADE);
    } else if (jj_2_1403(3)) {
      jj_consume_token(SET);
      jj_consume_token(NULL_);
    } else if (jj_2_1404(3)) {
      jj_consume_token(SET);
      jj_consume_token(DEFAULT_);
    } else if (jj_2_1405(3)) {
      jj_consume_token(RESTRICT);
    } else if (jj_2_1406(3)) {
      jj_consume_token(NO);
      jj_consume_token(ACTION);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::check_constraint_definition() {
    jj_consume_token(CHECK);
    jj_consume_token(lparen);
    search_condition();
    jj_consume_token(rparen);
}


void SqlParser::alter_table_statement() {
    jj_consume_token(ALTER);
    jj_consume_token(TABLE);
    table_name();
    alter_table_action();
}


void SqlParser::alter_table_action() {
    if (jj_2_1407(3)) {
      add_column_definition();
    } else if (jj_2_1408(3)) {
      alter_column_definition();
    } else if (jj_2_1409(3)) {
      drop_column_definition();
    } else if (jj_2_1410(3)) {
      add_table_constraint_definition();
    } else if (jj_2_1411(3)) {
      alter_table_constraint_definition();
    } else if (jj_2_1412(3)) {
      drop_table_constraint_definition();
    } else if (jj_2_1413(3)) {
      add_system_versioning_clause();
    } else if (jj_2_1414(3)) {
      alter_system_versioning_clause();
    } else if (jj_2_1415(3)) {
      drop_system_versioning_clause();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::add_column_definition() {
    jj_consume_token(ADD);
    if (jj_2_1416(3)) {
      jj_consume_token(COLUMN);
    } else {
      ;
    }
    column_definition();
}


void SqlParser::alter_column_definition() {
    jj_consume_token(ALTER);
    if (jj_2_1417(3)) {
      jj_consume_token(COLUMN);
    } else {
      ;
    }
    identifier();
    alter_column_action();
}


void SqlParser::alter_column_action() {
    if (jj_2_1418(3)) {
      set_column_default_clause();
    } else if (jj_2_1419(3)) {
      drop_column_default_clause();
    } else if (jj_2_1420(3)) {
      set_column_not_null_clause();
    } else if (jj_2_1421(3)) {
      drop_column_not_null_clause();
    } else if (jj_2_1422(3)) {
      add_column_scope_clause();
    } else if (jj_2_1423(3)) {
      drop_column_scope_clause();
    } else if (jj_2_1424(3)) {
      alter_column_data_type_clause();
    } else if (jj_2_1425(3)) {
      alter_identity_column_specification();
    } else if (jj_2_1426(3)) {
      drop_identity_property_clause();
    } else if (jj_2_1427(3)) {
      drop_column_generation_expression_clause();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::set_column_default_clause() {
    jj_consume_token(SET);
    default_clause();
}


void SqlParser::drop_column_default_clause() {
    jj_consume_token(DROP);
    jj_consume_token(DEFAULT_);
}


void SqlParser::set_column_not_null_clause() {
    jj_consume_token(SET);
    jj_consume_token(NOT);
    jj_consume_token(NULL_);
}


void SqlParser::drop_column_not_null_clause() {
    jj_consume_token(DROP);
    jj_consume_token(NOT);
    jj_consume_token(NULL_);
}


void SqlParser::add_column_scope_clause() {
    jj_consume_token(ADD);
    scope_clause();
}


void SqlParser::drop_column_scope_clause() {
    jj_consume_token(DROP);
    jj_consume_token(SCOPE);
    drop_behavior();
}


void SqlParser::alter_column_data_type_clause() {
    jj_consume_token(SET);
    jj_consume_token(DATA);
    jj_consume_token(TYPE);
    data_type();
}


void SqlParser::alter_identity_column_specification() {
    if (jj_2_1430(3)) {
      set_identity_column_generation_clause();
      while (!hasError) {
        if (jj_2_1428(3)) {
          ;
        } else {
          goto end_label_63;
        }
        alter_identity_column_option();
      }
      end_label_63: ;
    } else if (jj_2_1431(3)) {
      while (!hasError) {
        alter_identity_column_option();
        if (jj_2_1429(3)) {
          ;
        } else {
          goto end_label_64;
        }
      }
      end_label_64: ;
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::set_identity_column_generation_clause() {
    jj_consume_token(SET);
    jj_consume_token(GENERATED);
    if (jj_2_1432(3)) {
      jj_consume_token(ALWAYS);
    } else if (jj_2_1433(3)) {
      jj_consume_token(BY);
      jj_consume_token(DEFAULT_);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::alter_identity_column_option() {
    if (jj_2_1434(3)) {
      alter_sequence_generator_restart_option();
    } else if (jj_2_1435(3)) {
      jj_consume_token(SET);
      basic_sequence_generator_option();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::drop_identity_property_clause() {
    jj_consume_token(DROP);
    jj_consume_token(IDENTITY);
}


void SqlParser::drop_column_generation_expression_clause() {
    jj_consume_token(DROP);
    jj_consume_token(EXPRESSION);
}


void SqlParser::drop_column_definition() {
    jj_consume_token(DROP);
    if (jj_2_1436(3)) {
      jj_consume_token(COLUMN);
    } else {
      ;
    }
    identifier();
    drop_behavior();
}


void SqlParser::add_table_constraint_definition() {
    jj_consume_token(ADD);
    table_constraint_definition();
}


void SqlParser::alter_table_constraint_definition() {
    jj_consume_token(ALTER);
    jj_consume_token(CONSTRAINT);
    schema_qualified_name();
    constraint_enforcement();
}


void SqlParser::drop_table_constraint_definition() {
    jj_consume_token(DROP);
    jj_consume_token(CONSTRAINT);
    schema_qualified_name();
    drop_behavior();
}


void SqlParser::add_system_versioning_clause() {
    jj_consume_token(ADD);
    system_versioning_clause();
    add_system_version_column_list();
}


void SqlParser::add_system_version_column_list() {
    jj_consume_token(ADD);
    if (jj_2_1437(3)) {
      jj_consume_token(COLUMN);
    } else {
      ;
    }
    column_definition_1();
    jj_consume_token(ADD);
    if (jj_2_1438(3)) {
      jj_consume_token(COLUMN);
    } else {
      ;
    }
    column_definition_2();
}


void SqlParser::column_definition_1() {
    column_definition();
}


void SqlParser::column_definition_2() {
    column_definition();
}


void SqlParser::alter_system_versioning_clause() {
    jj_consume_token(ALTER);
    jj_consume_token(SYSTEM);
    jj_consume_token(VERSIONING);
    retention_period_specification();
}


void SqlParser::drop_system_versioning_clause() {
    jj_consume_token(DROP);
    jj_consume_token(SYSTEM);
    jj_consume_token(VERSIONING);
    drop_behavior();
}


void SqlParser::drop_table_statement() {
    jj_consume_token(DROP);
    jj_consume_token(TABLE);
    if (jj_2_1439(3)) {
      jj_consume_token(IF);
      jj_consume_token(EXISTS);
    } else {
      ;
    }
    table_name();
    if (jj_2_1440(3)) {
      drop_behavior();
    } else {
      ;
    }
}


void SqlParser::view_definition() {
    jj_consume_token(CREATE);
    if (jj_2_1441(3)) {
      or_replace();
    } else {
      ;
    }
    if (jj_2_1442(3)) {
      jj_consume_token(RECURSIVE);
    } else {
      ;
    }
    jj_consume_token(VIEW);
    table_name();
    view_specification();
    jj_consume_token(AS);
    query_expression();
    if (jj_2_1444(3)) {
      jj_consume_token(WITH);
      if (jj_2_1443(3)) {
        levels_clause();
      } else {
        ;
      }
      jj_consume_token(CHECK);
      jj_consume_token(OPTION);
    } else {
      ;
    }
}


void SqlParser::view_specification() {
    regular_view_specification();
}


void SqlParser::regular_view_specification() {
    if (jj_2_1445(3)) {
      jj_consume_token(lparen);
      view_column_list();
      jj_consume_token(rparen);
    } else {
      ;
    }
}


void SqlParser::referenceable_view_specification() {
    jj_consume_token(OF);
    path_resolved_user_defined_type_name();
    if (jj_2_1446(3)) {
      subview_clause();
    } else {
      ;
    }
    if (jj_2_1447(3)) {
      view_element_list();
    } else {
      ;
    }
}


void SqlParser::subview_clause() {
    jj_consume_token(UNDER);
    table_name();
}


void SqlParser::view_element_list() {
    jj_consume_token(lparen);
    view_element();
    while (!hasError) {
      if (jj_2_1448(3)) {
        ;
      } else {
        goto end_label_65;
      }
      jj_consume_token(570);
      view_element();
    }
    end_label_65: ;
    jj_consume_token(rparen);
}


void SqlParser::view_element() {
    if (jj_2_1449(3)) {
      self_referencing_column_specification();
    } else if (jj_2_1450(3)) {
      view_column_option();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::view_column_option() {
    identifier();
    jj_consume_token(WITH);
    jj_consume_token(OPTIONS);
    scope_clause();
}


void SqlParser::levels_clause() {/*@bgen(jjtree) Unsupported */
  Unsupported *jjtn000 = new Unsupported(JJTUNSUPPORTED);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_1451(3)) {
        jj_consume_token(CASCADED);
      } else if (jj_2_1452(3)) {
        jj_consume_token(LOCAL);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::view_column_list() {
    column_name_list();
}


void SqlParser::drop_view_statement() {
    jj_consume_token(DROP);
    jj_consume_token(VIEW);
    table_name();
    drop_behavior();
}


void SqlParser::domain_definition() {
    jj_consume_token(CREATE);
    jj_consume_token(DOMAIN);
    schema_qualified_name();
    if (jj_2_1453(3)) {
      jj_consume_token(AS);
    } else {
      ;
    }
    predefined_type();
    if (jj_2_1454(3)) {
      default_clause();
    } else {
      ;
    }
    while (!hasError) {
      if (jj_2_1455(3)) {
        ;
      } else {
        goto end_label_66;
      }
      domain_constraint();
    }
    end_label_66: ;
    if (jj_2_1456(3)) {
      collate_clause();
    } else {
      ;
    }
}


void SqlParser::domain_constraint() {
    if (jj_2_1457(3)) {
      constraint_name_definition();
    } else {
      ;
    }
    check_constraint_definition();
    if (jj_2_1458(3)) {
      constraint_characteristics();
    } else {
      ;
    }
}


void SqlParser::alter_domain_statement() {
    jj_consume_token(ALTER);
    jj_consume_token(DOMAIN);
    schema_qualified_name();
    alter_domain_action();
}


void SqlParser::alter_domain_action() {
    if (jj_2_1459(3)) {
      set_domain_default_clause();
    } else if (jj_2_1460(3)) {
      drop_domain_default_clause();
    } else if (jj_2_1461(3)) {
      add_domain_constraint_definition();
    } else if (jj_2_1462(3)) {
      drop_domain_constraint_definition();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::set_domain_default_clause() {
    jj_consume_token(SET);
    default_clause();
}


void SqlParser::drop_domain_default_clause() {
    jj_consume_token(DROP);
    jj_consume_token(DEFAULT_);
}


void SqlParser::add_domain_constraint_definition() {
    jj_consume_token(ADD);
    domain_constraint();
}


void SqlParser::drop_domain_constraint_definition() {
    jj_consume_token(DROP);
    jj_consume_token(CONSTRAINT);
    schema_qualified_name();
}


void SqlParser::drop_domain_statement() {
    jj_consume_token(DROP);
    jj_consume_token(DOMAIN);
    schema_qualified_name();
    drop_behavior();
}


void SqlParser::character_set_definition() {
    jj_consume_token(CREATE);
    jj_consume_token(CHARACTER);
    jj_consume_token(SET);
    character_set_name();
    if (jj_2_1463(3)) {
      jj_consume_token(AS);
    } else {
      ;
    }
    character_set_source();
    if (jj_2_1464(3)) {
      collate_clause();
    } else {
      ;
    }
}


void SqlParser::character_set_source() {
    jj_consume_token(GET);
    character_set_specification();
}


void SqlParser::drop_character_set_statement() {
    jj_consume_token(DROP);
    jj_consume_token(CHARACTER);
    jj_consume_token(SET);
    character_set_name();
}


void SqlParser::collation_definition() {
    jj_consume_token(CREATE);
    jj_consume_token(COLLATION);
    schema_qualified_name();
    jj_consume_token(FOR);
    character_set_specification();
    jj_consume_token(FROM);
    schema_qualified_name();
    if (jj_2_1465(3)) {
      pad_characteristic();
    } else {
      ;
    }
}


void SqlParser::pad_characteristic() {
    if (jj_2_1466(3)) {
      jj_consume_token(NO);
      jj_consume_token(PAD);
    } else if (jj_2_1467(3)) {
      jj_consume_token(PAD);
      jj_consume_token(SPACE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::drop_collation_statement() {
    jj_consume_token(DROP);
    jj_consume_token(COLLATION);
    schema_qualified_name();
    drop_behavior();
}


void SqlParser::transliteration_definition() {
    jj_consume_token(CREATE);
    jj_consume_token(TRANSLATION);
    schema_qualified_name();
    jj_consume_token(FOR);
    source_character_set_specification();
    jj_consume_token(TO);
    target_character_set_specification();
    jj_consume_token(FROM);
    transliteration_source();
}


void SqlParser::source_character_set_specification() {
    character_set_specification();
}


void SqlParser::target_character_set_specification() {
    character_set_specification();
}


void SqlParser::transliteration_source() {
    if (jj_2_1468(3)) {
      schema_qualified_name();
    } else if (jj_2_1469(3)) {
      transliteration_routine();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::transliteration_routine() {
    specific_routine_designator();
}


void SqlParser::drop_transliteration_statement() {
    jj_consume_token(DROP);
    jj_consume_token(TRANSLATION);
    schema_qualified_name();
}


void SqlParser::assertion_definition() {
    jj_consume_token(CREATE);
    jj_consume_token(ASSERTION);
    schema_qualified_name();
    jj_consume_token(CHECK);
    jj_consume_token(lparen);
    search_condition();
    jj_consume_token(rparen);
    if (jj_2_1470(3)) {
      constraint_characteristics();
    } else {
      ;
    }
}


void SqlParser::drop_assertion_statement() {
    jj_consume_token(DROP);
    jj_consume_token(ASSERTION);
    schema_qualified_name();
    if (jj_2_1471(3)) {
      drop_behavior();
    } else {
      ;
    }
}


void SqlParser::trigger_definition() {
    jj_consume_token(CREATE);
    jj_consume_token(TRIGGER);
    schema_qualified_name();
    trigger_action_time();
    trigger_event();
    jj_consume_token(ON);
    table_name();
    if (jj_2_1472(3)) {
      jj_consume_token(REFERENCING);
      transition_table_or_variable_list();
    } else {
      ;
    }
    triggered_action();
}


void SqlParser::trigger_action_time() {
    if (jj_2_1473(3)) {
      jj_consume_token(BEFORE);
    } else if (jj_2_1474(3)) {
      jj_consume_token(AFTER);
    } else if (jj_2_1475(3)) {
      jj_consume_token(INSTEAD);
      jj_consume_token(OF);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::trigger_event() {
    if (jj_2_1477(3)) {
      jj_consume_token(INSERT);
    } else if (jj_2_1478(3)) {
      jj_consume_token(DELETE);
    } else if (jj_2_1479(3)) {
      jj_consume_token(UPDATE);
      if (jj_2_1476(3)) {
        jj_consume_token(OF);
        trigger_column_list();
      } else {
        ;
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::trigger_column_list() {
    column_name_list();
}


void SqlParser::triggered_action() {
    if (jj_2_1482(3)) {
      jj_consume_token(FOR);
      jj_consume_token(EACH);
      if (jj_2_1480(3)) {
        jj_consume_token(ROW);
      } else if (jj_2_1481(3)) {
        jj_consume_token(STATEMENT);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } else {
      ;
    }
    if (jj_2_1483(3)) {
      triggered_when_clause();
    } else {
      ;
    }
    triggered_SQL_statement();
}


void SqlParser::triggered_when_clause() {
    jj_consume_token(WHEN);
    jj_consume_token(lparen);
    search_condition();
    jj_consume_token(rparen);
}


void SqlParser::triggered_SQL_statement() {
    if (jj_2_1485(3)) {
      SQL_procedure_statement();
    } else if (jj_2_1486(3)) {
      jj_consume_token(BEGIN);
      jj_consume_token(ATOMIC);
      while (!hasError) {
        SQL_procedure_statement();
        jj_consume_token(semicolon);
        if (jj_2_1484(3)) {
          ;
        } else {
          goto end_label_67;
        }
      }
      end_label_67: ;
      jj_consume_token(END);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::transition_table_or_variable_list() {
    while (!hasError) {
      transition_table_or_variable();
      if (jj_2_1487(3)) {
        ;
      } else {
        goto end_label_68;
      }
    }
    end_label_68: ;
}


void SqlParser::transition_table_or_variable() {
    if (jj_2_1494(3)) {
      jj_consume_token(OLD);
      if (jj_2_1488(3)) {
        jj_consume_token(ROW);
      } else {
        ;
      }
      if (jj_2_1489(3)) {
        jj_consume_token(AS);
      } else {
        ;
      }
      identifier();
    } else if (jj_2_1495(3)) {
      jj_consume_token(NEW);
      if (jj_2_1490(3)) {
        jj_consume_token(ROW);
      } else {
        ;
      }
      if (jj_2_1491(3)) {
        jj_consume_token(AS);
      } else {
        ;
      }
      identifier();
    } else if (jj_2_1496(3)) {
      jj_consume_token(OLD);
      jj_consume_token(TABLE);
      if (jj_2_1492(3)) {
        jj_consume_token(AS);
      } else {
        ;
      }
      identifier();
    } else if (jj_2_1497(3)) {
      jj_consume_token(NEW);
      jj_consume_token(TABLE);
      if (jj_2_1493(3)) {
        jj_consume_token(AS);
      } else {
        ;
      }
      identifier();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::drop_trigger_statement() {
    jj_consume_token(DROP);
    jj_consume_token(TRIGGER);
    schema_qualified_name();
}


void SqlParser::user_defined_type_definition() {
    jj_consume_token(CREATE);
    jj_consume_token(TYPE);
    user_defined_type_body();
}


void SqlParser::user_defined_type_body() {
    schema_resolved_user_defined_type_name();
    if (jj_2_1498(3)) {
      subtype_clause();
    } else {
      ;
    }
    if (jj_2_1499(3)) {
      jj_consume_token(AS);
      representation();
    } else {
      ;
    }
    if (jj_2_1500(3)) {
      user_defined_type_option_list();
    } else {
      ;
    }
    if (jj_2_1501(3)) {
      method_specification_list();
    } else {
      ;
    }
}


void SqlParser::user_defined_type_option_list() {
    user_defined_type_option();
    while (!hasError) {
      if (jj_2_1502(3)) {
        ;
      } else {
        goto end_label_69;
      }
      user_defined_type_option();
    }
    end_label_69: ;
}


void SqlParser::user_defined_type_option() {
    if (jj_2_1503(3)) {
      instantiable_clause();
    } else if (jj_2_1504(3)) {
      finality();
    } else if (jj_2_1505(3)) {
      reference_type_specification();
    } else if (jj_2_1506(3)) {
      cast_to_ref();
    } else if (jj_2_1507(3)) {
      cast_to_type();
    } else if (jj_2_1508(3)) {
      cast_to_distinct();
    } else if (jj_2_1509(3)) {
      cast_to_source();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::subtype_clause() {
    jj_consume_token(UNDER);
    supertype_name();
}


void SqlParser::supertype_name() {
    path_resolved_user_defined_type_name();
}


void SqlParser::representation() {
    if (jj_2_1510(3)) {
      predefined_type();
    } else if (jj_2_1511(3)) {
      data_type();
    } else if (jj_2_1512(3)) {
      member_list();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::member_list() {
    jj_consume_token(lparen);
    member();
    while (!hasError) {
      if (jj_2_1513(3)) {
        ;
      } else {
        goto end_label_70;
      }
      jj_consume_token(570);
      member();
    }
    end_label_70: ;
    jj_consume_token(rparen);
}


void SqlParser::member() {
    attribute_definition();
}


void SqlParser::instantiable_clause() {
    if (jj_2_1514(3)) {
      jj_consume_token(INSTANTIABLE);
    } else if (jj_2_1515(3)) {
      jj_consume_token(NOT);
      jj_consume_token(INSTANTIABLE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::finality() {
    if (jj_2_1516(3)) {
      jj_consume_token(FINAL);
    } else if (jj_2_1517(3)) {
      jj_consume_token(NOT);
      jj_consume_token(FINAL);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::reference_type_specification() {
    if (jj_2_1518(3)) {
      user_defined_representation();
    } else if (jj_2_1519(3)) {
      derived_representation();
    } else if (jj_2_1520(3)) {
      system_generated_representation();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::user_defined_representation() {
    jj_consume_token(REF);
    jj_consume_token(USING);
    predefined_type();
}


void SqlParser::derived_representation() {
    jj_consume_token(REF);
    jj_consume_token(FROM);
    list_of_attributes();
}


void SqlParser::system_generated_representation() {
    jj_consume_token(REF);
    jj_consume_token(IS);
    jj_consume_token(SYSTEM);
    jj_consume_token(GENERATED);
}


void SqlParser::cast_to_ref() {
    jj_consume_token(CAST);
    jj_consume_token(lparen);
    jj_consume_token(SOURCE);
    jj_consume_token(AS);
    jj_consume_token(REF);
    jj_consume_token(rparen);
    jj_consume_token(WITH);
    identifier();
}


void SqlParser::cast_to_type() {
    jj_consume_token(CAST);
    jj_consume_token(lparen);
    jj_consume_token(REF);
    jj_consume_token(AS);
    jj_consume_token(SOURCE);
    jj_consume_token(rparen);
    jj_consume_token(WITH);
    identifier();
}


void SqlParser::list_of_attributes() {
    jj_consume_token(lparen);
    identifier();
    while (!hasError) {
      if (jj_2_1521(3)) {
        ;
      } else {
        goto end_label_71;
      }
      jj_consume_token(570);
      identifier();
    }
    end_label_71: ;
    jj_consume_token(rparen);
}


void SqlParser::cast_to_distinct() {
    jj_consume_token(CAST);
    jj_consume_token(lparen);
    jj_consume_token(SOURCE);
    jj_consume_token(AS);
    jj_consume_token(DISTINCT);
    jj_consume_token(rparen);
    jj_consume_token(WITH);
    identifier();
}


void SqlParser::cast_to_source() {
    jj_consume_token(CAST);
    jj_consume_token(lparen);
    jj_consume_token(DISTINCT);
    jj_consume_token(AS);
    jj_consume_token(SOURCE);
    jj_consume_token(rparen);
    jj_consume_token(WITH);
    identifier();
}


void SqlParser::method_specification_list() {
    method_specification();
    while (!hasError) {
      if (jj_2_1522(3)) {
        ;
      } else {
        goto end_label_72;
      }
      jj_consume_token(570);
      method_specification();
    }
    end_label_72: ;
}


void SqlParser::method_specification() {
    if (jj_2_1523(3)) {
      original_method_specification();
    } else if (jj_2_1524(3)) {
      overriding_method_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::original_method_specification() {
    partial_method_specification();
    if (jj_2_1525(3)) {
      jj_consume_token(SELF);
      jj_consume_token(AS);
      jj_consume_token(RESULT);
    } else {
      ;
    }
    if (jj_2_1526(3)) {
      jj_consume_token(SELF);
      jj_consume_token(AS);
      jj_consume_token(LOCATOR);
    } else {
      ;
    }
    if (jj_2_1527(3)) {
      method_characteristics();
    } else {
      ;
    }
}


void SqlParser::overriding_method_specification() {
    jj_consume_token(OVERRIDING);
    partial_method_specification();
}


void SqlParser::partial_method_specification() {
    if (jj_2_1531(3)) {
      if (jj_2_1528(3)) {
        jj_consume_token(INSTANCE);
      } else if (jj_2_1529(3)) {
        jj_consume_token(STATIC);
      } else if (jj_2_1530(3)) {
        jj_consume_token(CONSTRUCTOR);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } else {
      ;
    }
    jj_consume_token(METHOD);
    identifier();
    SQL_parameter_declaration_list();
    returns_clause();
    if (jj_2_1532(3)) {
      jj_consume_token(SPECIFIC);
      specific_identifier();
    } else {
      ;
    }
}


void SqlParser::specific_identifier() {
    if (jj_2_1533(3)) {
      schema_name();
      jj_consume_token(569);
    } else {
      ;
    }
    identifier();
}


void SqlParser::method_characteristics() {
    while (!hasError) {
      method_characteristic();
      if (jj_2_1534(3)) {
        ;
      } else {
        goto end_label_73;
      }
    }
    end_label_73: ;
}


void SqlParser::method_characteristic() {
    if (jj_2_1535(3)) {
      language_clause();
    } else if (jj_2_1536(3)) {
      parameter_style_clause();
    } else if (jj_2_1537(3)) {
      deterministic_characteristic();
    } else if (jj_2_1538(3)) {
      SQL_data_access_indication();
    } else if (jj_2_1539(3)) {
      null_call_clause();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::attribute_definition() {
    identifier();
    data_type();
    if (jj_2_1540(3)) {
      attribute_default();
    } else {
      ;
    }
    if (jj_2_1541(3)) {
      collate_clause();
    } else {
      ;
    }
}


void SqlParser::attribute_default() {
    default_clause();
}


void SqlParser::alter_type_statement() {
    jj_consume_token(ALTER);
    jj_consume_token(TYPE);
    schema_resolved_user_defined_type_name();
    alter_type_action();
}


void SqlParser::alter_type_action() {
    if (jj_2_1542(3)) {
      add_attribute_definition();
    } else if (jj_2_1543(3)) {
      drop_attribute_definition();
    } else if (jj_2_1544(3)) {
      add_original_method_specification();
    } else if (jj_2_1545(3)) {
      add_overriding_method_specification();
    } else if (jj_2_1546(3)) {
      drop_method_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::add_attribute_definition() {
    jj_consume_token(ADD);
    jj_consume_token(ATTRIBUTE);
    attribute_definition();
}


void SqlParser::drop_attribute_definition() {
    jj_consume_token(DROP);
    jj_consume_token(ATTRIBUTE);
    identifier();
    jj_consume_token(RESTRICT);
}


void SqlParser::add_original_method_specification() {
    jj_consume_token(ADD);
    original_method_specification();
}


void SqlParser::add_overriding_method_specification() {
    jj_consume_token(ADD);
    overriding_method_specification();
}


void SqlParser::drop_method_specification() {
    jj_consume_token(DROP);
    specific_method_specification_designator();
    jj_consume_token(RESTRICT);
}


void SqlParser::specific_method_specification_designator() {
    if (jj_2_1550(3)) {
      if (jj_2_1547(3)) {
        jj_consume_token(INSTANCE);
      } else if (jj_2_1548(3)) {
        jj_consume_token(STATIC);
      } else if (jj_2_1549(3)) {
        jj_consume_token(CONSTRUCTOR);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } else {
      ;
    }
    jj_consume_token(METHOD);
    identifier();
    data_type_list();
}


void SqlParser::drop_data_type_statement() {
    jj_consume_token(DROP);
    jj_consume_token(TYPE);
    schema_resolved_user_defined_type_name();
    drop_behavior();
}


void SqlParser::SQL_invoked_routine() {
    schema_routine();
}


void SqlParser::schema_routine() {
    if (jj_2_1551(3)) {
      schema_procedure();
    } else if (jj_2_1552(3)) {
      schema_function();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::schema_procedure() {
    jj_consume_token(CREATE);
    SQL_invoked_procedure();
}


void SqlParser::schema_function() {
    jj_consume_token(CREATE);
    if (jj_2_1553(3)) {
      or_replace();
    } else {
      ;
    }
    SQL_invoked_function();
}


void SqlParser::SQL_invoked_procedure() {
    jj_consume_token(PROCEDURE);
    schema_qualified_name();
    SQL_parameter_declaration_list();
    routine_characteristics();
    routine_body();
}


void SqlParser::SQL_invoked_function() {
    if (jj_2_1554(3)) {
      function_specification();
    } else if (jj_2_1555(3)) {
      method_specification_designator();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
    routine_body();
}


void SqlParser::SQL_parameter_declaration_list() {
    jj_consume_token(lparen);
    if (jj_2_1557(3)) {
      SQL_parameter_declaration();
      while (!hasError) {
        if (jj_2_1556(3)) {
          ;
        } else {
          goto end_label_74;
        }
        jj_consume_token(570);
        SQL_parameter_declaration();
      }
      end_label_74: ;
    } else {
      ;
    }
    jj_consume_token(rparen);
}


void SqlParser::SQL_parameter_declaration() {
    if (jj_2_1558(3)) {
      parameter_mode();
    } else {
      ;
    }
    if (jj_2_1559(3)) {
      identifier();
    } else {
      ;
    }
    parameter_type();
    if (jj_2_1560(3)) {
      jj_consume_token(RESULT);
    } else {
      ;
    }
    if (jj_2_1561(3)) {
      jj_consume_token(DEFAULT_);
      parameter_default();
    } else {
      ;
    }
}


void SqlParser::parameter_default() {
    if (jj_2_1562(3)) {
      value_expression();
    } else if (jj_2_1563(3)) {
      contextually_typed_value_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::parameter_mode() {
    if (jj_2_1564(3)) {
      jj_consume_token(IN);
    } else if (jj_2_1565(3)) {
      jj_consume_token(OUT);
    } else if (jj_2_1566(3)) {
      jj_consume_token(INOUT);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::parameter_type() {
    data_type();
    if (jj_2_1567(3)) {
      locator_indication();
    } else {
      ;
    }
}


void SqlParser::locator_indication() {
    jj_consume_token(AS);
    jj_consume_token(LOCATOR);
}


void SqlParser::function_specification() {
    jj_consume_token(FUNCTION);
    schema_qualified_name();
    SQL_parameter_declaration_list();
    returns_clause();
    if (jj_2_1568(3)) {
      routine_description();
    } else {
      ;
    }
    routine_characteristics();
    if (jj_2_1569(3)) {
      dispatch_clause();
    } else {
      ;
    }
}


void SqlParser::method_specification_designator() {
    if (jj_2_1575(3)) {
      jj_consume_token(SPECIFIC);
      jj_consume_token(METHOD);
      specific_identifier();
    } else if (jj_2_1576(3)) {
      if (jj_2_1573(3)) {
        if (jj_2_1570(3)) {
          jj_consume_token(INSTANCE);
        } else if (jj_2_1571(3)) {
          jj_consume_token(STATIC);
        } else if (jj_2_1572(3)) {
          jj_consume_token(CONSTRUCTOR);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } else {
        ;
      }
      jj_consume_token(METHOD);
      identifier();
      SQL_parameter_declaration_list();
      if (jj_2_1574(3)) {
        returns_clause();
      } else {
        ;
      }
      jj_consume_token(FOR);
      schema_resolved_user_defined_type_name();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::routine_characteristics() {
    while (!hasError) {
      if (jj_2_1577(3)) {
        ;
      } else {
        goto end_label_75;
      }
      routine_characteristic();
    }
    end_label_75: ;
}


void SqlParser::routine_characteristic() {
    if (jj_2_1578(3)) {
      language_clause();
    } else if (jj_2_1579(3)) {
      parameter_style_clause();
    } else if (jj_2_1580(3)) {
      jj_consume_token(SPECIFIC);
      schema_qualified_name();
    } else if (jj_2_1581(3)) {
      deterministic_characteristic();
    } else if (jj_2_1582(3)) {
      SQL_data_access_indication();
    } else if (jj_2_1583(3)) {
      null_call_clause();
    } else if (jj_2_1584(3)) {
      returned_result_sets_characteristic();
    } else if (jj_2_1585(3)) {
      savepoint_level_indication();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::savepoint_level_indication() {
    if (jj_2_1586(3)) {
      jj_consume_token(NEW);
      jj_consume_token(SAVEPOINT);
      jj_consume_token(LEVEL);
    } else if (jj_2_1587(3)) {
      jj_consume_token(OLD);
      jj_consume_token(SAVEPOINT);
      jj_consume_token(LEVEL);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::returned_result_sets_characteristic() {
    jj_consume_token(DYNAMIC);
    jj_consume_token(RESULT);
    jj_consume_token(SETS);
    maximum_returned_result_sets();
}


void SqlParser::parameter_style_clause() {
    jj_consume_token(PARAMETER);
    jj_consume_token(STYLE);
    parameter_style();
}


void SqlParser::dispatch_clause() {
    jj_consume_token(STATIC);
    jj_consume_token(DISPATCH);
}


void SqlParser::returns_clause() {
    jj_consume_token(RETURNS);
    returns_type();
}


void SqlParser::returns_type() {
    if (jj_2_1589(3)) {
      returns_data_type();
      if (jj_2_1588(3)) {
        result_cast();
      } else {
        ;
      }
    } else if (jj_2_1590(3)) {
      returns_table_type();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::returns_table_type() {
    jj_consume_token(TABLE);
    table_function_column_list();
}


void SqlParser::table_function_column_list() {
    jj_consume_token(lparen);
    table_function_column_list_element();
    while (!hasError) {
      if (jj_2_1591(3)) {
        ;
      } else {
        goto end_label_76;
      }
      jj_consume_token(570);
      table_function_column_list_element();
    }
    end_label_76: ;
    jj_consume_token(rparen);
}


void SqlParser::table_function_column_list_element() {
    identifier();
    data_type();
}


void SqlParser::result_cast() {
    jj_consume_token(CAST);
    jj_consume_token(FROM);
    result_cast_from_type();
}


void SqlParser::result_cast_from_type() {
    data_type();
    if (jj_2_1592(3)) {
      locator_indication();
    } else {
      ;
    }
}


void SqlParser::returns_data_type() {
    data_type();
    if (jj_2_1593(3)) {
      locator_indication();
    } else {
      ;
    }
}


void SqlParser::routine_body() {
    if (jj_2_1594(3)) {
      SQL_routine_spec();
    } else if (jj_2_1595(3)) {
      external_body_reference();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::SQL_routine_spec() {
    if (jj_2_1596(3)) {
      rights_clause();
    } else {
      ;
    }
    SQL_routine_body();
}


void SqlParser::rights_clause() {
    if (jj_2_1597(3)) {
      jj_consume_token(SQL);
      jj_consume_token(SECURITY);
      jj_consume_token(INVOKER);
    } else if (jj_2_1598(3)) {
      jj_consume_token(SQL);
      jj_consume_token(SECURITY);
      jj_consume_token(DEFINER);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::SQL_routine_body() {
    SQL_procedure_statement();
}


void SqlParser::external_body_reference() {
    jj_consume_token(EXTERNAL);
    if (jj_2_1599(3)) {
      jj_consume_token(NAME);
      external_routine_name();
    } else {
      ;
    }
    if (jj_2_1600(3)) {
      parameter_style_clause();
    } else {
      ;
    }
    if (jj_2_1601(3)) {
      transform_group_specification();
    } else {
      ;
    }
    if (jj_2_1602(3)) {
      external_security_clause();
    } else {
      ;
    }
}


void SqlParser::external_security_clause() {
    if (jj_2_1603(3)) {
      jj_consume_token(EXTERNAL);
      jj_consume_token(SECURITY);
      jj_consume_token(DEFINER);
    } else if (jj_2_1604(3)) {
      jj_consume_token(EXTERNAL);
      jj_consume_token(SECURITY);
      jj_consume_token(INVOKER);
    } else if (jj_2_1605(3)) {
      jj_consume_token(EXTERNAL);
      jj_consume_token(SECURITY);
      jj_consume_token(IMPLEMENTATION);
      jj_consume_token(DEFINED);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::parameter_style() {
    if (jj_2_1606(3)) {
      jj_consume_token(SQL);
    } else if (jj_2_1607(3)) {
      jj_consume_token(GENERAL);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::deterministic_characteristic() {
    if (jj_2_1608(3)) {
      jj_consume_token(DETERMINISTIC);
    } else if (jj_2_1609(3)) {
      jj_consume_token(NOT);
      jj_consume_token(DETERMINISTIC);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::SQL_data_access_indication() {
    if (jj_2_1610(3)) {
      jj_consume_token(NO);
      jj_consume_token(SQL);
    } else if (jj_2_1611(3)) {
      jj_consume_token(CONTAINS);
      jj_consume_token(SQL);
    } else if (jj_2_1612(3)) {
      jj_consume_token(READS);
      jj_consume_token(SQL);
      jj_consume_token(DATA);
    } else if (jj_2_1613(3)) {
      jj_consume_token(MODIFIES);
      jj_consume_token(SQL);
      jj_consume_token(DATA);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::null_call_clause() {
    if (jj_2_1614(3)) {
      jj_consume_token(RETURNS);
      jj_consume_token(NULL_);
      jj_consume_token(ON);
      jj_consume_token(NULL_);
      jj_consume_token(INPUT);
    } else if (jj_2_1615(3)) {
      jj_consume_token(CALLED);
      jj_consume_token(ON);
      jj_consume_token(NULL_);
      jj_consume_token(INPUT);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::maximum_returned_result_sets() {
    jj_consume_token(unsigned_integer);
}


void SqlParser::transform_group_specification() {
    jj_consume_token(TRANSFORM);
    jj_consume_token(GROUP);
    if (jj_2_1616(3)) {
      single_group_specification();
    } else if (jj_2_1617(3)) {
      multiple_group_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::single_group_specification() {
    identifier();
}


void SqlParser::multiple_group_specification() {
    group_specification();
    while (!hasError) {
      if (jj_2_1618(3)) {
        ;
      } else {
        goto end_label_77;
      }
      jj_consume_token(570);
      group_specification();
    }
    end_label_77: ;
}


void SqlParser::group_specification() {
    identifier();
    jj_consume_token(FOR);
    jj_consume_token(TYPE);
    path_resolved_user_defined_type_name();
}


void SqlParser::alter_routine_statement() {
    jj_consume_token(ALTER);
    specific_routine_designator();
    alter_routine_characteristics();
    alter_routine_behavior();
}


void SqlParser::alter_routine_characteristics() {
    while (!hasError) {
      alter_routine_characteristic();
      if (jj_2_1619(3)) {
        ;
      } else {
        goto end_label_78;
      }
    }
    end_label_78: ;
}


void SqlParser::alter_routine_characteristic() {
    if (jj_2_1620(3)) {
      language_clause();
    } else if (jj_2_1621(3)) {
      parameter_style_clause();
    } else if (jj_2_1622(3)) {
      SQL_data_access_indication();
    } else if (jj_2_1623(3)) {
      null_call_clause();
    } else if (jj_2_1624(3)) {
      returned_result_sets_characteristic();
    } else if (jj_2_1625(3)) {
      jj_consume_token(NAME);
      external_routine_name();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::alter_routine_behavior() {
    jj_consume_token(RESTRICT);
}


void SqlParser::drop_routine_statement() {
    jj_consume_token(DROP);
    specific_routine_designator();
    drop_behavior();
}


void SqlParser::user_defined_cast_definition() {
    jj_consume_token(CREATE);
    jj_consume_token(CAST);
    jj_consume_token(lparen);
    source_data_type();
    jj_consume_token(AS);
    target_data_type();
    jj_consume_token(rparen);
    jj_consume_token(WITH);
    cast_function();
    if (jj_2_1626(3)) {
      jj_consume_token(AS);
      jj_consume_token(ASSIGNMENT);
    } else {
      ;
    }
}


void SqlParser::cast_function() {
    specific_routine_designator();
}


void SqlParser::source_data_type() {
    data_type();
}


void SqlParser::target_data_type() {
    data_type();
}


void SqlParser::drop_user_defined_cast_statement() {
    jj_consume_token(DROP);
    jj_consume_token(CAST);
    jj_consume_token(lparen);
    source_data_type();
    jj_consume_token(AS);
    target_data_type();
    jj_consume_token(rparen);
    drop_behavior();
}


void SqlParser::user_defined_ordering_definition() {
    jj_consume_token(CREATE);
    jj_consume_token(ORDERING);
    jj_consume_token(FOR);
    schema_resolved_user_defined_type_name();
    ordering_form();
}


void SqlParser::ordering_form() {
    if (jj_2_1627(3)) {
      equals_ordering_form();
    } else if (jj_2_1628(3)) {
      full_ordering_form();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::equals_ordering_form() {
    jj_consume_token(EQUALS);
    jj_consume_token(ONLY);
    jj_consume_token(BY);
    ordering_category();
}


void SqlParser::full_ordering_form() {
    jj_consume_token(ORDER);
    jj_consume_token(FULL);
    jj_consume_token(BY);
    ordering_category();
}


void SqlParser::ordering_category() {
    if (jj_2_1629(3)) {
      relative_category();
    } else if (jj_2_1630(3)) {
      map_category();
    } else if (jj_2_1631(3)) {
      state_category();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::relative_category() {
    jj_consume_token(RELATIVE);
    jj_consume_token(WITH);
    relative_function_specification();
}


void SqlParser::map_category() {
    jj_consume_token(MAP);
    jj_consume_token(WITH);
    map_function_specification();
}


void SqlParser::state_category() {
    jj_consume_token(STATE);
    if (jj_2_1632(3)) {
      schema_qualified_name();
    } else {
      ;
    }
}


void SqlParser::relative_function_specification() {
    specific_routine_designator();
}


void SqlParser::map_function_specification() {
    specific_routine_designator();
}


void SqlParser::drop_user_defined_ordering_statement() {
    jj_consume_token(DROP);
    jj_consume_token(ORDERING);
    jj_consume_token(FOR);
    schema_resolved_user_defined_type_name();
    drop_behavior();
}


void SqlParser::transform_definition() {
    jj_consume_token(CREATE);
    if (jj_2_1633(3)) {
      jj_consume_token(TRANSFORM);
    } else if (jj_2_1634(3)) {
      jj_consume_token(TRANSFORMS);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
    jj_consume_token(FOR);
    schema_resolved_user_defined_type_name();
    while (!hasError) {
      transform_group();
      if (jj_2_1635(3)) {
        ;
      } else {
        goto end_label_79;
      }
    }
    end_label_79: ;
}


void SqlParser::transform_group() {
    identifier();
    jj_consume_token(lparen);
    transform_element_list();
    jj_consume_token(rparen);
}


void SqlParser::transform_element_list() {
    transform_element();
    if (jj_2_1636(3)) {
      jj_consume_token(570);
      transform_element();
    } else {
      ;
    }
}


void SqlParser::transform_element() {
    if (jj_2_1637(3)) {
      to_sql();
    } else if (jj_2_1638(3)) {
      from_sql();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::to_sql() {
    jj_consume_token(TO);
    jj_consume_token(SQL);
    jj_consume_token(WITH);
    to_sql_function();
}


void SqlParser::from_sql() {
    jj_consume_token(FROM);
    jj_consume_token(SQL);
    jj_consume_token(WITH);
    from_sql_function();
}


void SqlParser::to_sql_function() {
    specific_routine_designator();
}


void SqlParser::from_sql_function() {
    specific_routine_designator();
}


void SqlParser::alter_transform_statement() {
    jj_consume_token(ALTER);
    if (jj_2_1639(3)) {
      jj_consume_token(TRANSFORM);
    } else if (jj_2_1640(3)) {
      jj_consume_token(TRANSFORMS);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
    jj_consume_token(FOR);
    schema_resolved_user_defined_type_name();
    while (!hasError) {
      alter_group();
      if (jj_2_1641(3)) {
        ;
      } else {
        goto end_label_80;
      }
    }
    end_label_80: ;
}


void SqlParser::alter_group() {
    identifier();
    jj_consume_token(lparen);
    alter_transform_action_list();
    jj_consume_token(rparen);
}


void SqlParser::alter_transform_action_list() {
    alter_transform_action();
    while (!hasError) {
      if (jj_2_1642(3)) {
        ;
      } else {
        goto end_label_81;
      }
      jj_consume_token(570);
      alter_transform_action();
    }
    end_label_81: ;
}


void SqlParser::alter_transform_action() {
    if (jj_2_1643(3)) {
      add_transform_element_list();
    } else if (jj_2_1644(3)) {
      drop_transform_element_list();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::add_transform_element_list() {
    jj_consume_token(ADD);
    jj_consume_token(lparen);
    transform_element_list();
    jj_consume_token(rparen);
}


void SqlParser::drop_transform_element_list() {
    jj_consume_token(DROP);
    jj_consume_token(lparen);
    transform_kind();
    if (jj_2_1645(3)) {
      jj_consume_token(570);
      transform_kind();
    } else {
      ;
    }
    drop_behavior();
    jj_consume_token(rparen);
}


void SqlParser::transform_kind() {
    if (jj_2_1646(3)) {
      jj_consume_token(TO);
      jj_consume_token(SQL);
    } else if (jj_2_1647(3)) {
      jj_consume_token(FROM);
      jj_consume_token(SQL);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::drop_transform_statement() {
    jj_consume_token(DROP);
    if (jj_2_1648(3)) {
      jj_consume_token(TRANSFORM);
    } else if (jj_2_1649(3)) {
      jj_consume_token(TRANSFORMS);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
    transforms_to_be_dropped();
    jj_consume_token(FOR);
    schema_resolved_user_defined_type_name();
    drop_behavior();
}


void SqlParser::transforms_to_be_dropped() {
    if (jj_2_1650(3)) {
      jj_consume_token(ALL);
    } else if (jj_2_1651(3)) {
      transform_group_element();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::transform_group_element() {
    identifier();
}


void SqlParser::sequence_generator_definition() {
    jj_consume_token(CREATE);
    jj_consume_token(SEQUENCE);
    schema_qualified_name();
    if (jj_2_1652(3)) {
      sequence_generator_options();
    } else {
      ;
    }
}


void SqlParser::sequence_generator_options() {
    while (!hasError) {
      sequence_generator_option();
      if (jj_2_1653(3)) {
        ;
      } else {
        goto end_label_82;
      }
    }
    end_label_82: ;
}


void SqlParser::sequence_generator_option() {
    if (jj_2_1654(3)) {
      sequence_generator_data_type_option();
    } else if (jj_2_1655(3)) {
      common_sequence_generator_options();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::common_sequence_generator_options() {
    while (!hasError) {
      common_sequence_generator_option();
      if (jj_2_1656(3)) {
        ;
      } else {
        goto end_label_83;
      }
    }
    end_label_83: ;
}


void SqlParser::common_sequence_generator_option() {
    if (jj_2_1657(3)) {
      sequence_generator_start_with_option();
    } else if (jj_2_1658(3)) {
      basic_sequence_generator_option();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::basic_sequence_generator_option() {
    if (jj_2_1659(3)) {
      sequence_generator_increment_by_option();
    } else if (jj_2_1660(3)) {
      sequence_generator_maxvalue_option();
    } else if (jj_2_1661(3)) {
      sequence_generator_minvalue_option();
    } else if (jj_2_1662(3)) {
      sequence_generator_cycle_option();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::sequence_generator_data_type_option() {
    jj_consume_token(AS);
    data_type();
}


void SqlParser::sequence_generator_start_with_option() {
    jj_consume_token(START);
    jj_consume_token(WITH);
    sequence_generator_start_value();
}


void SqlParser::sequence_generator_start_value() {
    signed_numeric_literal();
}


void SqlParser::sequence_generator_increment_by_option() {
    jj_consume_token(INCREMENT);
    jj_consume_token(BY);
    sequence_generator_increment();
}


void SqlParser::sequence_generator_increment() {
    signed_numeric_literal();
}


void SqlParser::sequence_generator_maxvalue_option() {
    if (jj_2_1663(3)) {
      jj_consume_token(MAXVALUE);
      sequence_generator_max_value();
    } else if (jj_2_1664(3)) {
      jj_consume_token(NO);
      jj_consume_token(MAXVALUE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::sequence_generator_max_value() {
    signed_numeric_literal();
}


void SqlParser::sequence_generator_minvalue_option() {
    if (jj_2_1665(3)) {
      jj_consume_token(MINVALUE);
      sequence_generator_min_value();
    } else if (jj_2_1666(3)) {
      jj_consume_token(NO);
      jj_consume_token(MINVALUE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::sequence_generator_min_value() {
    signed_numeric_literal();
}


void SqlParser::sequence_generator_cycle_option() {
    if (jj_2_1667(3)) {
      jj_consume_token(CYCLE);
    } else if (jj_2_1668(3)) {
      jj_consume_token(NO);
      jj_consume_token(CYCLE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::alter_sequence_generator_statement() {
    jj_consume_token(ALTER);
    jj_consume_token(SEQUENCE);
    schema_qualified_name();
    alter_sequence_generator_options();
}


void SqlParser::alter_sequence_generator_options() {
    while (!hasError) {
      alter_sequence_generator_option();
      if (jj_2_1669(3)) {
        ;
      } else {
        goto end_label_84;
      }
    }
    end_label_84: ;
}


void SqlParser::alter_sequence_generator_option() {
    if (jj_2_1670(3)) {
      alter_sequence_generator_restart_option();
    } else if (jj_2_1671(3)) {
      basic_sequence_generator_option();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::alter_sequence_generator_restart_option() {
    jj_consume_token(RESTART);
    if (jj_2_1672(3)) {
      jj_consume_token(WITH);
      sequence_generator_restart_value();
    } else {
      ;
    }
}


void SqlParser::sequence_generator_restart_value() {
    signed_numeric_literal();
}


void SqlParser::drop_sequence_generator_statement() {
    jj_consume_token(DROP);
    jj_consume_token(SEQUENCE);
    schema_qualified_name();
    drop_behavior();
}


void SqlParser::grant_statement() {
    if (jj_2_1673(3)) {
      grant_privilege_statement();
    } else if (jj_2_1674(3)) {
      grant_role_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::grant_privilege_statement() {
    jj_consume_token(GRANT);
    privileges();
    jj_consume_token(TO);
    grantee();
    while (!hasError) {
      if (jj_2_1675(3)) {
        ;
      } else {
        goto end_label_85;
      }
      jj_consume_token(570);
      grantee();
    }
    end_label_85: ;
    if (jj_2_1676(3)) {
      jj_consume_token(WITH);
      jj_consume_token(HIERARCHY);
      jj_consume_token(OPTION);
    } else {
      ;
    }
    if (jj_2_1677(3)) {
      jj_consume_token(WITH);
      jj_consume_token(GRANT);
      jj_consume_token(OPTION);
    } else {
      ;
    }
    if (jj_2_1678(3)) {
      jj_consume_token(GRANTED);
      jj_consume_token(BY);
      grantor();
    } else {
      ;
    }
}


void SqlParser::privileges() {
    object_privileges();
    jj_consume_token(ON);
    object_name();
}


void SqlParser::object_name() {
    if (jj_2_1680(3)) {
      if (jj_2_1679(3)) {
        jj_consume_token(TABLE);
      } else {
        ;
      }
      table_name();
    } else if (jj_2_1681(3)) {
      jj_consume_token(DOMAIN);
      schema_qualified_name();
    } else if (jj_2_1682(3)) {
      jj_consume_token(COLLATION);
      schema_qualified_name();
    } else if (jj_2_1683(3)) {
      jj_consume_token(CHARACTER);
      jj_consume_token(SET);
      character_set_name();
    } else if (jj_2_1684(3)) {
      jj_consume_token(TRANSLATION);
      schema_qualified_name();
    } else if (jj_2_1685(3)) {
      jj_consume_token(TYPE);
      schema_resolved_user_defined_type_name();
    } else if (jj_2_1686(3)) {
      jj_consume_token(SEQUENCE);
      schema_qualified_name();
    } else if (jj_2_1687(3)) {
      specific_routine_designator();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::object_privileges() {
    if (jj_2_1689(3)) {
      jj_consume_token(ALL);
      jj_consume_token(PRIVILEGES);
    } else if (jj_2_1690(3)) {
      action();
      while (!hasError) {
        if (jj_2_1688(3)) {
          ;
        } else {
          goto end_label_86;
        }
        jj_consume_token(570);
        action();
      }
      end_label_86: ;
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::action() {
    if (jj_2_1694(3)) {
      jj_consume_token(SELECT);
    } else if (jj_2_1695(3)) {
      jj_consume_token(SELECT);
      jj_consume_token(lparen);
      privilege_column_list();
      jj_consume_token(rparen);
    } else if (jj_2_1696(3)) {
      jj_consume_token(SELECT);
      jj_consume_token(lparen);
      privilege_method_list();
      jj_consume_token(rparen);
    } else if (jj_2_1697(3)) {
      jj_consume_token(DELETE);
    } else if (jj_2_1698(3)) {
      jj_consume_token(INSERT);
      if (jj_2_1691(3)) {
        jj_consume_token(lparen);
        privilege_column_list();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_1699(3)) {
      jj_consume_token(UPDATE);
      if (jj_2_1692(3)) {
        jj_consume_token(lparen);
        privilege_column_list();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_1700(3)) {
      jj_consume_token(REFERENCES);
      if (jj_2_1693(3)) {
        jj_consume_token(lparen);
        privilege_column_list();
        jj_consume_token(rparen);
      } else {
        ;
      }
    } else if (jj_2_1701(3)) {
      jj_consume_token(USAGE);
    } else if (jj_2_1702(3)) {
      jj_consume_token(TRIGGER);
    } else if (jj_2_1703(3)) {
      jj_consume_token(UNDER);
    } else if (jj_2_1704(3)) {
      jj_consume_token(EXECUTE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::privilege_method_list() {
    specific_routine_designator();
    while (!hasError) {
      if (jj_2_1705(3)) {
        ;
      } else {
        goto end_label_87;
      }
      jj_consume_token(570);
      specific_routine_designator();
    }
    end_label_87: ;
}


void SqlParser::privilege_column_list() {
    column_name_list();
}


void SqlParser::grantee() {
    if (jj_2_1706(3)) {
      jj_consume_token(PUBLIC);
    } else if (jj_2_1707(3)) {
      identifier();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::grantor() {
    if (jj_2_1708(3)) {
      jj_consume_token(CURRENT_USER);
    } else if (jj_2_1709(3)) {
      jj_consume_token(CURRENT_ROLE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::role_definition() {
    jj_consume_token(CREATE);
    jj_consume_token(ROLE);
    identifier();
    if (jj_2_1710(3)) {
      jj_consume_token(WITH);
      jj_consume_token(ADMIN);
      grantor();
    } else {
      ;
    }
}


void SqlParser::grant_role_statement() {
    jj_consume_token(GRANT);
    identifier();
    while (!hasError) {
      if (jj_2_1711(3)) {
        ;
      } else {
        goto end_label_88;
      }
      jj_consume_token(570);
      identifier();
    }
    end_label_88: ;
    jj_consume_token(TO);
    grantee();
    while (!hasError) {
      if (jj_2_1712(3)) {
        ;
      } else {
        goto end_label_89;
      }
      jj_consume_token(570);
      grantee();
    }
    end_label_89: ;
    if (jj_2_1713(3)) {
      jj_consume_token(WITH);
      jj_consume_token(ADMIN);
      jj_consume_token(OPTION);
    } else {
      ;
    }
    if (jj_2_1714(3)) {
      jj_consume_token(GRANTED);
      jj_consume_token(BY);
      grantor();
    } else {
      ;
    }
}


void SqlParser::drop_role_statement() {
    jj_consume_token(DROP);
    jj_consume_token(ROLE);
    identifier();
}


void SqlParser::revoke_statement() {
    if (jj_2_1715(3)) {
      revoke_privilege_statement();
    } else if (jj_2_1716(3)) {
      revoke_role_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::revoke_privilege_statement() {
    jj_consume_token(REVOKE);
    if (jj_2_1717(3)) {
      revoke_option_extension();
    } else {
      ;
    }
    privileges();
    jj_consume_token(FROM);
    grantee();
    while (!hasError) {
      if (jj_2_1718(3)) {
        ;
      } else {
        goto end_label_90;
      }
      jj_consume_token(570);
      grantee();
    }
    end_label_90: ;
    if (jj_2_1719(3)) {
      jj_consume_token(GRANTED);
      jj_consume_token(BY);
      grantor();
    } else {
      ;
    }
    drop_behavior();
}


void SqlParser::revoke_option_extension() {
    if (jj_2_1720(3)) {
      jj_consume_token(GRANT);
      jj_consume_token(OPTION);
      jj_consume_token(FOR);
    } else if (jj_2_1721(3)) {
      jj_consume_token(HIERARCHY);
      jj_consume_token(OPTION);
      jj_consume_token(FOR);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::revoke_role_statement() {
    jj_consume_token(REVOKE);
    if (jj_2_1722(3)) {
      jj_consume_token(ADMIN);
      jj_consume_token(OPTION);
      jj_consume_token(FOR);
    } else {
      ;
    }
    identifier();
    while (!hasError) {
      if (jj_2_1723(3)) {
        ;
      } else {
        goto end_label_91;
      }
      jj_consume_token(570);
      identifier();
    }
    end_label_91: ;
    jj_consume_token(FROM);
    grantee();
    while (!hasError) {
      if (jj_2_1724(3)) {
        ;
      } else {
        goto end_label_92;
      }
      jj_consume_token(570);
      grantee();
    }
    end_label_92: ;
    if (jj_2_1725(3)) {
      jj_consume_token(GRANTED);
      jj_consume_token(BY);
      grantor();
    } else {
      ;
    }
    drop_behavior();
}


void SqlParser::SQL_client_module_definition() {
    module_name_clause();
    language_clause();
    module_authorization_clause();
    if (jj_2_1726(3)) {
      module_path_specification();
    } else {
      ;
    }
    if (jj_2_1727(3)) {
      module_transform_group_specification();
    } else {
      ;
    }
    if (jj_2_1728(3)) {
      module_collations();
    } else {
      ;
    }
    while (!hasError) {
      if (jj_2_1729(3)) {
        ;
      } else {
        goto end_label_93;
      }
      temporary_table_declaration();
    }
    end_label_93: ;
    while (!hasError) {
      module_contents();
      if (jj_2_1730(3)) {
        ;
      } else {
        goto end_label_94;
      }
    }
    end_label_94: ;
}


void SqlParser::module_authorization_clause() {
    if (jj_2_1737(3)) {
      jj_consume_token(SCHEMA);
      schema_name();
    } else if (jj_2_1738(3)) {
      jj_consume_token(AUTHORIZATION);
      identifier();
      if (jj_2_1733(3)) {
        jj_consume_token(FOR);
        jj_consume_token(STATIC);
        if (jj_2_1731(3)) {
          jj_consume_token(ONLY);
        } else if (jj_2_1732(3)) {
          jj_consume_token(AND);
          jj_consume_token(DYNAMIC);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } else {
        ;
      }
    } else if (jj_2_1739(3)) {
      jj_consume_token(SCHEMA);
      schema_name();
      jj_consume_token(AUTHORIZATION);
      identifier();
      if (jj_2_1736(3)) {
        jj_consume_token(FOR);
        jj_consume_token(STATIC);
        if (jj_2_1734(3)) {
          jj_consume_token(ONLY);
        } else if (jj_2_1735(3)) {
          jj_consume_token(AND);
          jj_consume_token(DYNAMIC);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } else {
        ;
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::module_path_specification() {
    path_specification();
}


void SqlParser::module_transform_group_specification() {
    transform_group_specification();
}


void SqlParser::module_collations() {
    while (!hasError) {
      module_collation_specification();
      if (jj_2_1740(3)) {
        ;
      } else {
        goto end_label_95;
      }
    }
    end_label_95: ;
}


void SqlParser::module_collation_specification() {
    jj_consume_token(COLLATION);
    schema_qualified_name();
    if (jj_2_1741(3)) {
      jj_consume_token(FOR);
      character_set_specification_list();
    } else {
      ;
    }
}


void SqlParser::character_set_specification_list() {
    character_set_specification();
    while (!hasError) {
      if (jj_2_1742(3)) {
        ;
      } else {
        goto end_label_96;
      }
      jj_consume_token(570);
      character_set_specification();
    }
    end_label_96: ;
}


void SqlParser::module_contents() {
    if (jj_2_1743(3)) {
      declare_cursor();
    } else if (jj_2_1744(3)) {
      dynamic_declare_cursor();
    } else if (jj_2_1745(3)) {
      externally_invoked_procedure();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::module_name_clause() {
    jj_consume_token(MODULE);
    if (jj_2_1746(3)) {
      identifier();
    } else {
      ;
    }
    if (jj_2_1747(3)) {
      module_character_set_specification();
    } else {
      ;
    }
}


void SqlParser::module_character_set_specification() {
    jj_consume_token(NAMES);
    jj_consume_token(ARE);
    character_set_specification();
}


void SqlParser::externally_invoked_procedure() {
    jj_consume_token(PROCEDURE);
    identifier();
    host_parameter_declaration_list();
    jj_consume_token(semicolon);
    SQL_procedure_statement();
    jj_consume_token(semicolon);
}


void SqlParser::host_parameter_declaration_list() {
    jj_consume_token(lparen);
    host_parameter_declaration();
    while (!hasError) {
      if (jj_2_1748(3)) {
        ;
      } else {
        goto end_label_97;
      }
      jj_consume_token(570);
      host_parameter_declaration();
    }
    end_label_97: ;
    jj_consume_token(rparen);
}


void SqlParser::host_parameter_declaration() {
    if (jj_2_1749(3)) {
      host_parameter_name();
      host_parameter_data_type();
    } else if (jj_2_1750(3)) {
      status_parameter();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::host_parameter_data_type() {
    data_type();
    if (jj_2_1751(3)) {
      locator_indication();
    } else {
      ;
    }
}


void SqlParser::status_parameter() {
    jj_consume_token(SQLSTATE);
}


void SqlParser::SQL_procedure_statement() {
    SQL_executable_statement();
}


void SqlParser::SQL_executable_statement() {
    if (jj_2_1752(3)) {
      SQL_schema_statement();
    } else if (jj_2_1753(3)) {
      SQL_data_statement();
    } else if (jj_2_1754(3)) {
      SQL_control_statement();
    } else if (jj_2_1755(3)) {
      SQL_transaction_statement();
    } else if (jj_2_1756(3)) {
      SQL_connection_statement();
    } else if (jj_2_1757(3)) {
      SQL_session_statement();
    } else if (jj_2_1758(3)) {
      SQL_diagnostics_statement();
    } else if (jj_2_1759(3)) {
      SQL_dynamic_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::SQL_schema_statement() {
    if (jj_2_1760(3)) {
      SQL_schema_definition_statement();
    } else if (jj_2_1761(3)) {
      SQL_schema_manipulation_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::SQL_schema_definition_statement() {
    if (jj_2_1762(3)) {
      schema_definition();
    } else if (jj_2_1763(3)) {
      table_definition();
    } else if (jj_2_1764(4)) {
      view_definition();
    } else if (jj_2_1765(3)) {
      SQL_invoked_routine();
    } else if (jj_2_1766(3)) {
      grant_statement();
    } else if (jj_2_1767(3)) {
      role_definition();
    } else if (jj_2_1768(3)) {
      domain_definition();
    } else if (jj_2_1769(3)) {
      character_set_definition();
    } else if (jj_2_1770(3)) {
      collation_definition();
    } else if (jj_2_1771(3)) {
      transliteration_definition();
    } else if (jj_2_1772(3)) {
      assertion_definition();
    } else if (jj_2_1773(3)) {
      trigger_definition();
    } else if (jj_2_1774(3)) {
      user_defined_type_definition();
    } else if (jj_2_1775(3)) {
      user_defined_cast_definition();
    } else if (jj_2_1776(3)) {
      user_defined_ordering_definition();
    } else if (jj_2_1777(3)) {
      transform_definition();
    } else if (jj_2_1778(3)) {
      sequence_generator_definition();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::SQL_schema_manipulation_statement() {
    if (jj_2_1779(3)) {
      drop_schema_statement();
    } else if (jj_2_1780(3)) {
      alter_table_statement();
    } else if (jj_2_1781(3)) {
      drop_table_statement();
    } else if (jj_2_1782(3)) {
      drop_view_statement();
    } else if (jj_2_1783(3)) {
      alter_routine_statement();
    } else if (jj_2_1784(3)) {
      drop_routine_statement();
    } else if (jj_2_1785(3)) {
      drop_user_defined_cast_statement();
    } else if (jj_2_1786(3)) {
      revoke_statement();
    } else if (jj_2_1787(3)) {
      drop_role_statement();
    } else if (jj_2_1788(3)) {
      alter_domain_statement();
    } else if (jj_2_1789(3)) {
      drop_domain_statement();
    } else if (jj_2_1790(3)) {
      drop_character_set_statement();
    } else if (jj_2_1791(3)) {
      drop_collation_statement();
    } else if (jj_2_1792(3)) {
      drop_transliteration_statement();
    } else if (jj_2_1793(3)) {
      drop_assertion_statement();
    } else if (jj_2_1794(3)) {
      drop_trigger_statement();
    } else if (jj_2_1795(3)) {
      alter_type_statement();
    } else if (jj_2_1796(3)) {
      drop_data_type_statement();
    } else if (jj_2_1797(3)) {
      drop_user_defined_ordering_statement();
    } else if (jj_2_1798(3)) {
      alter_transform_statement();
    } else if (jj_2_1799(3)) {
      drop_transform_statement();
    } else if (jj_2_1800(3)) {
      alter_sequence_generator_statement();
    } else if (jj_2_1801(3)) {
      drop_sequence_generator_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::SQL_data_statement() {
    if (jj_2_1802(3)) {
      open_statement();
    } else if (jj_2_1803(3)) {
      fetch_statement();
    } else if (jj_2_1804(3)) {
      close_statement();
    } else if (jj_2_1805(3)) {
      select_statement_single_row();
    } else if (jj_2_1806(3)) {
      SQL_data_change_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::SQL_data_change_statement() {
    if (jj_2_1807(3)) {
      delete_statement_positioned();
    } else if (jj_2_1808(3)) {
      delete_statement_searched();
    } else if (jj_2_1809(3)) {
      insert_statement();
    } else if (jj_2_1810(3)) {
      update_statement_positioned();
    } else if (jj_2_1811(3)) {
      update_statement_searched();
    } else if (jj_2_1812(3)) {
      truncate_table_statement();
    } else if (jj_2_1813(3)) {
      merge_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::SQL_control_statement() {
    if (jj_2_1814(3)) {
      call_statement();
    } else if (jj_2_1815(3)) {
      return_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::SQL_transaction_statement() {
    if (jj_2_1816(3)) {
      start_transaction_statement();
    } else if (jj_2_1817(3)) {
      set_transaction_statement();
    } else if (jj_2_1818(3)) {
      set_constraints_mode_statement();
    } else if (jj_2_1819(3)) {
      savepoint_statement();
    } else if (jj_2_1820(3)) {
      release_savepoint_statement();
    } else if (jj_2_1821(3)) {
      commit_statement();
    } else if (jj_2_1822(3)) {
      rollback_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::SQL_connection_statement() {
    if (jj_2_1823(3)) {
      connect_statement();
    } else if (jj_2_1824(3)) {
      set_connection_statement();
    } else if (jj_2_1825(3)) {
      disconnect_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::SQL_session_statement() {
    if (jj_2_1826(3)) {
      set_session_user_identifier_statement();
    } else if (jj_2_1827(3)) {
      set_role_statement();
    } else if (jj_2_1828(3)) {
      set_local_time_zone_statement();
    } else if (jj_2_1829(3)) {
      set_session_characteristics_statement();
    } else if (jj_2_1830(3)) {
      set_catalog_statement();
    } else if (jj_2_1831(3)) {
      set_schema_statement();
    } else if (jj_2_1832(3)) {
      set_names_statement();
    } else if (jj_2_1833(3)) {
      set_path_statement();
    } else if (jj_2_1834(3)) {
      set_transform_group_statement();
    } else if (jj_2_1835(3)) {
      set_session_collation_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::SQL_diagnostics_statement() {
    get_diagnostics_statement();
}


void SqlParser::SQL_dynamic_statement() {
    if (jj_2_1836(3)) {
      SQL_descriptor_statement();
    } else if (jj_2_1837(3)) {
      prepare_statement();
    } else if (jj_2_1838(3)) {
      deallocate_prepared_statement();
    } else if (jj_2_1839(3)) {
      describe_statement();
    } else if (jj_2_1840(3)) {
      execute_statement();
    } else if (jj_2_1841(3)) {
      execute_immediate_statement();
    } else if (jj_2_1842(3)) {
      SQL_dynamic_data_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::SQL_dynamic_data_statement() {
    if (jj_2_1843(3)) {
      allocate_cursor_statement();
    } else if (jj_2_1844(3)) {
      dynamic_open_statement();
    } else if (jj_2_1845(3)) {
      dynamic_fetch_statement();
    } else if (jj_2_1846(3)) {
      dynamic_close_statement();
    } else if (jj_2_1847(3)) {
      dynamic_delete_statement_positioned();
    } else if (jj_2_1848(3)) {
      dynamic_update_statement_positioned();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::SQL_descriptor_statement() {
    if (jj_2_1849(3)) {
      allocate_descriptor_statement();
    } else if (jj_2_1850(3)) {
      deallocate_descriptor_statement();
    } else if (jj_2_1851(3)) {
      set_descriptor_statement();
    } else if (jj_2_1852(3)) {
      get_descriptor_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::declare_cursor() {
    jj_consume_token(DECLARE);
    cursor_name();
    cursor_properties();
    jj_consume_token(FOR);
    cursor_specification();
}


void SqlParser::cursor_properties() {
    if (jj_2_1853(3)) {
      cursor_sensitivity();
    } else {
      ;
    }
    if (jj_2_1854(3)) {
      cursor_scrollability();
    } else {
      ;
    }
    jj_consume_token(CURSOR);
    if (jj_2_1855(3)) {
      cursor_holdability();
    } else {
      ;
    }
    if (jj_2_1856(3)) {
      cursor_returnability();
    } else {
      ;
    }
}


void SqlParser::cursor_sensitivity() {
    if (jj_2_1857(3)) {
      jj_consume_token(SENSITIVE);
    } else if (jj_2_1858(3)) {
      jj_consume_token(INSENSITIVE);
    } else if (jj_2_1859(3)) {
      jj_consume_token(ASENSITIVE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::cursor_scrollability() {
    if (jj_2_1860(3)) {
      jj_consume_token(SCROLL);
    } else if (jj_2_1861(3)) {
      jj_consume_token(NO);
      jj_consume_token(SCROLL);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::cursor_holdability() {
    if (jj_2_1862(3)) {
      jj_consume_token(WITH);
      jj_consume_token(HOLD);
    } else if (jj_2_1863(3)) {
      jj_consume_token(WITHOUT);
      jj_consume_token(HOLD);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::cursor_returnability() {
    if (jj_2_1864(3)) {
      jj_consume_token(WITH);
      jj_consume_token(RETURN);
    } else if (jj_2_1865(3)) {
      jj_consume_token(WITHOUT);
      jj_consume_token(RETURN);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::cursor_specification() {
    query_expression();
    if (jj_2_1866(3)) {
Unsupported *jjtn001 = new Unsupported(JJTUNSUPPORTED);
                           bool jjtc001 = true;
                           jjtree.openNodeScope(jjtn001);
                           jjtreeOpenNodeScope(jjtn001);
      try {
        updatability_clause();
      } catch ( ...) {
if (jjtc001) {
                             jjtree.clearNodeScope(jjtn001);
                             jjtc001 = false;
                           } else {
                             jjtree.popNode();
                           }
      }
if (jjtc001) {
                             jjtree.closeNodeScope(jjtn001, true);
                             if (jjtree.nodeCreated()) {
                              jjtreeCloseNodeScope(jjtn001);
                             }
                           }
    } else {
      ;
    }
}


void SqlParser::updatability_clause() {
    jj_consume_token(FOR);
    if (jj_2_1868(3)) {
      jj_consume_token(READ);
      jj_consume_token(ONLY);
    } else if (jj_2_1869(3)) {
      jj_consume_token(UPDATE);
      if (jj_2_1867(3)) {
        jj_consume_token(OF);
        column_name_list();
      } else {
        ;
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::open_statement() {
    jj_consume_token(OPEN);
    cursor_name();
}


void SqlParser::fetch_statement() {
    jj_consume_token(FETCH);
    if (jj_2_1871(3)) {
      if (jj_2_1870(3)) {
        fetch_orientation();
      } else {
        ;
      }
      jj_consume_token(FROM);
    } else {
      ;
    }
    cursor_name();
    jj_consume_token(INTO);
    fetch_target_list();
}


void SqlParser::fetch_orientation() {
    if (jj_2_1874(3)) {
      jj_consume_token(NEXT);
    } else if (jj_2_1875(3)) {
      jj_consume_token(PRIOR);
    } else if (jj_2_1876(3)) {
      jj_consume_token(FIRST);
    } else if (jj_2_1877(3)) {
      jj_consume_token(LAST);
    } else if (jj_2_1878(3)) {
      if (jj_2_1872(3)) {
        jj_consume_token(ABSOLUTE);
      } else if (jj_2_1873(3)) {
        jj_consume_token(RELATIVE);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
      simple_value_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::fetch_target_list() {
    target_specification();
    while (!hasError) {
      if (jj_2_1879(3)) {
        ;
      } else {
        goto end_label_98;
      }
      jj_consume_token(570);
      target_specification();
    }
    end_label_98: ;
}


void SqlParser::close_statement() {
    jj_consume_token(CLOSE);
    cursor_name();
}


void SqlParser::select_statement_single_row() {
    jj_consume_token(SELECT);
    if (jj_2_1880(3)) {
      set_quantifier();
    } else {
      ;
    }
    select_list();
    if (jj_2_1881(3)) {
      jj_consume_token(INTO);
      select_target_list();
    } else {
      ;
    }
    if (jj_2_1882(3)) {
      table_expression();
    } else {
      ;
    }
}


void SqlParser::select_target_list() {
    target_specification();
    while (!hasError) {
      if (jj_2_1883(3)) {
        ;
      } else {
        goto end_label_99;
      }
      jj_consume_token(570);
      target_specification();
    }
    end_label_99: ;
}


void SqlParser::delete_statement_positioned() {
    jj_consume_token(DELETE);
    jj_consume_token(FROM);
    target_table();
    if (jj_2_1885(3)) {
      if (jj_2_1884(3)) {
        jj_consume_token(AS);
      } else {
        ;
      }
      identifier();
    } else {
      ;
    }
    jj_consume_token(WHERE);
    jj_consume_token(CURRENT);
    jj_consume_token(OF);
    cursor_name();
}


void SqlParser::target_table() {
    if (jj_2_1886(3)) {
      table_name();
    } else if (jj_2_1887(3)) {
      jj_consume_token(ONLY);
      jj_consume_token(lparen);
      table_name();
      jj_consume_token(rparen);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::delete_statement_searched() {
    jj_consume_token(DELETE);
    jj_consume_token(FROM);
    target_table();
    if (jj_2_1889(3)) {
      if (jj_2_1888(3)) {
        jj_consume_token(AS);
      } else {
        ;
      }
      identifier();
    } else {
      ;
    }
    if (jj_2_1890(3)) {
      jj_consume_token(WHERE);
      search_condition();
    } else {
      ;
    }
}


void SqlParser::truncate_table_statement() {
    jj_consume_token(TRUNCATE);
    jj_consume_token(TABLE);
    target_table();
    if (jj_2_1891(3)) {
      identity_column_restart_option();
    } else {
      ;
    }
}


void SqlParser::identity_column_restart_option() {
    if (jj_2_1892(3)) {
      jj_consume_token(CONTINUE);
      jj_consume_token(IDENTITY);
    } else if (jj_2_1893(3)) {
      jj_consume_token(RESTART);
      jj_consume_token(IDENTITY);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::insert_statement() {/*@bgen(jjtree) Insert */
  Insert *jjtn000 = new Insert(JJTINSERT);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(INSERT);
      jj_consume_token(INTO);
      insertion_target();
      insert_columns_and_source();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::insertion_target() {
    table_name();
}


void SqlParser::insert_columns_and_source() {
    if (jj_2_1894(3)) {
      from_subquery();
    } else if (jj_2_1895(3)) {
      from_constructor();
    } else if (jj_2_1896(3)) {
      from_default();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::from_subquery() {
    if (jj_2_1897(3)) {
      jj_consume_token(lparen);
      insert_column_list();
      jj_consume_token(rparen);
    } else {
      ;
    }
    if (jj_2_1898(3)) {
      override_clause();
    } else {
      ;
    }
    query_expression();
}


void SqlParser::from_constructor() {
    if (jj_2_1899(3)) {
      jj_consume_token(lparen);
      insert_column_list();
      jj_consume_token(rparen);
    } else {
      ;
    }
    if (jj_2_1900(3)) {
      override_clause();
    } else {
      ;
    }
    contextually_typed_table_value_constructor();
}


void SqlParser::override_clause() {
    if (jj_2_1901(3)) {
      jj_consume_token(OVERRIDING);
      jj_consume_token(USER);
      jj_consume_token(VALUE);
    } else if (jj_2_1902(3)) {
      jj_consume_token(OVERRIDING);
      jj_consume_token(SYSTEM);
      jj_consume_token(VALUE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::from_default() {
    jj_consume_token(DEFAULT_);
    jj_consume_token(VALUES);
}


void SqlParser::insert_column_list() {
    column_name_list();
}


void SqlParser::merge_statement() {
    jj_consume_token(MERGE);
    jj_consume_token(INTO);
    target_table();
    if (jj_2_1904(3)) {
      if (jj_2_1903(3)) {
        jj_consume_token(AS);
      } else {
        ;
      }
      identifier();
    } else {
      ;
    }
    jj_consume_token(USING);
    table_reference();
    jj_consume_token(ON);
    search_condition();
    merge_operation_specification();
}


void SqlParser::merge_operation_specification() {
    while (!hasError) {
      merge_when_clause();
      if (jj_2_1905(3)) {
        ;
      } else {
        goto end_label_100;
      }
    }
    end_label_100: ;
}


void SqlParser::merge_when_clause() {
    if (jj_2_1906(3)) {
      merge_when_matched_clause();
    } else if (jj_2_1907(3)) {
      merge_when_not_matched_clause();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::merge_when_matched_clause() {
    jj_consume_token(WHEN);
    jj_consume_token(MATCHED);
    if (jj_2_1908(3)) {
      jj_consume_token(AND);
      search_condition();
    } else {
      ;
    }
    jj_consume_token(THEN);
    merge_update_or_delete_specification();
}


void SqlParser::merge_update_or_delete_specification() {
    if (jj_2_1909(3)) {
      merge_update_specification();
    } else if (jj_2_1910(3)) {
      merge_delete_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::merge_when_not_matched_clause() {
    jj_consume_token(WHEN);
    jj_consume_token(NOT);
    jj_consume_token(MATCHED);
    if (jj_2_1911(3)) {
      jj_consume_token(AND);
      search_condition();
    } else {
      ;
    }
    jj_consume_token(THEN);
    merge_insert_specification();
}


void SqlParser::merge_update_specification() {
    jj_consume_token(UPDATE);
    jj_consume_token(SET);
    set_clause_list();
}


void SqlParser::merge_delete_specification() {
    jj_consume_token(DELETE);
}


void SqlParser::merge_insert_specification() {
    jj_consume_token(INSERT);
    if (jj_2_1912(3)) {
      jj_consume_token(lparen);
      insert_column_list();
      jj_consume_token(rparen);
    } else {
      ;
    }
    if (jj_2_1913(3)) {
      override_clause();
    } else {
      ;
    }
    jj_consume_token(VALUES);
    merge_insert_value_list();
}


void SqlParser::merge_insert_value_list() {
    jj_consume_token(lparen);
    merge_insert_value_element();
    while (!hasError) {
      if (jj_2_1914(3)) {
        ;
      } else {
        goto end_label_101;
      }
      jj_consume_token(570);
      merge_insert_value_element();
    }
    end_label_101: ;
    jj_consume_token(rparen);
}


void SqlParser::merge_insert_value_element() {
    if (jj_2_1915(3)) {
      value_expression();
    } else if (jj_2_1916(3)) {
      contextually_typed_value_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::update_statement_positioned() {
    jj_consume_token(UPDATE);
    target_table();
    if (jj_2_1918(3)) {
      if (jj_2_1917(3)) {
        jj_consume_token(AS);
      } else {
        ;
      }
      identifier();
    } else {
      ;
    }
    jj_consume_token(SET);
    set_clause_list();
    jj_consume_token(WHERE);
    jj_consume_token(CURRENT);
    jj_consume_token(OF);
    cursor_name();
}


void SqlParser::update_statement_searched() {
    jj_consume_token(UPDATE);
    target_table();
    if (jj_2_1920(3)) {
      if (jj_2_1919(3)) {
        jj_consume_token(AS);
      } else {
        ;
      }
      identifier();
    } else {
      ;
    }
    jj_consume_token(SET);
    set_clause_list();
    if (jj_2_1921(3)) {
      jj_consume_token(WHERE);
      search_condition();
    } else {
      ;
    }
}


void SqlParser::set_clause_list() {
    set_clause();
    while (!hasError) {
      if (jj_2_1922(3)) {
        ;
      } else {
        goto end_label_102;
      }
      jj_consume_token(570);
      set_clause();
    }
    end_label_102: ;
}


void SqlParser::set_clause() {
    if (jj_2_1923(3)) {
      multiple_column_assignment();
    } else if (jj_2_1924(3)) {
      set_target();
      jj_consume_token(EQUAL);
      update_source();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::set_target() {
    if (jj_2_1925(3)) {
      update_target();
    } else if (jj_2_1926(3)) {
      mutated_set_clause();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::multiple_column_assignment() {
    set_target_list();
    jj_consume_token(EQUAL);
    assigned_row();
}


void SqlParser::set_target_list() {
    jj_consume_token(lparen);
    set_target();
    while (!hasError) {
      if (jj_2_1927(3)) {
        ;
      } else {
        goto end_label_103;
      }
      jj_consume_token(570);
      set_target();
    }
    end_label_103: ;
    jj_consume_token(rparen);
}


void SqlParser::assigned_row() {
    contextually_typed_row_value_expression();
}


void SqlParser::update_target() {
    if (jj_2_1928(3)) {
      identifier();
    } else if (jj_2_1929(3)) {
      identifier();
      left_bracket_or_trigraph();
      simple_value_specification();
      right_bracket_or_trigraph();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::mutated_set_clause() {
    identifier();
    while (!hasError) {
      jj_consume_token(569);
      identifier();
      if (jj_2_1930(3)) {
        ;
      } else {
        goto end_label_104;
      }
    }
    end_label_104: ;
}


void SqlParser::mutated_target() {
    if (jj_2_1931(3)) {
      identifier();
    } else if (jj_2_1932(3)) {
      mutated_set_clause();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::update_source() {
    if (jj_2_1933(3)) {
      value_expression();
    } else if (jj_2_1934(3)) {
      contextually_typed_value_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::temporary_table_declaration() {
    jj_consume_token(DECLARE);
    jj_consume_token(LOCAL);
    jj_consume_token(TEMPORARY);
    jj_consume_token(TABLE);
    table_name();
    table_element_list();
    if (jj_2_1935(3)) {
      jj_consume_token(ON);
      jj_consume_token(COMMIT);
      table_commit_action();
      jj_consume_token(ROWS);
    } else {
      ;
    }
}


void SqlParser::call_statement() {
    jj_consume_token(CALL);
    routine_invocation();
}


void SqlParser::return_statement() {
    jj_consume_token(RETURN);
    return_value();
}


void SqlParser::return_value() {
    if (jj_2_1936(3)) {
      value_expression();
    } else if (jj_2_1937(3)) {
      jj_consume_token(NULL_);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::start_transaction_statement() {
    jj_consume_token(START);
    jj_consume_token(TRANSACTION);
    if (jj_2_1938(3)) {
      transaction_characteristics();
    } else {
      ;
    }
}


void SqlParser::set_transaction_statement() {
    jj_consume_token(SET);
    if (jj_2_1939(3)) {
      jj_consume_token(LOCAL);
    } else {
      ;
    }
    jj_consume_token(TRANSACTION);
    if (jj_2_1940(3)) {
      transaction_characteristics();
    } else {
      ;
    }
}


void SqlParser::transaction_characteristics() {
    transaction_mode();
    while (!hasError) {
      if (jj_2_1941(3)) {
        ;
      } else {
        goto end_label_105;
      }
      jj_consume_token(570);
      transaction_mode();
    }
    end_label_105: ;
}


void SqlParser::transaction_mode() {
    if (jj_2_1942(3)) {
      isolation_level();
    } else if (jj_2_1943(3)) {
      transaction_access_mode();
    } else if (jj_2_1944(3)) {
      diagnostics_size();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::transaction_access_mode() {
    if (jj_2_1945(3)) {
      jj_consume_token(READ);
      jj_consume_token(ONLY);
    } else if (jj_2_1946(3)) {
      jj_consume_token(READ);
      jj_consume_token(WRITE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::isolation_level() {
    jj_consume_token(ISOLATION);
    jj_consume_token(LEVEL);
    level_of_isolation();
}


void SqlParser::level_of_isolation() {
    if (jj_2_1947(3)) {
      jj_consume_token(READ);
      jj_consume_token(UNCOMMITTED);
    } else if (jj_2_1948(3)) {
      jj_consume_token(READ);
      jj_consume_token(COMMITTED);
    } else if (jj_2_1949(3)) {
      jj_consume_token(REPEATABLE);
      jj_consume_token(READ);
    } else if (jj_2_1950(3)) {
      jj_consume_token(SERIALIZABLE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::diagnostics_size() {
    jj_consume_token(DIAGNOSTICS);
    jj_consume_token(SIZE);
    simple_value_specification();
}


void SqlParser::set_constraints_mode_statement() {
    jj_consume_token(SET);
    jj_consume_token(CONSTRAINTS);
    constraint_name_list();
    if (jj_2_1951(3)) {
      jj_consume_token(DEFERRED);
    } else if (jj_2_1952(3)) {
      jj_consume_token(IMMEDIATE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::constraint_name_list() {
    if (jj_2_1954(3)) {
      jj_consume_token(ALL);
    } else if (jj_2_1955(3)) {
      schema_qualified_name();
      while (!hasError) {
        if (jj_2_1953(3)) {
          ;
        } else {
          goto end_label_106;
        }
        jj_consume_token(570);
        schema_qualified_name();
      }
      end_label_106: ;
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::savepoint_statement() {
    jj_consume_token(SAVEPOINT);
    savepoint_specifier();
}


void SqlParser::savepoint_specifier() {
    identifier();
}


void SqlParser::release_savepoint_statement() {
    jj_consume_token(RELEASE);
    jj_consume_token(SAVEPOINT);
    savepoint_specifier();
}


void SqlParser::commit_statement() {
    jj_consume_token(COMMIT);
    if (jj_2_1956(3)) {
      jj_consume_token(WORK);
    } else {
      ;
    }
    if (jj_2_1958(3)) {
      jj_consume_token(AND);
      if (jj_2_1957(3)) {
        jj_consume_token(NO);
      } else {
        ;
      }
      jj_consume_token(CHAIN);
    } else {
      ;
    }
}


void SqlParser::rollback_statement() {
    jj_consume_token(ROLLBACK);
    if (jj_2_1959(3)) {
      jj_consume_token(WORK);
    } else {
      ;
    }
    if (jj_2_1961(3)) {
      jj_consume_token(AND);
      if (jj_2_1960(3)) {
        jj_consume_token(NO);
      } else {
        ;
      }
      jj_consume_token(CHAIN);
    } else {
      ;
    }
    if (jj_2_1962(3)) {
      savepoint_clause();
    } else {
      ;
    }
}


void SqlParser::savepoint_clause() {
    jj_consume_token(TO);
    jj_consume_token(SAVEPOINT);
    savepoint_specifier();
}


void SqlParser::connect_statement() {
    jj_consume_token(CONNECT);
    jj_consume_token(TO);
    connection_target();
}


void SqlParser::connection_target() {
    if (jj_2_1965(3)) {
      simple_value_specification();
      if (jj_2_1963(3)) {
        jj_consume_token(AS);
        simple_value_specification();
      } else {
        ;
      }
      if (jj_2_1964(3)) {
        jj_consume_token(USER);
        simple_value_specification();
      } else {
        ;
      }
    } else if (jj_2_1966(3)) {
      jj_consume_token(DEFAULT_);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::set_connection_statement() {
    jj_consume_token(SET);
    jj_consume_token(CONNECTION);
    connection_object();
}


void SqlParser::connection_object() {
    if (jj_2_1967(3)) {
      jj_consume_token(DEFAULT_);
    } else if (jj_2_1968(3)) {
      simple_value_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::disconnect_statement() {
    jj_consume_token(DISCONNECT);
    disconnect_object();
}


void SqlParser::disconnect_object() {
    if (jj_2_1969(3)) {
      connection_object();
    } else if (jj_2_1970(3)) {
      jj_consume_token(ALL);
    } else if (jj_2_1971(3)) {
      jj_consume_token(CURRENT);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::set_session_characteristics_statement() {
    jj_consume_token(SET);
    jj_consume_token(SESSION);
    jj_consume_token(CHARACTERISTICS);
    jj_consume_token(AS);
    session_characteristic_list();
}


void SqlParser::session_characteristic_list() {
    session_characteristic();
    while (!hasError) {
      if (jj_2_1972(3)) {
        ;
      } else {
        goto end_label_107;
      }
      jj_consume_token(570);
      session_characteristic();
    }
    end_label_107: ;
}


void SqlParser::session_characteristic() {
    session_transaction_characteristics();
}


void SqlParser::session_transaction_characteristics() {
    jj_consume_token(TRANSACTION);
    transaction_mode();
    while (!hasError) {
      if (jj_2_1973(3)) {
        ;
      } else {
        goto end_label_108;
      }
      jj_consume_token(570);
      transaction_mode();
    }
    end_label_108: ;
}


void SqlParser::set_session_user_identifier_statement() {
    jj_consume_token(SET);
    jj_consume_token(SESSION);
    jj_consume_token(AUTHORIZATION);
    value_specification();
}


void SqlParser::set_role_statement() {
    jj_consume_token(SET);
    jj_consume_token(ROLE);
    role_specification();
}


void SqlParser::role_specification() {
    if (jj_2_1974(3)) {
      value_specification();
    } else if (jj_2_1975(3)) {
      jj_consume_token(NONE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::set_local_time_zone_statement() {
    jj_consume_token(SET);
    jj_consume_token(TIME);
    jj_consume_token(ZONE);
    set_time_zone_value();
}


void SqlParser::set_time_zone_value() {
    if (jj_2_1976(3)) {
      interval_value_expression();
    } else if (jj_2_1977(3)) {
      jj_consume_token(LOCAL);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::set_catalog_statement() {
    jj_consume_token(SET);
    catalog_name_characteristic();
}


void SqlParser::catalog_name_characteristic() {
    jj_consume_token(CATALOG);
    value_specification();
}


void SqlParser::set_schema_statement() {
    jj_consume_token(SET);
    schema_name_characteristic();
}


void SqlParser::schema_name_characteristic() {
    jj_consume_token(SCHEMA);
    value_specification();
}


void SqlParser::set_names_statement() {
    jj_consume_token(SET);
    character_set_name_characteristic();
}


void SqlParser::character_set_name_characteristic() {
    jj_consume_token(NAMES);
    value_specification();
}


void SqlParser::set_path_statement() {
    jj_consume_token(SET);
    SQL_path_characteristic();
}


void SqlParser::SQL_path_characteristic() {
    jj_consume_token(PATH);
    value_specification();
}


void SqlParser::set_transform_group_statement() {
    jj_consume_token(SET);
    transform_group_characteristic();
}


void SqlParser::transform_group_characteristic() {
    if (jj_2_1978(3)) {
      jj_consume_token(DEFAULT_);
      jj_consume_token(TRANSFORM);
      jj_consume_token(GROUP);
      value_specification();
    } else if (jj_2_1979(3)) {
      jj_consume_token(TRANSFORM);
      jj_consume_token(GROUP);
      jj_consume_token(FOR);
      jj_consume_token(TYPE);
      path_resolved_user_defined_type_name();
      value_specification();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::set_session_collation_statement() {
    if (jj_2_1982(3)) {
      jj_consume_token(SET);
      jj_consume_token(COLLATION);
      collation_specification();
      if (jj_2_1980(3)) {
        jj_consume_token(FOR);
        character_set_specification_list();
      } else {
        ;
      }
    } else if (jj_2_1983(3)) {
      jj_consume_token(SET);
      jj_consume_token(NO);
      jj_consume_token(COLLATION);
      if (jj_2_1981(3)) {
        jj_consume_token(FOR);
        character_set_specification_list();
      } else {
        ;
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::collation_specification() {
    value_specification();
}


void SqlParser::allocate_descriptor_statement() {
    jj_consume_token(ALLOCATE);
    if (jj_2_1984(3)) {
      jj_consume_token(SQL);
    } else {
      ;
    }
    jj_consume_token(DESCRIPTOR);
    descriptor_name();
    if (jj_2_1985(3)) {
      jj_consume_token(WITH);
      jj_consume_token(MAX);
      simple_value_specification();
    } else {
      ;
    }
}


void SqlParser::deallocate_descriptor_statement() {
    jj_consume_token(DEALLOCATE);
    if (jj_2_1986(3)) {
      jj_consume_token(SQL);
    } else {
      ;
    }
    jj_consume_token(DESCRIPTOR);
    descriptor_name();
}


void SqlParser::get_descriptor_statement() {
    jj_consume_token(GET);
    if (jj_2_1987(3)) {
      jj_consume_token(SQL);
    } else {
      ;
    }
    jj_consume_token(DESCRIPTOR);
    descriptor_name();
    get_descriptor_information();
}


void SqlParser::get_descriptor_information() {
    if (jj_2_1990(3)) {
      get_header_information();
      while (!hasError) {
        if (jj_2_1988(3)) {
          ;
        } else {
          goto end_label_109;
        }
        jj_consume_token(570);
        get_header_information();
      }
      end_label_109: ;
    } else if (jj_2_1991(3)) {
      jj_consume_token(VALUE);
      simple_value_specification();
      get_item_information();
      while (!hasError) {
        if (jj_2_1989(3)) {
          ;
        } else {
          goto end_label_110;
        }
        jj_consume_token(570);
        get_item_information();
      }
      end_label_110: ;
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::get_header_information() {
    simple_target_specification_1();
    jj_consume_token(EQUAL);
    header_item_name();
}


void SqlParser::header_item_name() {
    if (jj_2_1992(3)) {
      jj_consume_token(COUNT);
    } else if (jj_2_1993(3)) {
      jj_consume_token(KEY_TYPE);
    } else if (jj_2_1994(3)) {
      jj_consume_token(DYNAMIC_FUNCTION);
    } else if (jj_2_1995(3)) {
      jj_consume_token(DYNAMIC_FUNCTION_CODE);
    } else if (jj_2_1996(3)) {
      jj_consume_token(TOP_LEVEL_COUNT);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::get_item_information() {
    simple_target_specification_2();
    jj_consume_token(EQUAL);
    descriptor_item_name();
}


void SqlParser::simple_target_specification_1() {
    simple_target_specification();
}


void SqlParser::simple_target_specification_2() {
    simple_target_specification();
}


void SqlParser::descriptor_item_name() {
    if (jj_2_1997(3)) {
      jj_consume_token(CARDINALITY);
    } else if (jj_2_1998(3)) {
      jj_consume_token(CHARACTER_SET_CATALOG);
    } else if (jj_2_1999(3)) {
      jj_consume_token(CHARACTER_SET_NAME);
    } else if (jj_2_2000(3)) {
      jj_consume_token(CHARACTER_SET_SCHEMA);
    } else if (jj_2_2001(3)) {
      jj_consume_token(COLLATION_CATALOG);
    } else if (jj_2_2002(3)) {
      jj_consume_token(COLLATION_NAME);
    } else if (jj_2_2003(3)) {
      jj_consume_token(COLLATION_SCHEMA);
    } else if (jj_2_2004(3)) {
      jj_consume_token(DATA);
    } else if (jj_2_2005(3)) {
      jj_consume_token(DATETIME_INTERVAL_CODE);
    } else if (jj_2_2006(3)) {
      jj_consume_token(DATETIME_INTERVAL_PRECISION);
    } else if (jj_2_2007(3)) {
      jj_consume_token(DEGREE);
    } else if (jj_2_2008(3)) {
      jj_consume_token(INDICATOR);
    } else if (jj_2_2009(3)) {
      jj_consume_token(KEY_MEMBER);
    } else if (jj_2_2010(3)) {
      jj_consume_token(LENGTH);
    } else if (jj_2_2011(3)) {
      jj_consume_token(LEVEL);
    } else if (jj_2_2012(3)) {
      jj_consume_token(NAME);
    } else if (jj_2_2013(3)) {
      jj_consume_token(NULLABLE);
    } else if (jj_2_2014(3)) {
      jj_consume_token(OCTET_LENGTH);
    } else if (jj_2_2015(3)) {
      jj_consume_token(PARAMETER_MODE);
    } else if (jj_2_2016(3)) {
      jj_consume_token(PARAMETER_ORDINAL_POSITION);
    } else if (jj_2_2017(3)) {
      jj_consume_token(PARAMETER_SPECIFIC_CATALOG);
    } else if (jj_2_2018(3)) {
      jj_consume_token(PARAMETER_SPECIFIC_NAME);
    } else if (jj_2_2019(3)) {
      jj_consume_token(PARAMETER_SPECIFIC_SCHEMA);
    } else if (jj_2_2020(3)) {
      jj_consume_token(PRECISION);
    } else if (jj_2_2021(3)) {
      jj_consume_token(RETURNED_CARDINALITY);
    } else if (jj_2_2022(3)) {
      jj_consume_token(RETURNED_LENGTH);
    } else if (jj_2_2023(3)) {
      jj_consume_token(RETURNED_OCTET_LENGTH);
    } else if (jj_2_2024(3)) {
      jj_consume_token(SCALE);
    } else if (jj_2_2025(3)) {
      jj_consume_token(SCOPE_CATALOG);
    } else if (jj_2_2026(3)) {
      jj_consume_token(SCOPE_NAME);
    } else if (jj_2_2027(3)) {
      jj_consume_token(SCOPE_SCHEMA);
    } else if (jj_2_2028(3)) {
      jj_consume_token(TYPE);
    } else if (jj_2_2029(3)) {
      jj_consume_token(UNNAMED);
    } else if (jj_2_2030(3)) {
      jj_consume_token(USER_DEFINED_TYPE_CATALOG);
    } else if (jj_2_2031(3)) {
      jj_consume_token(USER_DEFINED_TYPE_NAME);
    } else if (jj_2_2032(3)) {
      jj_consume_token(USER_DEFINED_TYPE_SCHEMA);
    } else if (jj_2_2033(3)) {
      jj_consume_token(USER_DEFINED_TYPE_CODE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::set_descriptor_statement() {
    jj_consume_token(SET);
    if (jj_2_2034(3)) {
      jj_consume_token(SQL);
    } else {
      ;
    }
    jj_consume_token(DESCRIPTOR);
    descriptor_name();
    set_descriptor_information();
}


void SqlParser::set_descriptor_information() {
    if (jj_2_2037(3)) {
      set_header_information();
      while (!hasError) {
        if (jj_2_2035(3)) {
          ;
        } else {
          goto end_label_111;
        }
        jj_consume_token(570);
        set_header_information();
      }
      end_label_111: ;
    } else if (jj_2_2038(3)) {
      jj_consume_token(VALUE);
      simple_value_specification();
      set_item_information();
      while (!hasError) {
        if (jj_2_2036(3)) {
          ;
        } else {
          goto end_label_112;
        }
        jj_consume_token(570);
        set_item_information();
      }
      end_label_112: ;
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::set_header_information() {
    header_item_name();
    jj_consume_token(EQUAL);
    simple_value_specification();
}


void SqlParser::set_item_information() {
    descriptor_item_name();
    jj_consume_token(EQUAL);
    simple_value_specification();
}


void SqlParser::prepare_statement() {
    jj_consume_token(PREPARE);
    SQL_identifier();
    if (jj_2_2039(3)) {
      attributes_specification();
    } else {
      ;
    }
    jj_consume_token(FROM);
    simple_value_specification();
}


void SqlParser::attributes_specification() {
    jj_consume_token(ATTRIBUTES);
    simple_value_specification();
}


void SqlParser::preparable_statement() {
    if (jj_2_2040(3)) {
      preparable_SQL_data_statement();
    } else if (jj_2_2041(3)) {
      preparable_SQL_schema_statement();
    } else if (jj_2_2042(3)) {
      preparable_SQL_transaction_statement();
    } else if (jj_2_2043(3)) {
      preparable_SQL_control_statement();
    } else if (jj_2_2044(3)) {
      preparable_SQL_session_statement();
    } else if (jj_2_2045(3)) {
      preparable_implementation_defined_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::preparable_SQL_data_statement() {
    if (jj_2_2046(3)) {
      delete_statement_searched();
    } else if (jj_2_2047(3)) {
      dynamic_single_row_select_statement();
    } else if (jj_2_2048(3)) {
      insert_statement();
    } else if (jj_2_2049(3)) {
      dynamic_select_statement();
    } else if (jj_2_2050(3)) {
      update_statement_searched();
    } else if (jj_2_2051(3)) {
      truncate_table_statement();
    } else if (jj_2_2052(3)) {
      merge_statement();
    } else if (jj_2_2053(3)) {
      preparable_dynamic_delete_statement_positioned();
    } else if (jj_2_2054(3)) {
      preparable_dynamic_update_statement_positioned();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::preparable_SQL_schema_statement() {
    SQL_schema_statement();
}


void SqlParser::preparable_SQL_transaction_statement() {
    SQL_transaction_statement();
}


void SqlParser::preparable_SQL_control_statement() {
    SQL_control_statement();
}


void SqlParser::preparable_SQL_session_statement() {
    SQL_session_statement();
}


void SqlParser::dynamic_select_statement() {
    cursor_specification();
}


void SqlParser::preparable_implementation_defined_statement() {
    character_string_literal();
}


void SqlParser::cursor_attributes() {
    while (!hasError) {
      cursor_attribute();
      if (jj_2_2055(3)) {
        ;
      } else {
        goto end_label_113;
      }
    }
    end_label_113: ;
}


void SqlParser::cursor_attribute() {
    if (jj_2_2056(3)) {
      cursor_sensitivity();
    } else if (jj_2_2057(3)) {
      cursor_scrollability();
    } else if (jj_2_2058(3)) {
      cursor_holdability();
    } else if (jj_2_2059(3)) {
      cursor_returnability();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::deallocate_prepared_statement() {
    jj_consume_token(DEALLOCATE);
    jj_consume_token(PREPARE);
    SQL_identifier();
}


void SqlParser::describe_statement() {
    if (jj_2_2060(3)) {
      describe_input_statement();
    } else if (jj_2_2061(3)) {
      describe_output_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::describe_input_statement() {
    jj_consume_token(DESCRIBE);
    jj_consume_token(INPUT);
    SQL_identifier();
    using_descriptor();
    if (jj_2_2062(3)) {
      nesting_option();
    } else {
      ;
    }
}


void SqlParser::describe_output_statement() {
    jj_consume_token(DESCRIBE);
    if (jj_2_2063(3)) {
      jj_consume_token(OUTPUT);
    } else {
      ;
    }
    described_object();
    using_descriptor();
    if (jj_2_2064(3)) {
      nesting_option();
    } else {
      ;
    }
}


void SqlParser::nesting_option() {
    if (jj_2_2065(3)) {
      jj_consume_token(WITH);
      jj_consume_token(NESTING);
    } else if (jj_2_2066(3)) {
      jj_consume_token(WITHOUT);
      jj_consume_token(NESTING);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::using_descriptor() {
    jj_consume_token(USING);
    if (jj_2_2067(3)) {
      jj_consume_token(SQL);
    } else {
      ;
    }
    jj_consume_token(DESCRIPTOR);
    descriptor_name();
}


void SqlParser::described_object() {
    if (jj_2_2068(3)) {
      SQL_identifier();
    } else if (jj_2_2069(3)) {
      jj_consume_token(CURSOR);
      extended_cursor_name();
      jj_consume_token(STRUCTURE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::input_using_clause() {
    if (jj_2_2070(3)) {
      using_arguments();
    } else if (jj_2_2071(3)) {
      using_input_descriptor();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::using_arguments() {
    jj_consume_token(USING);
    using_argument();
    while (!hasError) {
      if (jj_2_2072(3)) {
        ;
      } else {
        goto end_label_114;
      }
      jj_consume_token(570);
      using_argument();
    }
    end_label_114: ;
}


void SqlParser::using_argument() {
    general_value_specification();
}


void SqlParser::using_input_descriptor() {
    using_descriptor();
}


void SqlParser::output_using_clause() {
    if (jj_2_2073(3)) {
      into_arguments();
    } else if (jj_2_2074(3)) {
      into_descriptor();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::into_arguments() {
    jj_consume_token(INTO);
    into_argument();
    while (!hasError) {
      if (jj_2_2075(3)) {
        ;
      } else {
        goto end_label_115;
      }
      jj_consume_token(570);
      into_argument();
    }
    end_label_115: ;
}


void SqlParser::into_argument() {
    target_specification();
}


void SqlParser::into_descriptor() {
    jj_consume_token(INTO);
    if (jj_2_2076(3)) {
      jj_consume_token(SQL);
    } else {
      ;
    }
    jj_consume_token(DESCRIPTOR);
    descriptor_name();
}


void SqlParser::execute_statement() {
    jj_consume_token(EXECUTE);
    SQL_identifier();
    if (jj_2_2077(3)) {
      result_using_clause();
    } else {
      ;
    }
    if (jj_2_2078(3)) {
      parameter_using_clause();
    } else {
      ;
    }
}


void SqlParser::result_using_clause() {
    output_using_clause();
}


void SqlParser::parameter_using_clause() {
    input_using_clause();
}


void SqlParser::execute_immediate_statement() {
    jj_consume_token(EXECUTE);
    jj_consume_token(IMMEDIATE);
    simple_value_specification();
}


void SqlParser::dynamic_declare_cursor() {
    jj_consume_token(DECLARE);
    cursor_name();
    cursor_properties();
    jj_consume_token(FOR);
    identifier();
}


void SqlParser::allocate_cursor_statement() {
    jj_consume_token(ALLOCATE);
    extended_cursor_name();
    cursor_intent();
}


void SqlParser::cursor_intent() {
    if (jj_2_2079(3)) {
      statement_cursor();
    } else if (jj_2_2080(3)) {
      result_set_cursor();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::statement_cursor() {
    cursor_properties();
    jj_consume_token(FOR);
    extended_identifier();
}


void SqlParser::result_set_cursor() {
    if (jj_2_2081(3)) {
      jj_consume_token(CURSOR);
    } else {
      ;
    }
    jj_consume_token(FOR);
    jj_consume_token(PROCEDURE);
    specific_routine_designator();
}


void SqlParser::dynamic_open_statement() {
    jj_consume_token(OPEN);
    dynamic_cursor_name();
    if (jj_2_2082(3)) {
      input_using_clause();
    } else {
      ;
    }
}


void SqlParser::dynamic_fetch_statement() {
    jj_consume_token(FETCH);
    if (jj_2_2084(3)) {
      if (jj_2_2083(3)) {
        fetch_orientation();
      } else {
        ;
      }
      jj_consume_token(FROM);
    } else {
      ;
    }
    dynamic_cursor_name();
    output_using_clause();
}


void SqlParser::dynamic_single_row_select_statement() {
    query_specification();
}


void SqlParser::dynamic_close_statement() {
    jj_consume_token(CLOSE);
    dynamic_cursor_name();
}


void SqlParser::dynamic_delete_statement_positioned() {
    jj_consume_token(DELETE);
    jj_consume_token(FROM);
    target_table();
    jj_consume_token(WHERE);
    jj_consume_token(CURRENT);
    jj_consume_token(OF);
    dynamic_cursor_name();
}


void SqlParser::dynamic_update_statement_positioned() {
    jj_consume_token(UPDATE);
    target_table();
    jj_consume_token(SET);
    set_clause_list();
    jj_consume_token(WHERE);
    jj_consume_token(CURRENT);
    jj_consume_token(OF);
    dynamic_cursor_name();
}


void SqlParser::preparable_dynamic_delete_statement_positioned() {
    jj_consume_token(DELETE);
    if (jj_2_2085(3)) {
      jj_consume_token(FROM);
      target_table();
    } else {
      ;
    }
    jj_consume_token(WHERE);
    jj_consume_token(CURRENT);
    jj_consume_token(OF);
    preparable_dynamic_cursor_name();
}


void SqlParser::preparable_dynamic_cursor_name() {
    if (jj_2_2086(3)) {
      scope_option();
    } else {
      ;
    }
    cursor_name();
}


void SqlParser::preparable_dynamic_update_statement_positioned() {
    jj_consume_token(UPDATE);
    if (jj_2_2087(3)) {
      target_table();
    } else {
      ;
    }
    jj_consume_token(SET);
    set_clause_list();
    jj_consume_token(WHERE);
    jj_consume_token(CURRENT);
    jj_consume_token(OF);
    preparable_dynamic_cursor_name();
}


void SqlParser::direct_SQL_statement() {/*@bgen(jjtree) #DirectSqlStatement( true) */
  DirectSqlStatement *jjtn000 = new DirectSqlStatement(JJTDIRECTSQLSTATEMENT);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      directly_executable_statement();
      jj_consume_token(semicolon);
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000,  true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::directly_executable_statement() {
    if (jj_2_2088(3)) {
      direct_SQL_data_statement();
    } else if (jj_2_2089(3)) {
      SQL_schema_statement();
    } else if (jj_2_2090(3)) {
      SQL_transaction_statement();
    } else if (jj_2_2091(3)) {
      SQL_connection_statement();
    } else if (jj_2_2092(3)) {
      SQL_session_statement();
    } else if (jj_2_2093(3)) {
      direct_implementation_defined_statement();
    } else if (jj_2_2094(3)) {
      use_statement();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::direct_SQL_data_statement() {
    if (jj_2_2095(3)) {
      delete_statement_searched();
    } else if (jj_2_2096(3)) {
      direct_select_statement_multiple_rows();
    } else if (jj_2_2097(3)) {
      insert_statement();
    } else if (jj_2_2098(3)) {
      update_statement_searched();
    } else if (jj_2_2099(3)) {
      truncate_table_statement();
    } else if (jj_2_2100(3)) {
      merge_statement();
    } else if (jj_2_2101(3)) {
      temporary_table_declaration();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::direct_implementation_defined_statement() {
    character_string_literal();
}


void SqlParser::direct_select_statement_multiple_rows() {
    cursor_specification();
}


void SqlParser::get_diagnostics_statement() {
    jj_consume_token(GET);
    jj_consume_token(DIAGNOSTICS);
    SQL_diagnostics_information();
}


void SqlParser::SQL_diagnostics_information() {
    if (jj_2_2102(3)) {
      statement_information();
    } else if (jj_2_2103(3)) {
      condition_information();
    } else if (jj_2_2104(3)) {
      all_information();
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::statement_information() {
    statement_information_item();
    while (!hasError) {
      if (jj_2_2105(3)) {
        ;
      } else {
        goto end_label_116;
      }
      jj_consume_token(570);
      statement_information_item();
    }
    end_label_116: ;
}


void SqlParser::statement_information_item() {
    simple_target_specification();
    jj_consume_token(EQUAL);
    statement_information_item_name();
}


void SqlParser::statement_information_item_name() {
    if (jj_2_2106(3)) {
      jj_consume_token(NUMBER);
    } else if (jj_2_2107(3)) {
      jj_consume_token(MORE_);
    } else if (jj_2_2108(3)) {
      jj_consume_token(COMMAND_FUNCTION);
    } else if (jj_2_2109(3)) {
      jj_consume_token(COMMAND_FUNCTION_CODE);
    } else if (jj_2_2110(3)) {
      jj_consume_token(DYNAMIC_FUNCTION);
    } else if (jj_2_2111(3)) {
      jj_consume_token(DYNAMIC_FUNCTION_CODE);
    } else if (jj_2_2112(3)) {
      jj_consume_token(ROW_COUNT);
    } else if (jj_2_2113(3)) {
      jj_consume_token(TRANSACTIONS_COMMITTED);
    } else if (jj_2_2114(3)) {
      jj_consume_token(TRANSACTIONS_ROLLED_BACK);
    } else if (jj_2_2115(3)) {
      jj_consume_token(TRANSACTION_ACTIVE);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::condition_information() {
    jj_consume_token(CONDITION);
    simple_value_specification();
    condition_information_item();
    while (!hasError) {
      if (jj_2_2116(3)) {
        ;
      } else {
        goto end_label_117;
      }
      jj_consume_token(570);
      condition_information_item();
    }
    end_label_117: ;
}


void SqlParser::condition_information_item() {
    simple_target_specification();
    jj_consume_token(EQUAL);
    condition_information_item_name();
}


void SqlParser::condition_information_item_name() {
    if (jj_2_2117(3)) {
      jj_consume_token(CATALOG_NAME);
    } else if (jj_2_2118(3)) {
      jj_consume_token(CLASS_ORIGIN);
    } else if (jj_2_2119(3)) {
      jj_consume_token(COLUMN_NAME);
    } else if (jj_2_2120(3)) {
      jj_consume_token(CONDITION_NUMBER);
    } else if (jj_2_2121(3)) {
      jj_consume_token(CONNECTION_NAME);
    } else if (jj_2_2122(3)) {
      jj_consume_token(CONSTRAINT_CATALOG);
    } else if (jj_2_2123(3)) {
      jj_consume_token(CONSTRAINT_NAME);
    } else if (jj_2_2124(3)) {
      jj_consume_token(CONSTRAINT_SCHEMA);
    } else if (jj_2_2125(3)) {
      jj_consume_token(CURSOR_NAME);
    } else if (jj_2_2126(3)) {
      jj_consume_token(MESSAGE_LENGTH);
    } else if (jj_2_2127(3)) {
      jj_consume_token(MESSAGE_OCTET_LENGTH);
    } else if (jj_2_2128(3)) {
      jj_consume_token(MESSAGE_TEXT);
    } else if (jj_2_2129(3)) {
      jj_consume_token(PARAMETER_MODE);
    } else if (jj_2_2130(3)) {
      jj_consume_token(PARAMETER_NAME);
    } else if (jj_2_2131(3)) {
      jj_consume_token(PARAMETER_ORDINAL_POSITION);
    } else if (jj_2_2132(3)) {
      jj_consume_token(RETURNED_SQLSTATE);
    } else if (jj_2_2133(3)) {
      jj_consume_token(ROUTINE_CATALOG);
    } else if (jj_2_2134(3)) {
      jj_consume_token(ROUTINE_NAME);
    } else if (jj_2_2135(3)) {
      jj_consume_token(ROUTINE_SCHEMA);
    } else if (jj_2_2136(3)) {
      jj_consume_token(SCHEMA_NAME);
    } else if (jj_2_2137(3)) {
      jj_consume_token(SERVER_NAME);
    } else if (jj_2_2138(3)) {
      jj_consume_token(SPECIFIC_NAME);
    } else if (jj_2_2139(3)) {
      jj_consume_token(SUBCLASS_ORIGIN);
    } else if (jj_2_2140(3)) {
      jj_consume_token(TABLE_NAME);
    } else if (jj_2_2141(3)) {
      jj_consume_token(TRIGGER_CATALOG);
    } else if (jj_2_2142(3)) {
      jj_consume_token(TRIGGER_NAME);
    } else if (jj_2_2143(3)) {
      jj_consume_token(TRIGGER_SCHEMA);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::all_information() {
    all_info_target();
    jj_consume_token(EQUAL);
    jj_consume_token(ALL);
    if (jj_2_2144(3)) {
      all_qualifier();
    } else {
      ;
    }
}


void SqlParser::all_info_target() {
    simple_target_specification();
}


void SqlParser::all_qualifier() {
    if (jj_2_2146(3)) {
      jj_consume_token(STATEMENT);
    } else if (jj_2_2147(3)) {
      jj_consume_token(CONDITION);
      if (jj_2_2145(3)) {
        simple_value_specification();
      } else {
        ;
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::use_statement() {/*@bgen(jjtree) UseStatement */
  UseStatement *jjtn000 = new UseStatement(JJTUSESTATEMENT);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(USE);
      identifier_chain();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::lambda() {/*@bgen(jjtree) #Lambda( 2) */
  Lambda *jjtn000 = new Lambda(JJTLAMBDA);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      lambda_params();
      lambda_body();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000,  2);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::lambda_body() {/*@bgen(jjtree) LambdaBody */
  LambdaBody *jjtn000 = new LambdaBody(JJTLAMBDABODY);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(573);
      value_expression();
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::lambda_params() {/*@bgen(jjtree) LambdaParams */
  LambdaParams *jjtn000 = new LambdaParams(JJTLAMBDAPARAMS);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_2150(3)) {
LambdaParam *jjtn001 = new LambdaParam(JJTLAMBDAPARAM);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
        try {
          actual_identifier();
        } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
        }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001,  0);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
      } else if (jj_2_2151(3)) {
        jj_consume_token(lparen);
        if (jj_2_2149(3)) {
LambdaParam *jjtn002 = new LambdaParam(JJTLAMBDAPARAM);
            bool jjtc002 = true;
            jjtree.openNodeScope(jjtn002);
            jjtreeOpenNodeScope(jjtn002);
          try {
            actual_identifier();
          } catch ( ...) {
if (jjtc002) {
              jjtree.clearNodeScope(jjtn002);
              jjtc002 = false;
            } else {
              jjtree.popNode();
            }
          }
if (jjtc002) {
              jjtree.closeNodeScope(jjtn002,  0);
              if (jjtree.nodeCreated()) {
               jjtreeCloseNodeScope(jjtn002);
              }
            }
          while (!hasError) {
            if (jj_2_2148(3)) {
              ;
            } else {
              goto end_label_118;
            }
            jj_consume_token(570);
LambdaParam *jjtn003 = new LambdaParam(JJTLAMBDAPARAM);
                                                           bool jjtc003 = true;
                                                           jjtree.openNodeScope(jjtn003);
                                                           jjtreeOpenNodeScope(jjtn003);
            try {
              actual_identifier();
            } catch ( ...) {
if (jjtc003) {
                                                             jjtree.clearNodeScope(jjtn003);
                                                             jjtc003 = false;
                                                           } else {
                                                             jjtree.popNode();
                                                           }
            }
if (jjtc003) {
                                                             jjtree.closeNodeScope(jjtn003,  0);
                                                             if (jjtree.nodeCreated()) {
                                                              jjtreeCloseNodeScope(jjtn003);
                                                             }
                                                           }
          }
          end_label_118: ;
        } else {
          ;
        }
        jj_consume_token(rparen);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::if_not_exists() {
    jj_consume_token(IF);
    jj_consume_token(NOT);
    jj_consume_token(EXISTS);
}


void SqlParser::identifier_suffix_chain() {
    while (!hasError) {
      if (jj_2_2152(3)) {
        jj_consume_token(585);
      } else if (jj_2_2153(3)) {
        jj_consume_token(568);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
      if (jj_2_2154(3)) {
        actual_identifier();
      } else {
        ;
      }
      if (jj_2_2155(3)) {
        ;
      } else {
        goto end_label_119;
      }
    }
    end_label_119: ;
}


void SqlParser::limit_clause() {/*@bgen(jjtree) LimitClause */
  LimitClause *jjtn000 = new LimitClause(JJTLIMITCLAUSE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(LIMIT);
      if (jj_2_2156(3)) {
        jj_consume_token(unsigned_integer);
      } else if (jj_2_2157(3)) {
        jj_consume_token(ALL);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::presto_generic_type() {
    if (jj_2_2159(3)) {
      presto_array_type();
    } else if (jj_2_2160(3)) {
      presto_map_type();
    } else if (jj_2_2161(3)) {
ParameterizedType *jjtn001 = new ParameterizedType(JJTPARAMETERIZEDTYPE);
      bool jjtc001 = true;
      jjtree.openNodeScope(jjtn001);
      jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(regular_identifier);
        jj_consume_token(lparen);
        data_type();
        while (!hasError) {
          if (jj_2_2158(3)) {
            ;
          } else {
            goto end_label_120;
          }
          jj_consume_token(570);
          data_type();
        }
        end_label_120: ;
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc001) {
        jjtree.clearNodeScope(jjtn001);
        jjtc001 = false;
      } else {
        jjtree.popNode();
      }
      }
if (jjtc001) {
        jjtree.closeNodeScope(jjtn001, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn001);
        }
      }
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::presto_array_type() {/*@bgen(jjtree) #ArrayType(true) */
  ArrayType *jjtn000 = new ArrayType(JJTARRAYTYPE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_2162(3)) {
        jj_consume_token(ARRAY);
        jj_consume_token(LESS_THAN);
        data_type();
        jj_consume_token(GREATER_THAN);
      } else if (jj_2_2163(3)) {
        jj_consume_token(ARRAY);
        jj_consume_token(lparen);
        data_type();
        jj_consume_token(rparen);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::presto_map_type() {/*@bgen(jjtree) #MapType(true) */
  MapType *jjtn000 = new MapType(JJTMAPTYPE);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      if (jj_2_2164(3)) {
        jj_consume_token(MAP);
        jj_consume_token(LESS_THAN);
        data_type();
        jj_consume_token(570);
        data_type();
        jj_consume_token(GREATER_THAN);
      } else if (jj_2_2165(3)) {
        jj_consume_token(MAP);
        jj_consume_token(lparen);
        data_type();
        jj_consume_token(570);
        data_type();
        jj_consume_token(rparen);
      } else {
        jj_consume_token(-1);
        errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
      }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::percent_operator() {
    jj_consume_token(PERCENT);
}


void SqlParser::distinct() {
    jj_consume_token(DISTINCT);
}


void SqlParser::grouping_expression() {
    value_expression();
}


void SqlParser::count() {
    if (jj_2_2170(3)) {
      jj_consume_token(COUNT);
      jj_consume_token(lparen);
      jj_consume_token(rparen);
    } else if (jj_2_2171(3)) {
      jj_consume_token(COUNT_QUOTED);
      jj_consume_token(lparen);
      if (jj_2_2166(3)) {
        set_quantifier();
      } else {
        ;
      }
      if (jj_2_2169(3)) {
        if (jj_2_2167(3)) {
          value_expression();
        } else if (jj_2_2168(3)) {
          jj_consume_token(STAR);
        } else {
          jj_consume_token(-1);
          errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
        }
      } else {
        ;
      }
      jj_consume_token(rparen);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::table_description() {
    jj_consume_token(COMMENT);
    character_string_literal();
}


void SqlParser::routine_description() {
    jj_consume_token(COMMENT);
    character_string_literal();
}


void SqlParser::column_description() {
    jj_consume_token(COMMENT);
    character_string_literal();
}


void SqlParser::presto_aggregation_function() {
    if (jj_2_2172(3)) {
      jj_consume_token(338);
    } else if (jj_2_2173(3)) {
      jj_consume_token(340);
    } else if (jj_2_2174(3)) {
      jj_consume_token(341);
    } else if (jj_2_2175(3)) {
      jj_consume_token(342);
    } else if (jj_2_2176(3)) {
      jj_consume_token(343);
    } else if (jj_2_2177(3)) {
      jj_consume_token(344);
    } else {
      jj_consume_token(-1);
      errorHandler->handleParseError(token, getToken(1), __FUNCTION__, this), hasError = true;
    }
}


void SqlParser::presto_aggregations() {
    presto_aggregation_function();
    jj_consume_token(lparen);
    if (jj_2_2180(3)) {
      if (jj_2_2178(3)) {
        set_quantifier();
      } else {
        ;
      }
      value_expression();
      while (!hasError) {
        if (jj_2_2179(3)) {
          ;
        } else {
          goto end_label_121;
        }
        jj_consume_token(570);
        value_expression();
      }
      end_label_121: ;
    } else {
      ;
    }
    jj_consume_token(rparen);
}


void SqlParser::try_cast() {/*@bgen(jjtree) TryExpression */
  TryExpression *jjtn000 = new TryExpression(JJTTRYEXPRESSION);
  bool jjtc000 = true;
  jjtree.openNodeScope(jjtn000);
  jjtreeOpenNodeScope(jjtn000);
    try {
      jj_consume_token(TRY_CAST);
CastExpression *jjtn001 = new CastExpression(JJTCASTEXPRESSION);
                 bool jjtc001 = true;
                 jjtree.openNodeScope(jjtn001);
                 jjtreeOpenNodeScope(jjtn001);
      try {
        jj_consume_token(lparen);
        cast_operand();
        jj_consume_token(AS);
        cast_target();
        jj_consume_token(rparen);
      } catch ( ...) {
if (jjtc001) {
                   jjtree.clearNodeScope(jjtn001);
                   jjtc001 = false;
                 } else {
                   jjtree.popNode();
                 }
      }
if (jjtc001) {
                   jjtree.closeNodeScope(jjtn001, true);
                   if (jjtree.nodeCreated()) {
                    jjtreeCloseNodeScope(jjtn001);
                   }
                 }
    } catch ( ...) {
if (jjtc000) {
        jjtree.clearNodeScope(jjtn000);
        jjtc000 = false;
      } else {
        jjtree.popNode();
      }
    }
if (jjtc000) {
        jjtree.closeNodeScope(jjtn000, true);
        if (jjtree.nodeCreated()) {
         jjtreeCloseNodeScope(jjtn000);
        }
      }
}


void SqlParser::varbinary() {
    jj_consume_token(VARBINARY);
}


void SqlParser::table_attributes() {
    jj_consume_token(lparen);
    actual_identifier();
    jj_consume_token(EQUAL);
    value_expression();
    while (!hasError) {
      if (jj_2_2181(3)) {
        ;
      } else {
        goto end_label_122;
      }
      jj_consume_token(570);
      actual_identifier();
      jj_consume_token(EQUAL);
      value_expression();
    }
    end_label_122: ;
    jj_consume_token(rparen);
}


void SqlParser::or_replace() {
    jj_consume_token(OR);
    jj_consume_token(REPLACE);
}


void SqlParser::udaf_filter() {
    filter_clause();
}


void SqlParser::extra_args_to_agg() {
    while (!hasError) {
      jj_consume_token(570);
      value_expression();
      if (jj_2_2182(3)) {
        ;
      } else {
        goto end_label_123;
      }
    }
    end_label_123: ;
}


void SqlParser::weird_identifiers() {
    jj_consume_token(underscore);
}


  SqlParser::SqlParser(TokenManager *tokenManager){
    head = nullptr;
    ReInit(tokenManager);
}
SqlParser::~SqlParser()
{
  clear();
}

void SqlParser::ReInit(TokenManager* tokenManager){
    clear();
    errorHandler = new ErrorHandler();
    hasError = false;
    token_source = tokenManager;
    head = token = new Token();
    token->kind = 0;
    token->next = nullptr;
    jj_lookingAhead = false;
    jj_rescan = false;
    jj_done = false;
    jj_scanpos = jj_lastpos = nullptr;
    jj_gc = 0;
    jj_kind = -1;
    indent = 0;
    trace = false;
    jj_ntk = -1;
    jjtree.reset();
  }


void SqlParser::clear(){
  //Since token manager was generate from outside,
  //parser should not take care of deleting
  //if (token_source) delete token_source;
  if (head) {
    Token *next, *t = head;
    while (t) {
      next = t->next;
      delete t;
      t = next;
    }
  }
  if (errorHandler) {
    delete errorHandler, errorHandler = nullptr;
  }
}


Token * SqlParser::jj_consume_token(int kind)  {
    Token *oldToken;
    if ((oldToken = token)->next != nullptr) token = token->next;
    else token = token->next = token_source->getNextToken();
    jj_ntk = -1;
    if (token->kind == kind) {
      return token;
    }
    token = oldToken;
    JJString image = kind >= 0 ? tokenImage[kind] : tokenImage[0];
    errorHandler->handleUnexpectedToken(kind, image.substr(1, image.size() - 2), getToken(1), this);
    hasError = true;
    return token;
  }


bool  SqlParser::jj_scan_token(int kind){
    if (jj_scanpos == jj_lastpos) {
      jj_la--;
      if (jj_scanpos->next == nullptr) {
        jj_lastpos = jj_scanpos = jj_scanpos->next = token_source->getNextToken();
      } else {
        jj_lastpos = jj_scanpos = jj_scanpos->next;
      }
    } else {
      jj_scanpos = jj_scanpos->next;
    }
    if (jj_scanpos->kind != kind) return true;
    if (jj_la == 0 && jj_scanpos == jj_lastpos) { return jj_done = true; }
    return false;
  }


/** Get the next Token. */

Token * SqlParser::getNextToken(){
    if (token->next != nullptr) token = token->next;
    else token = token->next = token_source->getNextToken();
    jj_ntk = -1;
    return token;
  }

/** Get the specific Token. */

Token * SqlParser::getToken(int index){
    Token *t = jj_lookingAhead ? jj_scanpos : token;
    for (int i = 0; i < index; i++) {
      if (t->next != nullptr) t = t->next;
      else t = t->next = token_source->getNextToken();
    }
    return t;
  }


int SqlParser::jj_ntk_f(){
    if ((jj_nt=token->next) == nullptr)
      return (jj_ntk = (token->next=token_source->getNextToken())->kind);
    else
      return (jj_ntk = jj_nt->kind);
  }


 void  SqlParser::parseError()   {
   }


  bool SqlParser::trace_enabled()  {
    return trace;
  }


  void SqlParser::enable_tracing()  {
  }

  void SqlParser::disable_tracing()  {
  }


}
}
