#ifndef SQLPARSERTOKENMANAGER_H
#define SQLPARSERTOKENMANAGER_H
#include "stdio.h"
#include "JavaCC.h"
#include "CharStream.h"
#include "Token.h"
#include "ErrorHandler.h"
#include "TokenManager.h"
#include "SqlParserConstants.h"

namespace commonsql {
namespace parser {
class SqlParser;

/** Token Manager. */
class SqlParserTokenManager : public TokenManager {
public:
  void CommonTokenAction(Token* token);


  FILE *debugStream;
  void  setDebugStream(FILE *ds);
 int jjStopStringLiteralDfa_0(int pos, unsigned long long active0, unsigned long long active1, unsigned long long active2, unsigned long long active3, unsigned long long active4, unsigned long long active5, unsigned long long active6, unsigned long long active7, unsigned long long active8, unsigned long long active9);
int  jjStartNfa_0(int pos, unsigned long long active0, unsigned long long active1, unsigned long long active2, unsigned long long active3, unsigned long long active4, unsigned long long active5, unsigned long long active6, unsigned long long active7, unsigned long long active8, unsigned long long active9);
 int  jjStopAtPos(int pos, int kind);
 int  jjMoveStringLiteralDfa0_0();
 int  jjMoveStringLiteralDfa1_0(unsigned long long active0, unsigned long long active1, unsigned long long active2, unsigned long long active3, unsigned long long active4, unsigned long long active5, unsigned long long active6, unsigned long long active7, unsigned long long active8, unsigned long long active9);
 int  jjMoveStringLiteralDfa2_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8, unsigned long long old9, unsigned long long active9);
 int  jjMoveStringLiteralDfa3_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa4_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa5_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa6_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa7_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa8_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa9_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa10_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa11_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa12_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa13_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa14_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old5, unsigned long long active5, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa15_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old5, unsigned long long active5, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa16_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old5, unsigned long long active5, unsigned long long old7, unsigned long long active7);
 int  jjMoveStringLiteralDfa17_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old5, unsigned long long active5, unsigned long long old7, unsigned long long active7);
 int  jjMoveStringLiteralDfa18_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old5, unsigned long long active5);
 int  jjMoveStringLiteralDfa19_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3);
 int  jjMoveStringLiteralDfa20_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3);
 int  jjMoveStringLiteralDfa21_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3);
 int  jjMoveStringLiteralDfa22_0(unsigned long long old0, unsigned long long active0, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3);
 int  jjMoveStringLiteralDfa23_0(unsigned long long old0, unsigned long long active0, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3);
 int  jjMoveStringLiteralDfa24_0(unsigned long long old0, unsigned long long active0, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3);
 int  jjMoveStringLiteralDfa25_0(unsigned long long old0, unsigned long long active0, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3);
 int  jjMoveStringLiteralDfa26_0(unsigned long long old0, unsigned long long active0, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3);
 int  jjMoveStringLiteralDfa27_0(unsigned long long old0, unsigned long long active0, unsigned long long old3, unsigned long long active3);
 int  jjMoveStringLiteralDfa28_0(unsigned long long old3, unsigned long long active3);
 int  jjMoveStringLiteralDfa29_0(unsigned long long old3, unsigned long long active3);
 int  jjMoveStringLiteralDfa30_0(unsigned long long old3, unsigned long long active3);
 int  jjMoveStringLiteralDfa31_0(unsigned long long old3, unsigned long long active3);
int jjStartNfaWithStates_0(int pos, int kind, int state);
int jjMoveNfa_0(int startState, int curPos);
 int  jjMoveStringLiteralDfa0_3();
 int  jjMoveStringLiteralDfa0_1();
 int  jjMoveStringLiteralDfa1_1(unsigned long long active0, unsigned long long active5, unsigned long long active8);
 int  jjMoveStringLiteralDfa2_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa3_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa4_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa5_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa6_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa7_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa8_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa9_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa10_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa11_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa12_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa13_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa14_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa15_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa16_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa17_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8);
 int  jjMoveStringLiteralDfa18_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5);
 int  jjMoveStringLiteralDfa19_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5);
 int  jjMoveStringLiteralDfa20_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5);
 int  jjMoveStringLiteralDfa0_2();
 int  jjMoveStringLiteralDfa1_2(unsigned long long active9);
bool jjCanMove_0(int hiByte, int i1, int i2, unsigned long long l1, unsigned long long l2);
bool jjCanMove_1(int hiByte, int i1, int i2, unsigned long long l1, unsigned long long l2);
Token * jjFillToken();

public:
    int curLexState = 0;
    int jjnewStateCnt = 0;
    int jjround = 0;
    int jjmatchedPos = 0;
    int jjmatchedKind = 0;

Token * getNextToken();
void  SkipLexicalActions(Token *matchedToken);
void  TokenLexicalActions(Token *matchedToken);
#define jjCheckNAdd(state)\
{\
   if (jjrounds[state] != jjround)\
   {\
      jjstateSet[jjnewStateCnt++] = state;\
      jjrounds[state] = jjround;\
   }\
}
#define jjAddStates(start, end)\
{\
   for (int x = start; x <= end; x++) {\
      jjstateSet[jjnewStateCnt++] = jjnextStates[x];\
   } /*while (start++ != end);*/\
}
#define jjCheckNAddTwoStates(state1, state2)\
{\
   jjCheckNAdd(state1);\
   jjCheckNAdd(state2);\
}

#define jjCheckNAddStates(start, end)\
{\
   for (int x = start; x <= end; x++) {\
      jjCheckNAdd(jjnextStates[x]);\
   } /*while (start++ != end);*/\
}

#ifndef JAVACC_CHARSTREAM
#define JAVACC_CHARSTREAM CharStream
#endif

private:
  void ReInitRounds();

public:
  SqlParserTokenManager(JAVACC_CHARSTREAM *stream, int lexState = 0);
  virtual ~SqlParserTokenManager();
  void ReInit(JAVACC_CHARSTREAM *stream, int lexState = 0);
  void SwitchTo(int lexState);
  void clear();
  const JJSimpleString jjKindsForBitVector(int i, unsigned long long vec);
  const JJSimpleString jjKindsForStateVector(int lexState, int vec[], int start, int end);

  JAVACC_CHARSTREAM*        input_stream;
  int                       jjrounds[152];
  int                       jjstateSet[2 * 152];
  JJString                  jjimage;
  JJString                  image;
  int                       jjimageLen;
  int                       lengthOfMatch;
  JJChar                    curChar;
  TokenManagerErrorHandler* errorHandler = nullptr;

public: 
  void setErrorHandler(TokenManagerErrorHandler *eh) {
      if (errorHandler) delete errorHandler, errorHandler = nullptr;
      errorHandler = eh;
    }
    
};
}
}
#endif
