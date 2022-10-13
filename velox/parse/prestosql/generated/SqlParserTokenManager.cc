/* SqlParserTokenManager.cc */
#include "SqlParserTokenManager.h"
#include "TokenMgrError.h"
#include "SimpleNode.h"
namespace commonsql {
namespace parser {
static const unsigned long long jjbitVec0[] = {
   0x0ULL, 0x0ULL, 0x80000000000000ULL, 0x0ULL
};
static const unsigned long long jjbitVec1[] = {
   0xfffffffffffffffeULL, 0xffffffffffffffffULL, 0xffffffffffffffffULL, 0xffffffffffffffffULL
};
static const unsigned long long jjbitVec3[] = {
   0x0ULL, 0x0ULL, 0xffffffffffffffffULL, 0xffffffffffffffffULL
};
static const int jjnextStates[] = {
   56, 57, 58, 59, 60, 62, 66, 67, 63, 145, 146, 147, 20, 22, 23, 11, 
   13, 14, 26, 28, 29, 4, 5, 6, 30, 31, 32, 33, 35, 29, 38, 39, 
   43, 39, 42, 43, 44, 45, 46, 47, 48, 43, 48, 51, 43, 73, 74, 86, 
   73, 74, 75, 86, 87, 88, 95, 101, 102, 103, 132, 104, 105, 131, 104, 105, 
   106, 108, 109, 103, 110, 111, 112, 119, 133, 134, 141, 70, 98, 16, 17, 40, 
   41, 49, 50, 64, 65, 
};
static JJChar jjstrLiteralChars_0[] = {0};
static JJChar jjstrLiteralChars_1[] = {0};
static JJChar jjstrLiteralChars_2[] = {0};
static JJChar jjstrLiteralChars_3[] = {0};
static JJChar jjstrLiteralChars_4[] = {0};
static JJChar jjstrLiteralChars_5[] = {0};
static JJChar jjstrLiteralChars_6[] = {0};

static JJChar jjstrLiteralChars_7[] = {0};
static JJChar jjstrLiteralChars_8[] = {0};
static JJChar jjstrLiteralChars_9[] = {0};
static JJChar jjstrLiteralChars_10[] = {0};
static JJChar jjstrLiteralChars_11[] = {0};
static JJChar jjstrLiteralChars_12[] = {0};
static JJChar jjstrLiteralChars_13[] = {0};
static JJChar jjstrLiteralChars_14[] = {0};
static JJChar jjstrLiteralChars_15[] = {0};
static JJChar jjstrLiteralChars_16[] = {0};
static JJChar jjstrLiteralChars_17[] = {0};
static JJChar jjstrLiteralChars_18[] = {0};
static JJChar jjstrLiteralChars_19[] = {0};
static JJChar jjstrLiteralChars_20[] = {0};

static JJChar jjstrLiteralChars_21[] = {0};
static JJChar jjstrLiteralChars_22[] = {0};
static JJChar jjstrLiteralChars_23[] = {0};
static JJChar jjstrLiteralChars_24[] = {0};
static JJChar jjstrLiteralChars_25[] = {0};
static JJChar jjstrLiteralChars_26[] = {0};
static JJChar jjstrLiteralChars_27[] = {0};
static JJChar jjstrLiteralChars_28[] = {0};
static JJChar jjstrLiteralChars_29[] = {0};
static JJChar jjstrLiteralChars_30[] = {0};
static JJChar jjstrLiteralChars_31[] = {0};
static JJChar jjstrLiteralChars_32[] = {0};
static JJChar jjstrLiteralChars_33[] = {0};
static JJChar jjstrLiteralChars_34[] = {0};

static JJChar jjstrLiteralChars_35[] = {0};
static JJChar jjstrLiteralChars_36[] = {0};
static JJChar jjstrLiteralChars_37[] = {0};
static JJChar jjstrLiteralChars_38[] = {0};
static JJChar jjstrLiteralChars_39[] = {0};
static JJChar jjstrLiteralChars_40[] = {0};
static JJChar jjstrLiteralChars_41[] = {0};
static JJChar jjstrLiteralChars_42[] = {0};
static JJChar jjstrLiteralChars_43[] = {0};
static JJChar jjstrLiteralChars_44[] = {0};
static JJChar jjstrLiteralChars_45[] = {0};
static JJChar jjstrLiteralChars_46[] = {0};
static JJChar jjstrLiteralChars_47[] = {0};
static JJChar jjstrLiteralChars_48[] = {0};

static JJChar jjstrLiteralChars_49[] = {0};
static JJChar jjstrLiteralChars_50[] = {0};
static JJChar jjstrLiteralChars_51[] = {0};
static JJChar jjstrLiteralChars_52[] = {0};
static JJChar jjstrLiteralChars_53[] = {0};
static JJChar jjstrLiteralChars_54[] = {0};
static JJChar jjstrLiteralChars_55[] = {0};
static JJChar jjstrLiteralChars_56[] = {0};
static JJChar jjstrLiteralChars_57[] = {0};
static JJChar jjstrLiteralChars_58[] = {0};
static JJChar jjstrLiteralChars_59[] = {0};
static JJChar jjstrLiteralChars_60[] = {0};
static JJChar jjstrLiteralChars_61[] = {0};
static JJChar jjstrLiteralChars_62[] = {0};

static JJChar jjstrLiteralChars_63[] = {0};
static JJChar jjstrLiteralChars_64[] = {0};
static JJChar jjstrLiteralChars_65[] = {0};
static JJChar jjstrLiteralChars_66[] = {0};
static JJChar jjstrLiteralChars_67[] = {0};
static JJChar jjstrLiteralChars_68[] = {0};
static JJChar jjstrLiteralChars_69[] = {0};
static JJChar jjstrLiteralChars_70[] = {0};
static JJChar jjstrLiteralChars_71[] = {0};
static JJChar jjstrLiteralChars_72[] = {0};
static JJChar jjstrLiteralChars_73[] = {0};
static JJChar jjstrLiteralChars_74[] = {0};
static JJChar jjstrLiteralChars_75[] = {0};
static JJChar jjstrLiteralChars_76[] = {0};

static JJChar jjstrLiteralChars_77[] = {0};
static JJChar jjstrLiteralChars_78[] = {0};
static JJChar jjstrLiteralChars_79[] = {0};
static JJChar jjstrLiteralChars_80[] = {0};
static JJChar jjstrLiteralChars_81[] = {0};
static JJChar jjstrLiteralChars_82[] = {0};
static JJChar jjstrLiteralChars_83[] = {0};
static JJChar jjstrLiteralChars_84[] = {0};
static JJChar jjstrLiteralChars_85[] = {0};
static JJChar jjstrLiteralChars_86[] = {0};
static JJChar jjstrLiteralChars_87[] = {0};
static JJChar jjstrLiteralChars_88[] = {0};
static JJChar jjstrLiteralChars_89[] = {0};
static JJChar jjstrLiteralChars_90[] = {0};

static JJChar jjstrLiteralChars_91[] = {0};
static JJChar jjstrLiteralChars_92[] = {0};
static JJChar jjstrLiteralChars_93[] = {0};
static JJChar jjstrLiteralChars_94[] = {0};
static JJChar jjstrLiteralChars_95[] = {0};
static JJChar jjstrLiteralChars_96[] = {0};
static JJChar jjstrLiteralChars_97[] = {0};
static JJChar jjstrLiteralChars_98[] = {0};
static JJChar jjstrLiteralChars_99[] = {0};
static JJChar jjstrLiteralChars_100[] = {0};
static JJChar jjstrLiteralChars_101[] = {0};
static JJChar jjstrLiteralChars_102[] = {0};
static JJChar jjstrLiteralChars_103[] = {0};
static JJChar jjstrLiteralChars_104[] = {0};

static JJChar jjstrLiteralChars_105[] = {0};
static JJChar jjstrLiteralChars_106[] = {0};
static JJChar jjstrLiteralChars_107[] = {0};
static JJChar jjstrLiteralChars_108[] = {0};
static JJChar jjstrLiteralChars_109[] = {0};
static JJChar jjstrLiteralChars_110[] = {0};
static JJChar jjstrLiteralChars_111[] = {0};
static JJChar jjstrLiteralChars_112[] = {0};
static JJChar jjstrLiteralChars_113[] = {0};
static JJChar jjstrLiteralChars_114[] = {0};
static JJChar jjstrLiteralChars_115[] = {0};
static JJChar jjstrLiteralChars_116[] = {0};
static JJChar jjstrLiteralChars_117[] = {0};
static JJChar jjstrLiteralChars_118[] = {0};

static JJChar jjstrLiteralChars_119[] = {0};
static JJChar jjstrLiteralChars_120[] = {0};
static JJChar jjstrLiteralChars_121[] = {0};
static JJChar jjstrLiteralChars_122[] = {0};
static JJChar jjstrLiteralChars_123[] = {0};
static JJChar jjstrLiteralChars_124[] = {0};
static JJChar jjstrLiteralChars_125[] = {0};
static JJChar jjstrLiteralChars_126[] = {0};
static JJChar jjstrLiteralChars_127[] = {0};
static JJChar jjstrLiteralChars_128[] = {0};
static JJChar jjstrLiteralChars_129[] = {0};
static JJChar jjstrLiteralChars_130[] = {0};
static JJChar jjstrLiteralChars_131[] = {0};
static JJChar jjstrLiteralChars_132[] = {0};

static JJChar jjstrLiteralChars_133[] = {0};
static JJChar jjstrLiteralChars_134[] = {0};
static JJChar jjstrLiteralChars_135[] = {0};
static JJChar jjstrLiteralChars_136[] = {0};
static JJChar jjstrLiteralChars_137[] = {0};
static JJChar jjstrLiteralChars_138[] = {0};
static JJChar jjstrLiteralChars_139[] = {0};
static JJChar jjstrLiteralChars_140[] = {0};
static JJChar jjstrLiteralChars_141[] = {0};
static JJChar jjstrLiteralChars_142[] = {0};
static JJChar jjstrLiteralChars_143[] = {0};
static JJChar jjstrLiteralChars_144[] = {0};
static JJChar jjstrLiteralChars_145[] = {0};
static JJChar jjstrLiteralChars_146[] = {0};

static JJChar jjstrLiteralChars_147[] = {0};
static JJChar jjstrLiteralChars_148[] = {0};
static JJChar jjstrLiteralChars_149[] = {0};
static JJChar jjstrLiteralChars_150[] = {0};
static JJChar jjstrLiteralChars_151[] = {0};
static JJChar jjstrLiteralChars_152[] = {0};
static JJChar jjstrLiteralChars_153[] = {0};
static JJChar jjstrLiteralChars_154[] = {0};
static JJChar jjstrLiteralChars_155[] = {0};
static JJChar jjstrLiteralChars_156[] = {0};
static JJChar jjstrLiteralChars_157[] = {0};
static JJChar jjstrLiteralChars_158[] = {0};
static JJChar jjstrLiteralChars_159[] = {0};
static JJChar jjstrLiteralChars_160[] = {0};

static JJChar jjstrLiteralChars_161[] = {0};
static JJChar jjstrLiteralChars_162[] = {0};
static JJChar jjstrLiteralChars_163[] = {0};
static JJChar jjstrLiteralChars_164[] = {0};
static JJChar jjstrLiteralChars_165[] = {0};
static JJChar jjstrLiteralChars_166[] = {0};
static JJChar jjstrLiteralChars_167[] = {0};
static JJChar jjstrLiteralChars_168[] = {0};
static JJChar jjstrLiteralChars_169[] = {0};
static JJChar jjstrLiteralChars_170[] = {0};
static JJChar jjstrLiteralChars_171[] = {0};
static JJChar jjstrLiteralChars_172[] = {0};
static JJChar jjstrLiteralChars_173[] = {0};
static JJChar jjstrLiteralChars_174[] = {0};

static JJChar jjstrLiteralChars_175[] = {0};
static JJChar jjstrLiteralChars_176[] = {0};
static JJChar jjstrLiteralChars_177[] = {0};
static JJChar jjstrLiteralChars_178[] = {0};
static JJChar jjstrLiteralChars_179[] = {0};
static JJChar jjstrLiteralChars_180[] = {0};
static JJChar jjstrLiteralChars_181[] = {0};
static JJChar jjstrLiteralChars_182[] = {0};
static JJChar jjstrLiteralChars_183[] = {0};
static JJChar jjstrLiteralChars_184[] = {0};
static JJChar jjstrLiteralChars_185[] = {0};
static JJChar jjstrLiteralChars_186[] = {0};
static JJChar jjstrLiteralChars_187[] = {0};
static JJChar jjstrLiteralChars_188[] = {0};

static JJChar jjstrLiteralChars_189[] = {0};
static JJChar jjstrLiteralChars_190[] = {0};
static JJChar jjstrLiteralChars_191[] = {0};
static JJChar jjstrLiteralChars_192[] = {0};
static JJChar jjstrLiteralChars_193[] = {0};
static JJChar jjstrLiteralChars_194[] = {0};
static JJChar jjstrLiteralChars_195[] = {0};
static JJChar jjstrLiteralChars_196[] = {0};
static JJChar jjstrLiteralChars_197[] = {0};
static JJChar jjstrLiteralChars_198[] = {0};
static JJChar jjstrLiteralChars_199[] = {0};
static JJChar jjstrLiteralChars_200[] = {0};
static JJChar jjstrLiteralChars_201[] = {0};
static JJChar jjstrLiteralChars_202[] = {0};

static JJChar jjstrLiteralChars_203[] = {0};
static JJChar jjstrLiteralChars_204[] = {0};
static JJChar jjstrLiteralChars_205[] = {0};
static JJChar jjstrLiteralChars_206[] = {0};
static JJChar jjstrLiteralChars_207[] = {0};
static JJChar jjstrLiteralChars_208[] = {0};
static JJChar jjstrLiteralChars_209[] = {0};
static JJChar jjstrLiteralChars_210[] = {0};
static JJChar jjstrLiteralChars_211[] = {0};
static JJChar jjstrLiteralChars_212[] = {0};
static JJChar jjstrLiteralChars_213[] = {0};
static JJChar jjstrLiteralChars_214[] = {0};
static JJChar jjstrLiteralChars_215[] = {0};
static JJChar jjstrLiteralChars_216[] = {0};

static JJChar jjstrLiteralChars_217[] = {0};
static JJChar jjstrLiteralChars_218[] = {0};
static JJChar jjstrLiteralChars_219[] = {0};
static JJChar jjstrLiteralChars_220[] = {0};
static JJChar jjstrLiteralChars_221[] = {0};
static JJChar jjstrLiteralChars_222[] = {0};
static JJChar jjstrLiteralChars_223[] = {0};
static JJChar jjstrLiteralChars_224[] = {0};
static JJChar jjstrLiteralChars_225[] = {0};
static JJChar jjstrLiteralChars_226[] = {0};
static JJChar jjstrLiteralChars_227[] = {0};
static JJChar jjstrLiteralChars_228[] = {0};
static JJChar jjstrLiteralChars_229[] = {0};
static JJChar jjstrLiteralChars_230[] = {0};

static JJChar jjstrLiteralChars_231[] = {0};
static JJChar jjstrLiteralChars_232[] = {0};
static JJChar jjstrLiteralChars_233[] = {0};
static JJChar jjstrLiteralChars_234[] = {0};
static JJChar jjstrLiteralChars_235[] = {0};
static JJChar jjstrLiteralChars_236[] = {0};
static JJChar jjstrLiteralChars_237[] = {0};
static JJChar jjstrLiteralChars_238[] = {0};
static JJChar jjstrLiteralChars_239[] = {0};
static JJChar jjstrLiteralChars_240[] = {0};
static JJChar jjstrLiteralChars_241[] = {0};
static JJChar jjstrLiteralChars_242[] = {0};
static JJChar jjstrLiteralChars_243[] = {0};
static JJChar jjstrLiteralChars_244[] = {0};

static JJChar jjstrLiteralChars_245[] = {0};
static JJChar jjstrLiteralChars_246[] = {0};
static JJChar jjstrLiteralChars_247[] = {0};
static JJChar jjstrLiteralChars_248[] = {0};
static JJChar jjstrLiteralChars_249[] = {0};
static JJChar jjstrLiteralChars_250[] = {0};
static JJChar jjstrLiteralChars_251[] = {0};
static JJChar jjstrLiteralChars_252[] = {0};
static JJChar jjstrLiteralChars_253[] = {0};
static JJChar jjstrLiteralChars_254[] = {0};
static JJChar jjstrLiteralChars_255[] = {0};
static JJChar jjstrLiteralChars_256[] = {0};
static JJChar jjstrLiteralChars_257[] = {0};
static JJChar jjstrLiteralChars_258[] = {0};

static JJChar jjstrLiteralChars_259[] = {0};
static JJChar jjstrLiteralChars_260[] = {0};
static JJChar jjstrLiteralChars_261[] = {0};
static JJChar jjstrLiteralChars_262[] = {0};
static JJChar jjstrLiteralChars_263[] = {0};
static JJChar jjstrLiteralChars_264[] = {0};
static JJChar jjstrLiteralChars_265[] = {0};
static JJChar jjstrLiteralChars_266[] = {0};
static JJChar jjstrLiteralChars_267[] = {0};
static JJChar jjstrLiteralChars_268[] = {0};
static JJChar jjstrLiteralChars_269[] = {0};
static JJChar jjstrLiteralChars_270[] = {0};
static JJChar jjstrLiteralChars_271[] = {0};
static JJChar jjstrLiteralChars_272[] = {0};

static JJChar jjstrLiteralChars_273[] = {0};
static JJChar jjstrLiteralChars_274[] = {0};
static JJChar jjstrLiteralChars_275[] = {0};
static JJChar jjstrLiteralChars_276[] = {0};
static JJChar jjstrLiteralChars_277[] = {0};
static JJChar jjstrLiteralChars_278[] = {0};
static JJChar jjstrLiteralChars_279[] = {0};
static JJChar jjstrLiteralChars_280[] = {0};
static JJChar jjstrLiteralChars_281[] = {0};
static JJChar jjstrLiteralChars_282[] = {0};
static JJChar jjstrLiteralChars_283[] = {0};
static JJChar jjstrLiteralChars_284[] = {0};
static JJChar jjstrLiteralChars_285[] = {0};
static JJChar jjstrLiteralChars_286[] = {0};

static JJChar jjstrLiteralChars_287[] = {0};
static JJChar jjstrLiteralChars_288[] = {0};
static JJChar jjstrLiteralChars_289[] = {0};
static JJChar jjstrLiteralChars_290[] = {0};
static JJChar jjstrLiteralChars_291[] = {0};
static JJChar jjstrLiteralChars_292[] = {0};
static JJChar jjstrLiteralChars_293[] = {0};
static JJChar jjstrLiteralChars_294[] = {0};
static JJChar jjstrLiteralChars_295[] = {0};
static JJChar jjstrLiteralChars_296[] = {0};
static JJChar jjstrLiteralChars_297[] = {0};
static JJChar jjstrLiteralChars_298[] = {0};
static JJChar jjstrLiteralChars_299[] = {0};
static JJChar jjstrLiteralChars_300[] = {0};

static JJChar jjstrLiteralChars_301[] = {0};
static JJChar jjstrLiteralChars_302[] = {0};
static JJChar jjstrLiteralChars_303[] = {0};
static JJChar jjstrLiteralChars_304[] = {0};
static JJChar jjstrLiteralChars_305[] = {0};
static JJChar jjstrLiteralChars_306[] = {0};
static JJChar jjstrLiteralChars_307[] = {0};
static JJChar jjstrLiteralChars_308[] = {0};
static JJChar jjstrLiteralChars_309[] = {0};
static JJChar jjstrLiteralChars_310[] = {0};
static JJChar jjstrLiteralChars_311[] = {0};
static JJChar jjstrLiteralChars_312[] = {0};
static JJChar jjstrLiteralChars_313[] = {0};
static JJChar jjstrLiteralChars_314[] = {0};

static JJChar jjstrLiteralChars_315[] = {0};
static JJChar jjstrLiteralChars_316[] = {0};
static JJChar jjstrLiteralChars_317[] = {0};
static JJChar jjstrLiteralChars_318[] = {0};
static JJChar jjstrLiteralChars_319[] = {0};
static JJChar jjstrLiteralChars_320[] = {0};
static JJChar jjstrLiteralChars_321[] = {0};
static JJChar jjstrLiteralChars_322[] = {0};
static JJChar jjstrLiteralChars_323[] = {0};
static JJChar jjstrLiteralChars_324[] = {0};
static JJChar jjstrLiteralChars_325[] = {0};
static JJChar jjstrLiteralChars_326[] = {0};
static JJChar jjstrLiteralChars_327[] = {0};
static JJChar jjstrLiteralChars_328[] = {0};

static JJChar jjstrLiteralChars_329[] = {0};
static JJChar jjstrLiteralChars_330[] = {0};
static JJChar jjstrLiteralChars_331[] = {0};
static JJChar jjstrLiteralChars_332[] = {0};
static JJChar jjstrLiteralChars_333[] = {0};
static JJChar jjstrLiteralChars_334[] = {0};
static JJChar jjstrLiteralChars_335[] = {0};
static JJChar jjstrLiteralChars_336[] = {0};
static JJChar jjstrLiteralChars_337[] = {0};
static JJChar jjstrLiteralChars_338[] = {0};
static JJChar jjstrLiteralChars_339[] = {0};
static JJChar jjstrLiteralChars_340[] = {0};
static JJChar jjstrLiteralChars_341[] = {0};
static JJChar jjstrLiteralChars_342[] = {0};

static JJChar jjstrLiteralChars_343[] = {0};
static JJChar jjstrLiteralChars_344[] = {0};
static JJChar jjstrLiteralChars_345[] = {0};
static JJChar jjstrLiteralChars_346[] = {0};
static JJChar jjstrLiteralChars_347[] = {0};
static JJChar jjstrLiteralChars_348[] = {0};
static JJChar jjstrLiteralChars_349[] = {0};
static JJChar jjstrLiteralChars_350[] = {0};
static JJChar jjstrLiteralChars_351[] = {0};
static JJChar jjstrLiteralChars_352[] = {0};
static JJChar jjstrLiteralChars_353[] = {0};
static JJChar jjstrLiteralChars_354[] = {0};
static JJChar jjstrLiteralChars_355[] = {0};
static JJChar jjstrLiteralChars_356[] = {0};

static JJChar jjstrLiteralChars_357[] = {0};
static JJChar jjstrLiteralChars_358[] = {0};
static JJChar jjstrLiteralChars_359[] = {0};
static JJChar jjstrLiteralChars_360[] = {0};
static JJChar jjstrLiteralChars_361[] = {0};
static JJChar jjstrLiteralChars_362[] = {0};
static JJChar jjstrLiteralChars_363[] = {0};
static JJChar jjstrLiteralChars_364[] = {0};
static JJChar jjstrLiteralChars_365[] = {0};
static JJChar jjstrLiteralChars_366[] = {0};
static JJChar jjstrLiteralChars_367[] = {0};
static JJChar jjstrLiteralChars_368[] = {0};
static JJChar jjstrLiteralChars_369[] = {0};
static JJChar jjstrLiteralChars_370[] = {0};

static JJChar jjstrLiteralChars_371[] = {0};
static JJChar jjstrLiteralChars_372[] = {0};
static JJChar jjstrLiteralChars_373[] = {0};
static JJChar jjstrLiteralChars_374[] = {0};
static JJChar jjstrLiteralChars_375[] = {0};
static JJChar jjstrLiteralChars_376[] = {0};
static JJChar jjstrLiteralChars_377[] = {0};
static JJChar jjstrLiteralChars_378[] = {0};
static JJChar jjstrLiteralChars_379[] = {0};
static JJChar jjstrLiteralChars_380[] = {0};
static JJChar jjstrLiteralChars_381[] = {0};
static JJChar jjstrLiteralChars_382[] = {0};
static JJChar jjstrLiteralChars_383[] = {0};
static JJChar jjstrLiteralChars_384[] = {0};

static JJChar jjstrLiteralChars_385[] = {0};
static JJChar jjstrLiteralChars_386[] = {0};
static JJChar jjstrLiteralChars_387[] = {0};
static JJChar jjstrLiteralChars_388[] = {0};
static JJChar jjstrLiteralChars_389[] = {0};
static JJChar jjstrLiteralChars_390[] = {0};
static JJChar jjstrLiteralChars_391[] = {0};
static JJChar jjstrLiteralChars_392[] = {0};
static JJChar jjstrLiteralChars_393[] = {0};
static JJChar jjstrLiteralChars_394[] = {0};
static JJChar jjstrLiteralChars_395[] = {0};
static JJChar jjstrLiteralChars_396[] = {0};
static JJChar jjstrLiteralChars_397[] = {0};
static JJChar jjstrLiteralChars_398[] = {0};

static JJChar jjstrLiteralChars_399[] = {0};
static JJChar jjstrLiteralChars_400[] = {0};
static JJChar jjstrLiteralChars_401[] = {0};
static JJChar jjstrLiteralChars_402[] = {0};
static JJChar jjstrLiteralChars_403[] = {0};
static JJChar jjstrLiteralChars_404[] = {0};
static JJChar jjstrLiteralChars_405[] = {0};
static JJChar jjstrLiteralChars_406[] = {0};
static JJChar jjstrLiteralChars_407[] = {0};
static JJChar jjstrLiteralChars_408[] = {0};
static JJChar jjstrLiteralChars_409[] = {0};
static JJChar jjstrLiteralChars_410[] = {0};
static JJChar jjstrLiteralChars_411[] = {0};
static JJChar jjstrLiteralChars_412[] = {0};

static JJChar jjstrLiteralChars_413[] = {0};
static JJChar jjstrLiteralChars_414[] = {0};
static JJChar jjstrLiteralChars_415[] = {0};
static JJChar jjstrLiteralChars_416[] = {0};
static JJChar jjstrLiteralChars_417[] = {0};
static JJChar jjstrLiteralChars_418[] = {0};
static JJChar jjstrLiteralChars_419[] = {0};
static JJChar jjstrLiteralChars_420[] = {0};
static JJChar jjstrLiteralChars_421[] = {0};
static JJChar jjstrLiteralChars_422[] = {0};
static JJChar jjstrLiteralChars_423[] = {0};
static JJChar jjstrLiteralChars_424[] = {0};
static JJChar jjstrLiteralChars_425[] = {0};
static JJChar jjstrLiteralChars_426[] = {0};

static JJChar jjstrLiteralChars_427[] = {0};
static JJChar jjstrLiteralChars_428[] = {0};
static JJChar jjstrLiteralChars_429[] = {0};
static JJChar jjstrLiteralChars_430[] = {0};
static JJChar jjstrLiteralChars_431[] = {0};
static JJChar jjstrLiteralChars_432[] = {0};
static JJChar jjstrLiteralChars_433[] = {0};
static JJChar jjstrLiteralChars_434[] = {0};
static JJChar jjstrLiteralChars_435[] = {0};
static JJChar jjstrLiteralChars_436[] = {0};
static JJChar jjstrLiteralChars_437[] = {0};
static JJChar jjstrLiteralChars_438[] = {0};
static JJChar jjstrLiteralChars_439[] = {0};
static JJChar jjstrLiteralChars_440[] = {0};

static JJChar jjstrLiteralChars_441[] = {0};
static JJChar jjstrLiteralChars_442[] = {0};
static JJChar jjstrLiteralChars_443[] = {0};
static JJChar jjstrLiteralChars_444[] = {0};
static JJChar jjstrLiteralChars_445[] = {0};
static JJChar jjstrLiteralChars_446[] = {0};
static JJChar jjstrLiteralChars_447[] = {0};
static JJChar jjstrLiteralChars_448[] = {0};
static JJChar jjstrLiteralChars_449[] = {0};
static JJChar jjstrLiteralChars_450[] = {0};
static JJChar jjstrLiteralChars_451[] = {0};
static JJChar jjstrLiteralChars_452[] = {0};
static JJChar jjstrLiteralChars_453[] = {0};
static JJChar jjstrLiteralChars_454[] = {0};

static JJChar jjstrLiteralChars_455[] = {0};
static JJChar jjstrLiteralChars_456[] = {0};
static JJChar jjstrLiteralChars_457[] = {0};
static JJChar jjstrLiteralChars_458[] = {0};
static JJChar jjstrLiteralChars_459[] = {0};
static JJChar jjstrLiteralChars_460[] = {0};
static JJChar jjstrLiteralChars_461[] = {0};
static JJChar jjstrLiteralChars_462[] = {0};
static JJChar jjstrLiteralChars_463[] = {0};
static JJChar jjstrLiteralChars_464[] = {0};
static JJChar jjstrLiteralChars_465[] = {0};
static JJChar jjstrLiteralChars_466[] = {0};
static JJChar jjstrLiteralChars_467[] = {0};
static JJChar jjstrLiteralChars_468[] = {0};

static JJChar jjstrLiteralChars_469[] = {0};
static JJChar jjstrLiteralChars_470[] = {0};
static JJChar jjstrLiteralChars_471[] = {0};
static JJChar jjstrLiteralChars_472[] = {0};
static JJChar jjstrLiteralChars_473[] = {0};
static JJChar jjstrLiteralChars_474[] = {0};
static JJChar jjstrLiteralChars_475[] = {0};
static JJChar jjstrLiteralChars_476[] = {0};
static JJChar jjstrLiteralChars_477[] = {0};
static JJChar jjstrLiteralChars_478[] = {0};
static JJChar jjstrLiteralChars_479[] = {0};
static JJChar jjstrLiteralChars_480[] = {0};
static JJChar jjstrLiteralChars_481[] = {0};
static JJChar jjstrLiteralChars_482[] = {0};

static JJChar jjstrLiteralChars_483[] = {0};
static JJChar jjstrLiteralChars_484[] = {0};
static JJChar jjstrLiteralChars_485[] = {0};
static JJChar jjstrLiteralChars_486[] = {0};
static JJChar jjstrLiteralChars_487[] = {0};
static JJChar jjstrLiteralChars_488[] = {0};
static JJChar jjstrLiteralChars_489[] = {0};
static JJChar jjstrLiteralChars_490[] = {0};
static JJChar jjstrLiteralChars_491[] = {0};
static JJChar jjstrLiteralChars_492[] = {0};
static JJChar jjstrLiteralChars_493[] = {0};
static JJChar jjstrLiteralChars_494[] = {0};
static JJChar jjstrLiteralChars_495[] = {0};
static JJChar jjstrLiteralChars_496[] = {0};

static JJChar jjstrLiteralChars_497[] = {0};
static JJChar jjstrLiteralChars_498[] = {0};
static JJChar jjstrLiteralChars_499[] = {0};
static JJChar jjstrLiteralChars_500[] = {0};
static JJChar jjstrLiteralChars_501[] = {0};
static JJChar jjstrLiteralChars_502[] = {0};
static JJChar jjstrLiteralChars_503[] = {0};
static JJChar jjstrLiteralChars_504[] = {0};
static JJChar jjstrLiteralChars_505[] = {0};
static JJChar jjstrLiteralChars_506[] = {0};
static JJChar jjstrLiteralChars_507[] = {0};
static JJChar jjstrLiteralChars_508[] = {0};
static JJChar jjstrLiteralChars_509[] = {0};
static JJChar jjstrLiteralChars_510[] = {0};

static JJChar jjstrLiteralChars_511[] = {0};
static JJChar jjstrLiteralChars_512[] = {0};
static JJChar jjstrLiteralChars_513[] = {0};
static JJChar jjstrLiteralChars_514[] = {0};
static JJChar jjstrLiteralChars_515[] = {0};
static JJChar jjstrLiteralChars_516[] = {0};
static JJChar jjstrLiteralChars_517[] = {0};
static JJChar jjstrLiteralChars_518[] = {0};
static JJChar jjstrLiteralChars_519[] = {0};
static JJChar jjstrLiteralChars_520[] = {0};
static JJChar jjstrLiteralChars_521[] = {0};
static JJChar jjstrLiteralChars_522[] = {0};
static JJChar jjstrLiteralChars_523[] = {0};
static JJChar jjstrLiteralChars_524[] = {0};

static JJChar jjstrLiteralChars_525[] = {0};
static JJChar jjstrLiteralChars_526[] = {0};
static JJChar jjstrLiteralChars_527[] = {0};
static JJChar jjstrLiteralChars_528[] = {0};
static JJChar jjstrLiteralChars_529[] = {0};
static JJChar jjstrLiteralChars_530[] = {0};
static JJChar jjstrLiteralChars_531[] = {0};
static JJChar jjstrLiteralChars_532[] = {0};
static JJChar jjstrLiteralChars_533[] = {0};
static JJChar jjstrLiteralChars_534[] = {0};
static JJChar jjstrLiteralChars_535[] = {0};
static JJChar jjstrLiteralChars_536[] = {0};
static JJChar jjstrLiteralChars_537[] = {0};
static JJChar jjstrLiteralChars_538[] = {0};

static JJChar jjstrLiteralChars_539[] = {0};
static JJChar jjstrLiteralChars_540[] = {0};
static JJChar jjstrLiteralChars_541[] = {0};
static JJChar jjstrLiteralChars_542[] = {0};
static JJChar jjstrLiteralChars_543[] = {0};
static JJChar jjstrLiteralChars_544[] = {0};
static JJChar jjstrLiteralChars_545[] = {0};
static JJChar jjstrLiteralChars_546[] = {0};
static JJChar jjstrLiteralChars_547[] = {0};
static JJChar jjstrLiteralChars_548[] = {0};
static JJChar jjstrLiteralChars_549[] = {0};
static JJChar jjstrLiteralChars_550[] = {0};
static JJChar jjstrLiteralChars_551[] = {0};
static JJChar jjstrLiteralChars_552[] = {0};

static JJChar jjstrLiteralChars_553[] = {0};
static JJChar jjstrLiteralChars_554[] = {0};
static JJChar jjstrLiteralChars_555[] = {0};
static JJChar jjstrLiteralChars_556[] = {0};
static JJChar jjstrLiteralChars_557[] = {0};
static JJChar jjstrLiteralChars_558[] = {0x3b, 0};

static JJChar jjstrLiteralChars_559[] = {0x28, 0};
static JJChar jjstrLiteralChars_560[] = {0x29, 0};

static JJChar jjstrLiteralChars_561[] = {0x5f, 0};
static JJChar jjstrLiteralChars_562[] = {0x5b, 0};

static JJChar jjstrLiteralChars_563[] = {0x3f, 0x3f, 0x28, 0};
static JJChar jjstrLiteralChars_564[] = {0x5d, 0};

static JJChar jjstrLiteralChars_565[] = {0x3f, 0x3f, 0x29, 0};
static JJChar jjstrLiteralChars_566[] = {0x2b, 0};

static JJChar jjstrLiteralChars_567[] = {0x2d, 0};
static JJChar jjstrLiteralChars_568[] = {0x3a, 0};

static JJChar jjstrLiteralChars_569[] = {0x2e, 0};
static JJChar jjstrLiteralChars_570[] = {0x2c, 0};

static JJChar jjstrLiteralChars_571[] = {0x3f, 0};
static JJChar jjstrLiteralChars_572[] = {0x3a, 0x3a, 0};

static JJChar jjstrLiteralChars_573[] = {0x2d, 0x3e, 0};
static JJChar jjstrLiteralChars_574[] = {0x2a, 0};

static JJChar jjstrLiteralChars_575[] = {0x2f, 0};
static JJChar jjstrLiteralChars_576[] = {0x7c, 0x7c, 0};

static JJChar jjstrLiteralChars_577[] = {0x3d, 0};
static JJChar jjstrLiteralChars_578[] = {0x3c, 0x3e, 0};

static JJChar jjstrLiteralChars_579[] = {0x3c, 0};
static JJChar jjstrLiteralChars_580[] = {0x3e, 0};

static JJChar jjstrLiteralChars_581[] = {0x3c, 0x3d, 0};
static JJChar jjstrLiteralChars_582[] = {0x3e, 0x3d, 0};

static JJChar jjstrLiteralChars_583[] = {0x21, 0x3d, 0};
static JJChar jjstrLiteralChars_584[] = {0x3d, 0x3e, 0};

static JJChar jjstrLiteralChars_585[] = {0x40, 0};
static JJChar jjstrLiteralChars_586[] = {0x25, 0};
static JJChar jjstrLiteralChars_587[] = {0};
static JJChar jjstrLiteralChars_588[] = {0};
static JJChar jjstrLiteralChars_589[] = {0};
static JJChar jjstrLiteralChars_590[] = {0};
static JJChar jjstrLiteralChars_591[] = {0};

static JJChar jjstrLiteralChars_592[] = {0};
static JJChar jjstrLiteralChars_593[] = {0};
static JJChar jjstrLiteralChars_594[] = {0};
static JJChar jjstrLiteralChars_595[] = {0};
static JJChar jjstrLiteralChars_596[] = {0};
static JJChar jjstrLiteralChars_597[] = {0};
static JJChar jjstrLiteralChars_598[] = {0};
static JJChar jjstrLiteralChars_599[] = {0};
static JJChar jjstrLiteralChars_600[] = {0};
static JJChar jjstrLiteralChars_601[] = {0};
static JJChar jjstrLiteralChars_602[] = {0};
static JJChar jjstrLiteralChars_603[] = {0};
static JJChar jjstrLiteralChars_604[] = {0};
static JJChar jjstrLiteralChars_605[] = {0};

static JJChar jjstrLiteralChars_606[] = {0};
static JJChar jjstrLiteralChars_607[] = {0};
static JJChar jjstrLiteralChars_608[] = {0};
static JJChar jjstrLiteralChars_609[] = {0};
static JJChar jjstrLiteralChars_610[] = {0};
static JJChar jjstrLiteralChars_611[] = {0};
static JJChar jjstrLiteralChars_612[] = {0};
static JJChar jjstrLiteralChars_613[] = {0};
static JJChar jjstrLiteralChars_614[] = {0};
static JJChar jjstrLiteralChars_615[] = {0};
static JJChar jjstrLiteralChars_616[] = {0};
static JJChar jjstrLiteralChars_617[] = {0};
static JJChar jjstrLiteralChars_618[] = {0};
static JJChar jjstrLiteralChars_619[] = {0};

static JJChar jjstrLiteralChars_620[] = {0};
static JJChar jjstrLiteralChars_621[] = {0};
static JJChar jjstrLiteralChars_622[] = {0};
static JJChar jjstrLiteralChars_623[] = {0};
static JJChar jjstrLiteralChars_624[] = {0};
static JJChar jjstrLiteralChars_625[] = {0};
static JJChar jjstrLiteralChars_626[] = {0};
static JJChar jjstrLiteralChars_627[] = {0};
static JJChar jjstrLiteralChars_628[] = {0};
static JJChar jjstrLiteralChars_629[] = {0};
static JJChar jjstrLiteralChars_630[] = {0};
static JJChar jjstrLiteralChars_631[] = {0};
static JJChar jjstrLiteralChars_632[] = {0};
static JJChar jjstrLiteralChars_633[] = {0};

static JJChar jjstrLiteralChars_634[] = {0};
static JJChar jjstrLiteralChars_635[] = {0};
static JJChar jjstrLiteralChars_636[] = {0};
static JJChar jjstrLiteralChars_637[] = {0};
static JJChar jjstrLiteralChars_638[] = {0};
static JJChar jjstrLiteralChars_639[] = {0};
static JJChar jjstrLiteralChars_640[] = {0};
static JJChar jjstrLiteralChars_641[] = {0};
static JJChar jjstrLiteralChars_642[] = {0};
static JJChar jjstrLiteralChars_643[] = {0};
static const JJString jjstrLiteralImages[] = {
jjstrLiteralChars_0, 
jjstrLiteralChars_1, 
jjstrLiteralChars_2, 
jjstrLiteralChars_3, 
jjstrLiteralChars_4, 
jjstrLiteralChars_5, 
jjstrLiteralChars_6, 
jjstrLiteralChars_7, 
jjstrLiteralChars_8, 
jjstrLiteralChars_9, 
jjstrLiteralChars_10, 
jjstrLiteralChars_11, 
jjstrLiteralChars_12, 
jjstrLiteralChars_13, 
jjstrLiteralChars_14, 
jjstrLiteralChars_15, 
jjstrLiteralChars_16, 
jjstrLiteralChars_17, 
jjstrLiteralChars_18, 
jjstrLiteralChars_19, 
jjstrLiteralChars_20, 
jjstrLiteralChars_21, 
jjstrLiteralChars_22, 
jjstrLiteralChars_23, 
jjstrLiteralChars_24, 
jjstrLiteralChars_25, 
jjstrLiteralChars_26, 
jjstrLiteralChars_27, 
jjstrLiteralChars_28, 
jjstrLiteralChars_29, 
jjstrLiteralChars_30, 
jjstrLiteralChars_31, 
jjstrLiteralChars_32, 
jjstrLiteralChars_33, 
jjstrLiteralChars_34, 
jjstrLiteralChars_35, 
jjstrLiteralChars_36, 
jjstrLiteralChars_37, 
jjstrLiteralChars_38, 
jjstrLiteralChars_39, 
jjstrLiteralChars_40, 
jjstrLiteralChars_41, 
jjstrLiteralChars_42, 
jjstrLiteralChars_43, 
jjstrLiteralChars_44, 
jjstrLiteralChars_45, 
jjstrLiteralChars_46, 
jjstrLiteralChars_47, 
jjstrLiteralChars_48, 
jjstrLiteralChars_49, 
jjstrLiteralChars_50, 
jjstrLiteralChars_51, 
jjstrLiteralChars_52, 
jjstrLiteralChars_53, 
jjstrLiteralChars_54, 
jjstrLiteralChars_55, 
jjstrLiteralChars_56, 
jjstrLiteralChars_57, 
jjstrLiteralChars_58, 
jjstrLiteralChars_59, 
jjstrLiteralChars_60, 
jjstrLiteralChars_61, 
jjstrLiteralChars_62, 
jjstrLiteralChars_63, 
jjstrLiteralChars_64, 
jjstrLiteralChars_65, 
jjstrLiteralChars_66, 
jjstrLiteralChars_67, 
jjstrLiteralChars_68, 
jjstrLiteralChars_69, 
jjstrLiteralChars_70, 
jjstrLiteralChars_71, 
jjstrLiteralChars_72, 
jjstrLiteralChars_73, 
jjstrLiteralChars_74, 
jjstrLiteralChars_75, 
jjstrLiteralChars_76, 
jjstrLiteralChars_77, 
jjstrLiteralChars_78, 
jjstrLiteralChars_79, 
jjstrLiteralChars_80, 
jjstrLiteralChars_81, 
jjstrLiteralChars_82, 
jjstrLiteralChars_83, 
jjstrLiteralChars_84, 
jjstrLiteralChars_85, 
jjstrLiteralChars_86, 
jjstrLiteralChars_87, 
jjstrLiteralChars_88, 
jjstrLiteralChars_89, 
jjstrLiteralChars_90, 
jjstrLiteralChars_91, 
jjstrLiteralChars_92, 
jjstrLiteralChars_93, 
jjstrLiteralChars_94, 
jjstrLiteralChars_95, 
jjstrLiteralChars_96, 
jjstrLiteralChars_97, 
jjstrLiteralChars_98, 
jjstrLiteralChars_99, 
jjstrLiteralChars_100, 
jjstrLiteralChars_101, 
jjstrLiteralChars_102, 
jjstrLiteralChars_103, 
jjstrLiteralChars_104, 
jjstrLiteralChars_105, 
jjstrLiteralChars_106, 
jjstrLiteralChars_107, 
jjstrLiteralChars_108, 
jjstrLiteralChars_109, 
jjstrLiteralChars_110, 
jjstrLiteralChars_111, 
jjstrLiteralChars_112, 
jjstrLiteralChars_113, 
jjstrLiteralChars_114, 
jjstrLiteralChars_115, 
jjstrLiteralChars_116, 
jjstrLiteralChars_117, 
jjstrLiteralChars_118, 
jjstrLiteralChars_119, 
jjstrLiteralChars_120, 
jjstrLiteralChars_121, 
jjstrLiteralChars_122, 
jjstrLiteralChars_123, 
jjstrLiteralChars_124, 
jjstrLiteralChars_125, 
jjstrLiteralChars_126, 
jjstrLiteralChars_127, 
jjstrLiteralChars_128, 
jjstrLiteralChars_129, 
jjstrLiteralChars_130, 
jjstrLiteralChars_131, 
jjstrLiteralChars_132, 
jjstrLiteralChars_133, 
jjstrLiteralChars_134, 
jjstrLiteralChars_135, 
jjstrLiteralChars_136, 
jjstrLiteralChars_137, 
jjstrLiteralChars_138, 
jjstrLiteralChars_139, 
jjstrLiteralChars_140, 
jjstrLiteralChars_141, 
jjstrLiteralChars_142, 
jjstrLiteralChars_143, 
jjstrLiteralChars_144, 
jjstrLiteralChars_145, 
jjstrLiteralChars_146, 
jjstrLiteralChars_147, 
jjstrLiteralChars_148, 
jjstrLiteralChars_149, 
jjstrLiteralChars_150, 
jjstrLiteralChars_151, 
jjstrLiteralChars_152, 
jjstrLiteralChars_153, 
jjstrLiteralChars_154, 
jjstrLiteralChars_155, 
jjstrLiteralChars_156, 
jjstrLiteralChars_157, 
jjstrLiteralChars_158, 
jjstrLiteralChars_159, 
jjstrLiteralChars_160, 
jjstrLiteralChars_161, 
jjstrLiteralChars_162, 
jjstrLiteralChars_163, 
jjstrLiteralChars_164, 
jjstrLiteralChars_165, 
jjstrLiteralChars_166, 
jjstrLiteralChars_167, 
jjstrLiteralChars_168, 
jjstrLiteralChars_169, 
jjstrLiteralChars_170, 
jjstrLiteralChars_171, 
jjstrLiteralChars_172, 
jjstrLiteralChars_173, 
jjstrLiteralChars_174, 
jjstrLiteralChars_175, 
jjstrLiteralChars_176, 
jjstrLiteralChars_177, 
jjstrLiteralChars_178, 
jjstrLiteralChars_179, 
jjstrLiteralChars_180, 
jjstrLiteralChars_181, 
jjstrLiteralChars_182, 
jjstrLiteralChars_183, 
jjstrLiteralChars_184, 
jjstrLiteralChars_185, 
jjstrLiteralChars_186, 
jjstrLiteralChars_187, 
jjstrLiteralChars_188, 
jjstrLiteralChars_189, 
jjstrLiteralChars_190, 
jjstrLiteralChars_191, 
jjstrLiteralChars_192, 
jjstrLiteralChars_193, 
jjstrLiteralChars_194, 
jjstrLiteralChars_195, 
jjstrLiteralChars_196, 
jjstrLiteralChars_197, 
jjstrLiteralChars_198, 
jjstrLiteralChars_199, 
jjstrLiteralChars_200, 
jjstrLiteralChars_201, 
jjstrLiteralChars_202, 
jjstrLiteralChars_203, 
jjstrLiteralChars_204, 
jjstrLiteralChars_205, 
jjstrLiteralChars_206, 
jjstrLiteralChars_207, 
jjstrLiteralChars_208, 
jjstrLiteralChars_209, 
jjstrLiteralChars_210, 
jjstrLiteralChars_211, 
jjstrLiteralChars_212, 
jjstrLiteralChars_213, 
jjstrLiteralChars_214, 
jjstrLiteralChars_215, 
jjstrLiteralChars_216, 
jjstrLiteralChars_217, 
jjstrLiteralChars_218, 
jjstrLiteralChars_219, 
jjstrLiteralChars_220, 
jjstrLiteralChars_221, 
jjstrLiteralChars_222, 
jjstrLiteralChars_223, 
jjstrLiteralChars_224, 
jjstrLiteralChars_225, 
jjstrLiteralChars_226, 
jjstrLiteralChars_227, 
jjstrLiteralChars_228, 
jjstrLiteralChars_229, 
jjstrLiteralChars_230, 
jjstrLiteralChars_231, 
jjstrLiteralChars_232, 
jjstrLiteralChars_233, 
jjstrLiteralChars_234, 
jjstrLiteralChars_235, 
jjstrLiteralChars_236, 
jjstrLiteralChars_237, 
jjstrLiteralChars_238, 
jjstrLiteralChars_239, 
jjstrLiteralChars_240, 
jjstrLiteralChars_241, 
jjstrLiteralChars_242, 
jjstrLiteralChars_243, 
jjstrLiteralChars_244, 
jjstrLiteralChars_245, 
jjstrLiteralChars_246, 
jjstrLiteralChars_247, 
jjstrLiteralChars_248, 
jjstrLiteralChars_249, 
jjstrLiteralChars_250, 
jjstrLiteralChars_251, 
jjstrLiteralChars_252, 
jjstrLiteralChars_253, 
jjstrLiteralChars_254, 
jjstrLiteralChars_255, 
jjstrLiteralChars_256, 
jjstrLiteralChars_257, 
jjstrLiteralChars_258, 
jjstrLiteralChars_259, 
jjstrLiteralChars_260, 
jjstrLiteralChars_261, 
jjstrLiteralChars_262, 
jjstrLiteralChars_263, 
jjstrLiteralChars_264, 
jjstrLiteralChars_265, 
jjstrLiteralChars_266, 
jjstrLiteralChars_267, 
jjstrLiteralChars_268, 
jjstrLiteralChars_269, 
jjstrLiteralChars_270, 
jjstrLiteralChars_271, 
jjstrLiteralChars_272, 
jjstrLiteralChars_273, 
jjstrLiteralChars_274, 
jjstrLiteralChars_275, 
jjstrLiteralChars_276, 
jjstrLiteralChars_277, 
jjstrLiteralChars_278, 
jjstrLiteralChars_279, 
jjstrLiteralChars_280, 
jjstrLiteralChars_281, 
jjstrLiteralChars_282, 
jjstrLiteralChars_283, 
jjstrLiteralChars_284, 
jjstrLiteralChars_285, 
jjstrLiteralChars_286, 
jjstrLiteralChars_287, 
jjstrLiteralChars_288, 
jjstrLiteralChars_289, 
jjstrLiteralChars_290, 
jjstrLiteralChars_291, 
jjstrLiteralChars_292, 
jjstrLiteralChars_293, 
jjstrLiteralChars_294, 
jjstrLiteralChars_295, 
jjstrLiteralChars_296, 
jjstrLiteralChars_297, 
jjstrLiteralChars_298, 
jjstrLiteralChars_299, 
jjstrLiteralChars_300, 
jjstrLiteralChars_301, 
jjstrLiteralChars_302, 
jjstrLiteralChars_303, 
jjstrLiteralChars_304, 
jjstrLiteralChars_305, 
jjstrLiteralChars_306, 
jjstrLiteralChars_307, 
jjstrLiteralChars_308, 
jjstrLiteralChars_309, 
jjstrLiteralChars_310, 
jjstrLiteralChars_311, 
jjstrLiteralChars_312, 
jjstrLiteralChars_313, 
jjstrLiteralChars_314, 
jjstrLiteralChars_315, 
jjstrLiteralChars_316, 
jjstrLiteralChars_317, 
jjstrLiteralChars_318, 
jjstrLiteralChars_319, 
jjstrLiteralChars_320, 
jjstrLiteralChars_321, 
jjstrLiteralChars_322, 
jjstrLiteralChars_323, 
jjstrLiteralChars_324, 
jjstrLiteralChars_325, 
jjstrLiteralChars_326, 
jjstrLiteralChars_327, 
jjstrLiteralChars_328, 
jjstrLiteralChars_329, 
jjstrLiteralChars_330, 
jjstrLiteralChars_331, 
jjstrLiteralChars_332, 
jjstrLiteralChars_333, 
jjstrLiteralChars_334, 
jjstrLiteralChars_335, 
jjstrLiteralChars_336, 
jjstrLiteralChars_337, 
jjstrLiteralChars_338, 
jjstrLiteralChars_339, 
jjstrLiteralChars_340, 
jjstrLiteralChars_341, 
jjstrLiteralChars_342, 
jjstrLiteralChars_343, 
jjstrLiteralChars_344, 
jjstrLiteralChars_345, 
jjstrLiteralChars_346, 
jjstrLiteralChars_347, 
jjstrLiteralChars_348, 
jjstrLiteralChars_349, 
jjstrLiteralChars_350, 
jjstrLiteralChars_351, 
jjstrLiteralChars_352, 
jjstrLiteralChars_353, 
jjstrLiteralChars_354, 
jjstrLiteralChars_355, 
jjstrLiteralChars_356, 
jjstrLiteralChars_357, 
jjstrLiteralChars_358, 
jjstrLiteralChars_359, 
jjstrLiteralChars_360, 
jjstrLiteralChars_361, 
jjstrLiteralChars_362, 
jjstrLiteralChars_363, 
jjstrLiteralChars_364, 
jjstrLiteralChars_365, 
jjstrLiteralChars_366, 
jjstrLiteralChars_367, 
jjstrLiteralChars_368, 
jjstrLiteralChars_369, 
jjstrLiteralChars_370, 
jjstrLiteralChars_371, 
jjstrLiteralChars_372, 
jjstrLiteralChars_373, 
jjstrLiteralChars_374, 
jjstrLiteralChars_375, 
jjstrLiteralChars_376, 
jjstrLiteralChars_377, 
jjstrLiteralChars_378, 
jjstrLiteralChars_379, 
jjstrLiteralChars_380, 
jjstrLiteralChars_381, 
jjstrLiteralChars_382, 
jjstrLiteralChars_383, 
jjstrLiteralChars_384, 
jjstrLiteralChars_385, 
jjstrLiteralChars_386, 
jjstrLiteralChars_387, 
jjstrLiteralChars_388, 
jjstrLiteralChars_389, 
jjstrLiteralChars_390, 
jjstrLiteralChars_391, 
jjstrLiteralChars_392, 
jjstrLiteralChars_393, 
jjstrLiteralChars_394, 
jjstrLiteralChars_395, 
jjstrLiteralChars_396, 
jjstrLiteralChars_397, 
jjstrLiteralChars_398, 
jjstrLiteralChars_399, 
jjstrLiteralChars_400, 
jjstrLiteralChars_401, 
jjstrLiteralChars_402, 
jjstrLiteralChars_403, 
jjstrLiteralChars_404, 
jjstrLiteralChars_405, 
jjstrLiteralChars_406, 
jjstrLiteralChars_407, 
jjstrLiteralChars_408, 
jjstrLiteralChars_409, 
jjstrLiteralChars_410, 
jjstrLiteralChars_411, 
jjstrLiteralChars_412, 
jjstrLiteralChars_413, 
jjstrLiteralChars_414, 
jjstrLiteralChars_415, 
jjstrLiteralChars_416, 
jjstrLiteralChars_417, 
jjstrLiteralChars_418, 
jjstrLiteralChars_419, 
jjstrLiteralChars_420, 
jjstrLiteralChars_421, 
jjstrLiteralChars_422, 
jjstrLiteralChars_423, 
jjstrLiteralChars_424, 
jjstrLiteralChars_425, 
jjstrLiteralChars_426, 
jjstrLiteralChars_427, 
jjstrLiteralChars_428, 
jjstrLiteralChars_429, 
jjstrLiteralChars_430, 
jjstrLiteralChars_431, 
jjstrLiteralChars_432, 
jjstrLiteralChars_433, 
jjstrLiteralChars_434, 
jjstrLiteralChars_435, 
jjstrLiteralChars_436, 
jjstrLiteralChars_437, 
jjstrLiteralChars_438, 
jjstrLiteralChars_439, 
jjstrLiteralChars_440, 
jjstrLiteralChars_441, 
jjstrLiteralChars_442, 
jjstrLiteralChars_443, 
jjstrLiteralChars_444, 
jjstrLiteralChars_445, 
jjstrLiteralChars_446, 
jjstrLiteralChars_447, 
jjstrLiteralChars_448, 
jjstrLiteralChars_449, 
jjstrLiteralChars_450, 
jjstrLiteralChars_451, 
jjstrLiteralChars_452, 
jjstrLiteralChars_453, 
jjstrLiteralChars_454, 
jjstrLiteralChars_455, 
jjstrLiteralChars_456, 
jjstrLiteralChars_457, 
jjstrLiteralChars_458, 
jjstrLiteralChars_459, 
jjstrLiteralChars_460, 
jjstrLiteralChars_461, 
jjstrLiteralChars_462, 
jjstrLiteralChars_463, 
jjstrLiteralChars_464, 
jjstrLiteralChars_465, 
jjstrLiteralChars_466, 
jjstrLiteralChars_467, 
jjstrLiteralChars_468, 
jjstrLiteralChars_469, 
jjstrLiteralChars_470, 
jjstrLiteralChars_471, 
jjstrLiteralChars_472, 
jjstrLiteralChars_473, 
jjstrLiteralChars_474, 
jjstrLiteralChars_475, 
jjstrLiteralChars_476, 
jjstrLiteralChars_477, 
jjstrLiteralChars_478, 
jjstrLiteralChars_479, 
jjstrLiteralChars_480, 
jjstrLiteralChars_481, 
jjstrLiteralChars_482, 
jjstrLiteralChars_483, 
jjstrLiteralChars_484, 
jjstrLiteralChars_485, 
jjstrLiteralChars_486, 
jjstrLiteralChars_487, 
jjstrLiteralChars_488, 
jjstrLiteralChars_489, 
jjstrLiteralChars_490, 
jjstrLiteralChars_491, 
jjstrLiteralChars_492, 
jjstrLiteralChars_493, 
jjstrLiteralChars_494, 
jjstrLiteralChars_495, 
jjstrLiteralChars_496, 
jjstrLiteralChars_497, 
jjstrLiteralChars_498, 
jjstrLiteralChars_499, 
jjstrLiteralChars_500, 
jjstrLiteralChars_501, 
jjstrLiteralChars_502, 
jjstrLiteralChars_503, 
jjstrLiteralChars_504, 
jjstrLiteralChars_505, 
jjstrLiteralChars_506, 
jjstrLiteralChars_507, 
jjstrLiteralChars_508, 
jjstrLiteralChars_509, 
jjstrLiteralChars_510, 
jjstrLiteralChars_511, 
jjstrLiteralChars_512, 
jjstrLiteralChars_513, 
jjstrLiteralChars_514, 
jjstrLiteralChars_515, 
jjstrLiteralChars_516, 
jjstrLiteralChars_517, 
jjstrLiteralChars_518, 
jjstrLiteralChars_519, 
jjstrLiteralChars_520, 
jjstrLiteralChars_521, 
jjstrLiteralChars_522, 
jjstrLiteralChars_523, 
jjstrLiteralChars_524, 
jjstrLiteralChars_525, 
jjstrLiteralChars_526, 
jjstrLiteralChars_527, 
jjstrLiteralChars_528, 
jjstrLiteralChars_529, 
jjstrLiteralChars_530, 
jjstrLiteralChars_531, 
jjstrLiteralChars_532, 
jjstrLiteralChars_533, 
jjstrLiteralChars_534, 
jjstrLiteralChars_535, 
jjstrLiteralChars_536, 
jjstrLiteralChars_537, 
jjstrLiteralChars_538, 
jjstrLiteralChars_539, 
jjstrLiteralChars_540, 
jjstrLiteralChars_541, 
jjstrLiteralChars_542, 
jjstrLiteralChars_543, 
jjstrLiteralChars_544, 
jjstrLiteralChars_545, 
jjstrLiteralChars_546, 
jjstrLiteralChars_547, 
jjstrLiteralChars_548, 
jjstrLiteralChars_549, 
jjstrLiteralChars_550, 
jjstrLiteralChars_551, 
jjstrLiteralChars_552, 
jjstrLiteralChars_553, 
jjstrLiteralChars_554, 
jjstrLiteralChars_555, 
jjstrLiteralChars_556, 
jjstrLiteralChars_557, 
jjstrLiteralChars_558, 
jjstrLiteralChars_559, 
jjstrLiteralChars_560, 
jjstrLiteralChars_561, 
jjstrLiteralChars_562, 
jjstrLiteralChars_563, 
jjstrLiteralChars_564, 
jjstrLiteralChars_565, 
jjstrLiteralChars_566, 
jjstrLiteralChars_567, 
jjstrLiteralChars_568, 
jjstrLiteralChars_569, 
jjstrLiteralChars_570, 
jjstrLiteralChars_571, 
jjstrLiteralChars_572, 
jjstrLiteralChars_573, 
jjstrLiteralChars_574, 
jjstrLiteralChars_575, 
jjstrLiteralChars_576, 
jjstrLiteralChars_577, 
jjstrLiteralChars_578, 
jjstrLiteralChars_579, 
jjstrLiteralChars_580, 
jjstrLiteralChars_581, 
jjstrLiteralChars_582, 
jjstrLiteralChars_583, 
jjstrLiteralChars_584, 
jjstrLiteralChars_585, 
jjstrLiteralChars_586, 
jjstrLiteralChars_587, 
jjstrLiteralChars_588, 
jjstrLiteralChars_589, 
jjstrLiteralChars_590, 
jjstrLiteralChars_591, 
jjstrLiteralChars_592, 
jjstrLiteralChars_593, 
jjstrLiteralChars_594, 
jjstrLiteralChars_595, 
jjstrLiteralChars_596, 
jjstrLiteralChars_597, 
jjstrLiteralChars_598, 
jjstrLiteralChars_599, 
jjstrLiteralChars_600, 
jjstrLiteralChars_601, 
jjstrLiteralChars_602, 
jjstrLiteralChars_603, 
jjstrLiteralChars_604, 
jjstrLiteralChars_605, 
jjstrLiteralChars_606, 
jjstrLiteralChars_607, 
jjstrLiteralChars_608, 
jjstrLiteralChars_609, 
jjstrLiteralChars_610, 
jjstrLiteralChars_611, 
jjstrLiteralChars_612, 
jjstrLiteralChars_613, 
jjstrLiteralChars_614, 
jjstrLiteralChars_615, 
jjstrLiteralChars_616, 
jjstrLiteralChars_617, 
jjstrLiteralChars_618, 
jjstrLiteralChars_619, 
jjstrLiteralChars_620, 
jjstrLiteralChars_621, 
jjstrLiteralChars_622, 
jjstrLiteralChars_623, 
jjstrLiteralChars_624, 
jjstrLiteralChars_625, 
jjstrLiteralChars_626, 
jjstrLiteralChars_627, 
jjstrLiteralChars_628, 
jjstrLiteralChars_629, 
jjstrLiteralChars_630, 
jjstrLiteralChars_631, 
jjstrLiteralChars_632, 
jjstrLiteralChars_633, 
jjstrLiteralChars_634, 
jjstrLiteralChars_635, 
jjstrLiteralChars_636, 
jjstrLiteralChars_637, 
jjstrLiteralChars_638, 
jjstrLiteralChars_639, 
jjstrLiteralChars_640, 
jjstrLiteralChars_641, 
jjstrLiteralChars_642, 
jjstrLiteralChars_643, 
};

/** Lexer state names. */
static const JJChar lexStateNames_arr_0[] = 
{0x44, 0x45, 0x46, 0x41, 0x55, 0x4c, 0x54, 0};
static const JJChar lexStateNames_arr_1[] = 
{0x55, 0x4e, 0x52, 0x45, 0x41, 0x43, 0x48, 0x41, 0x42, 0x4c, 0x45, 0};
static const JJChar lexStateNames_arr_2[] = 
{0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74, 0x5f, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x73, 0};
static const JJChar lexStateNames_arr_3[] = 
{0x6d, 0x61, 0x74, 0x63, 0x68, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74, 0};
static const JJString lexStateNames[] = {
lexStateNames_arr_0, 
lexStateNames_arr_1, 
lexStateNames_arr_2, 
lexStateNames_arr_3, 
};

/** Lex State array. */
static const int jjnewLexState[] = {
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 2, 3, -1, 0, -1, -1, -1, -1, 
   -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 
};
static const unsigned long long jjtoToken[] = {
   0xfffffffffffffffdULL, 0xffffffffffffffffULL, 0xffffffffffffffffULL, 0xffffffffffffffffULL, 
   0xffffffffffffffffULL, 0xfffffffff3ffffffULL, 0xffffffffffffffffULL, 0xffffffffffffffffULL, 
   0xffffdfffffffffffULL, 0x8e9c0000009c3fffULL, 0x8ULL, 
};
static const unsigned long long jjtoSkip[] = {
   0x2ULL, 0x0ULL, 0x0ULL, 0x0ULL, 
   0x0ULL, 0xc000000ULL, 0x0ULL, 0x0ULL, 
   0x200000000000ULL, 0x111400000000ULL, 0x0ULL, 
};
static const unsigned long long jjtoSpecial[] = {
   0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 
   0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 
   0x0ULL, 0x111400000000ULL, 0x0ULL, 
};
  void setKindToIdentifier(Token *t) {
    t->kind = regular_identifier;
  }

  void setUnicodeLiteralType(Token *t) {
    t->kind = unicode_literal;
  }

  void StoreImage(Token* matchedToken) {
    // TODO(sreeni): fix it.
    // matchedToken->image = image;
  }
  void  SqlParserTokenManager::setDebugStream(FILE *ds){ debugStream = ds; }

 int SqlParserTokenManager::jjStopStringLiteralDfa_0(int pos, unsigned long long active0, unsigned long long active1, unsigned long long active2, unsigned long long active3, unsigned long long active4, unsigned long long active5, unsigned long long active6, unsigned long long active7, unsigned long long active8, unsigned long long active9){
   switch (pos)
   {
      case 0:
         if ((active8 & 0x200000000000000ULL) != 0L)
            return 152;
         if ((active8 & 0x2000000000000ULL) != 0L)
            return 1;
         if ((active5 & 0x2000000ULL) != 0L)
            return 153;
         if ((active3 & 0xff80000ULL) != 0L || (active5 & 0x10078ULL) != 0L || (active8 & 0xf8000000ULL) != 0L)
         {
            jjmatchedKind = 589;
            return 7;
         }
         if ((active8 & 0x2080000000000000ULL) != 0L)
            return 15;
         if ((active0 & 0x1fffffffc7ffcULL) != 0L || (active1 & 0xffc3c000fc000ULL) != 0L || (active2 & 0x7ffff00ULL) != 0L || (active3 & 0x3ffff9f0007fff8ULL) != 0L || (active4 & 0xe000003c07fe0020ULL) != 0L || (active5 & 0xffffe07ff1604007ULL) != 0L || (active6 & 0x4003c00000000ffULL) != 0L || (active7 & 0x7f8000007c0ULL) != 0L || (active8 & 0x7ff8000ULL) != 0L)
            return 154;
         if ((active0 & 0xfffe000000038000ULL) != 0L || (active1 & 0x800003c3fff03fffULL) != 0L || (active2 & 0xfffffffff80000ffULL) != 0L || (active3 & 0xfc000060f0000007ULL) != 0L || (active4 & 0x1fffffc3c001ffdfULL) != 0L || (active5 & 0x1f80009abf80ULL) != 0L || (active6 & 0xfbffc3ffffffff00ULL) != 0L || (active7 & 0xfffff807ff80003fULL) != 0L || (active8 & 0x1fff00007fffULL) != 0L)
         {
            jjmatchedKind = 589;
            return 154;
         }
         if ((active1 & 0x7ff0000000000000ULL) != 0L || (active4 & 0x38000000ULL) != 0L || (active5 & 0x40000ULL) != 0L || (active7 & 0x7ff800ULL) != 0L)
         {
            jjmatchedKind = 589;
            return 25;
         }
         return -1;
      case 1:
         if ((active0 & 0xfffffffffffb83f8ULL) != 0L || (active1 & 0xf7fffbf801d9bfffULL) != 0L || (active2 & 0xfffffffffffffee7ULL) != 0L || (active3 & 0xfffffff7ffffff77ULL) != 0L || (active4 & 0xffffffff5fffe3ffULL) != 0L || (active5 & 0xffffefc3f1ffffffULL) != 0L || (active6 & 0xfe007fffffffffffULL) != 0L || (active7 & 0xffffffffc1fc7ff7ULL) != 0L || (active8 & 0x1ffffffbffffULL) != 0L)
         {
            if (jjmatchedPos != 1)
            {
               jjmatchedKind = 589;
               jjmatchedPos = 1;
            }
            return 154;
         }
         if ((active5 & 0x2000000ULL) != 0L)
            return 153;
         if ((active0 & 0x7c00ULL) != 0L || (active1 & 0x8000003fe260000ULL) != 0L || (active2 & 0x18ULL) != 0L || (active3 & 0x800000080ULL) != 0L || (active4 & 0xa0001c00ULL) != 0L || (active5 & 0x103c00000000ULL) != 0L || (active6 & 0x1ff800000000000ULL) != 0L || (active7 & 0x3e038008ULL) != 0L || (active8 & 0x40000ULL) != 0L)
            return 154;
         return -1;
      case 2:
         if ((active0 & 0xfffffffffffbfb90ULL) != 0L || (active1 & 0xfe7dd3c3ffddaf7fULL) != 0L || (active2 & 0xfefffbffffeffcbfULL) != 0L || (active3 & 0x47ffffe4f0fffff7ULL) != 0L || (active4 & 0xf6fc79feee0de7ffULL) != 0L || (active5 & 0xffffeffa203effbfULL) != 0L || (active6 & 0xf60f7b8ffe7ff9ffULL) != 0L || (active7 & 0xbfffdfff29fd7d77ULL) != 0L || (active8 & 0x1ffffffbff8fULL) != 0L)
         {
            if (jjmatchedPos != 2)
            {
               jjmatchedKind = 589;
               jjmatchedPos = 2;
            }
            return 154;
         }
         if ((active5 & 0x2000000ULL) != 0L)
            return 153;
         if ((active0 & 0x468ULL) != 0L || (active1 & 0x182283800001080ULL) != 0L || (active2 & 0x100040000100240ULL) != 0L || (active3 & 0xb80000130f000000ULL) != 0L || (active4 & 0x903860111f21800ULL) != 0L || (active5 & 0x1d1c10040ULL) != 0L || (active6 & 0x8f0047001800600ULL) != 0L || (active7 & 0x40002000c0020280ULL) != 0L || (active8 & 0x70ULL) != 0L)
            return 154;
         return -1;
      case 3:
         if ((active5 & 0x2000000ULL) != 0L)
            return 153;
         if ((active0 & 0x300e00000f800000ULL) != 0L || (active1 & 0x3654004000040400ULL) != 0L || (active2 & 0x508002008040080ULL) != 0L || (active3 & 0x14001060bf040040ULL) != 0L || (active4 & 0xe0020182280041c8ULL) != 0L || (active5 & 0x2ff600000003041ULL) != 0L || (active6 & 0xa680018030584006ULL) != 0L || (active7 & 0x80708300007ULL) != 0L || (active8 & 0x1cc007020082ULL) != 0L)
            return 154;
         if ((active0 & 0xcff1fffff07bfb98ULL) != 0L || (active1 & 0xc82bf3b3ffd9bbffULL) != 0L || (active2 & 0xfaf7ffdff7ebfc7fULL) != 0L || (active3 & 0x43ffef8440fbffb7ULL) != 0L || (active4 & 0x16fd7c7cc76dbe37ULL) != 0L || (active5 & 0xfd008ffa31fecfbeULL) != 0L || (active6 & 0x506f7a6fce27bff9ULL) != 0L || (active7 & 0xbffff7f8a1cd7ff0ULL) != 0L || (active8 & 0x33ff8f9ff7dULL) != 0L)
         {
            if (jjmatchedPos != 3)
            {
               jjmatchedKind = 589;
               jjmatchedPos = 3;
            }
            return 154;
         }
         return -1;
      case 4:
         if ((active0 & 0xebfdffffdfbbfa18ULL) != 0L || (active1 & 0xd823e2b3efd198ffULL) != 0L || (active2 & 0x2af637dff76bfcf7ULL) != 0L || (active3 & 0x41ffe6800f5bffa5ULL) != 0L || (active4 & 0xd4f57c2cc1683e33ULL) != 0L || (active5 & 0xfce8cf7811fcce1fULL) != 0L || (active6 & 0xe06c4260ea27dfbdULL) != 0L || (active7 & 0xbdfff7fe01e51f84ULL) != 0L || (active8 & 0x1abf6a787f7dULL) != 0L)
         {
            if (jjmatchedPos != 4)
            {
               jjmatchedKind = 589;
               jjmatchedPos = 4;
            }
            return 154;
         }
         if ((active5 & 0x2000000ULL) != 0L)
            return 153;
         if ((active0 & 0x400000020400180ULL) != 0L || (active1 & 0x2018110010082300ULL) != 0L || (active2 & 0xd001c80000800008ULL) != 0L || (active3 & 0x200090440a00012ULL) != 0L || (active4 & 0x208015006058104ULL) != 0L || (active5 & 0x1000082200221a0ULL) != 0L || (active6 & 0x1003380f04002040ULL) != 0L || (active7 & 0x2000000a0086070ULL) != 0L || (active8 & 0x10090818000ULL) != 0L)
            return 154;
         return -1;
      case 5:
         if ((active0 & 0xe9fcffdbdfbb7808ULL) != 0L || (active1 & 0x1823f233ef9998eeULL) != 0L || (active2 & 0xa0f7c7c1f369fc98ULL) != 0L || (active3 & 0x40ffe4840f5bffb5ULL) != 0L || (active4 & 0xc0850c2c40003e11ULL) != 0L || (active5 & 0xdce8895811fcc60fULL) != 0L || (active6 & 0xe0642064a023d79dULL) != 0L || (active7 & 0xa47ff7fe01c51eb4ULL) != 0L || (active8 & 0x12bf0a793f7dULL) != 0L)
         {
            if (jjmatchedPos != 5)
            {
               jjmatchedKind = 589;
               jjmatchedPos = 5;
            }
            return 154;
         }
         if ((active5 & 0x2000000ULL) != 0L)
            return 153;
         if ((active0 & 0x201002400008210ULL) != 0L || (active1 & 0xc000008000400011ULL) != 0L || (active2 & 0xa00301e04020067ULL) != 0L || (active3 & 0x100020000000000ULL) != 0L || (active4 & 0x1470700085680022ULL) != 0L || (active5 & 0x2000462000000910ULL) != 0L || (active6 & 0x842004a040820ULL) != 0L || (active7 & 0x1980000000200100ULL) != 0L || (active8 & 0x80060004000ULL) != 0L)
            return 154;
         return -1;
      case 6:
         if ((active5 & 0x2000000ULL) != 0L)
            return 12;
         if ((active0 & 0x9900180003a0000ULL) != 0L || (active1 & 0x20120180089026ULL) != 0L || (active2 & 0x8203c0c0090004ULL) != 0L || (active3 & 0x40ffe0000041c000ULL) != 0L || (active4 & 0xc0280000400000ULL) != 0L || (active5 & 0x5808890000ccc60aULL) != 0L || (active6 & 0xc0200060a0200601ULL) != 0L || (active7 & 0x8004030400401000ULL) != 0L || (active8 & 0x103908000000ULL) != 0L)
            return 154;
         if ((active0 & 0xe06dfe7fdf817808ULL) != 0L || (active1 & 0x1803e0326f9108c8ULL) != 0L || (active2 & 0xa075e41f3360fc98ULL) != 0L || (active3 & 0x4840f1a3fb5ULL) != 0L || (active4 & 0xc005042c40003e11ULL) != 0L || (active5 & 0x84e0005811300005ULL) != 0L || (active6 & 0x204420040003d19cULL) != 0L || (active7 & 0x247bf4fa01850eb4ULL) != 0L || (active8 & 0x28602797f7dULL) != 0L)
         {
            if (jjmatchedPos != 6)
            {
               jjmatchedKind = 589;
               jjmatchedPos = 6;
            }
            return 154;
         }
         return -1;
      case 7:
         if ((active0 & 0x602d3fffdfa17800ULL) != 0L || (active1 & 0x801c0124f9108c6ULL) != 0L || (active2 & 0x8061e79e2320fc90ULL) != 0L || (active3 & 0xffc4840f19ffb5ULL) != 0L || (active4 & 0xc085042440000c00ULL) != 0L || (active5 & 0x80e0005801340001ULL) != 0L || (active6 & 0x204400040001919cULL) != 0L || (active7 & 0x200bf478018500b4ULL) != 0L || (active8 & 0x22402717f50ULL) != 0L)
         {
            if (jjmatchedPos != 7)
            {
               jjmatchedKind = 589;
               jjmatchedPos = 7;
            }
            return 154;
         }
         if ((active0 & 0x8050c00000000008ULL) != 0L || (active1 & 0x1002202020000008ULL) != 0L || (active2 & 0x2014000110400008ULL) != 0L || (active3 & 0x20000ULL) != 0L || (active4 & 0x800003211ULL) != 0L || (active5 & 0x400800010000404ULL) != 0L || (active6 & 0x200000024000ULL) != 0L || (active7 & 0x470008200000e00ULL) != 0L || (active8 & 0x820008002dULL) != 0L)
            return 154;
         return -1;
      case 8:
         if ((active0 & 0x602d3f9c10201000ULL) != 0L || (active1 & 0x1c01041000086ULL) != 0L || (active2 & 0x2061e39e23000090ULL) != 0L || (active3 & 0xffc0800f11cf94ULL) != 0L || (active4 & 0x8081040040000000ULL) != 0L || (active5 & 0x8020005800240001ULL) != 0L || (active6 & 0x2004000400019114ULL) != 0L || (active7 & 0xb20f001800084ULL) != 0L || (active8 & 0x22002414758ULL) != 0L)
         {
            if (jjmatchedPos != 8)
            {
               jjmatchedKind = 589;
               jjmatchedPos = 8;
            }
            return 154;
         }
         if ((active0 & 0x63cf816800ULL) != 0L || (active1 & 0x80000020e910840ULL) != 0L || (active2 & 0x800004000020fc00ULL) != 0L || (active3 & 0x40400083021ULL) != 0L || (active4 & 0x4004002400000c00ULL) != 0L || (active5 & 0xc0000001100000ULL) != 0L || (active6 & 0x40000000000088ULL) != 0L || (active7 & 0x2000d40800050030ULL) != 0L || (active8 & 0x400303800ULL) != 0L)
            return 154;
         return -1;
      case 9:
         if ((active0 & 0x20201f8008005000ULL) != 0L || (active1 & 0x800001000000080ULL) != 0L || (active2 & 0x800023000090ULL) != 0L || (active3 & 0x2010ULL) != 0L || (active4 & 0x1040040000000ULL) != 0L || (active5 & 0x8000001800000000ULL) != 0L || (active6 & 0x2000000000011110ULL) != 0L || (active7 & 0x9000001000004ULL) != 0L || (active8 & 0x2002000140ULL) != 0L)
            return 154;
         if ((active0 & 0x400d205f97a00000ULL) != 0L || (active1 & 0x1c00041000006ULL) != 0L || (active2 & 0x2061639e0000fc00ULL) != 0L || (active3 & 0xffc0800f11cf84ULL) != 0L || (active4 & 0x8080000000000800ULL) != 0L || (active5 & 0xa0004000240001ULL) != 0L || (active6 & 0x4000400008004ULL) != 0L || (active7 & 0x220f0008000a0ULL) != 0L || (active8 & 0x20000615618ULL) != 0L)
         {
            if (jjmatchedPos != 9)
            {
               jjmatchedKind = 589;
               jjmatchedPos = 9;
            }
            return 154;
         }
         return -1;
      case 10:
         if ((active0 & 0xc0f5b97a00000ULL) != 0L || (active1 & 0x1c00041000006ULL) != 0L || (active2 & 0x2021439e0000fc00ULL) != 0L || (active3 & 0xffc0000f01c084ULL) != 0L || (active4 & 0x8080000000000800ULL) != 0L || (active5 & 0x80004000240001ULL) != 0L || (active6 & 0x8004ULL) != 0L || (active7 & 0x200f0018000a0ULL) != 0L || (active8 & 0x20000201018ULL) != 0L)
         {
            if (jjmatchedPos != 10)
            {
               jjmatchedKind = 589;
               jjmatchedPos = 10;
            }
            return 154;
         }
         if ((active0 & 0x4001300400000000ULL) != 0L || (active2 & 0x40200000000000ULL) != 0L || (active3 & 0x8000100f00ULL) != 0L || (active5 & 0x20000000000000ULL) != 0L || (active6 & 0x4000400000000ULL) != 0L || (active7 & 0x200000000000ULL) != 0L || (active8 & 0x414600ULL) != 0L)
            return 154;
         return -1;
      case 11:
         if ((active0 & 0xc0f5b87800000ULL) != 0L || (active1 & 0xc00001000006ULL) != 0L || (active2 & 0x2000429e0000fc00ULL) != 0L || (active3 & 0x4940000f014e84ULL) != 0L || (active4 & 0x8000000000000000ULL) != 0L || (active5 & 0x80004000240001ULL) != 0L || (active6 & 0x8004ULL) != 0L || (active7 & 0x200e0010000a0ULL) != 0L || (active8 & 0x201000ULL) != 0L)
         {
            if (jjmatchedPos != 11)
            {
               jjmatchedKind = 589;
               jjmatchedPos = 11;
            }
            return 154;
         }
         if ((active0 & 0x10200000ULL) != 0L || (active1 & 0x1000040000000ULL) != 0L || (active2 & 0x21010000000000ULL) != 0L || (active3 & 0xb6800000008000ULL) != 0L || (active4 & 0x80000000000800ULL) != 0L || (active7 & 0x1000800000ULL) != 0L || (active8 & 0x20000000018ULL) != 0L)
            return 154;
         return -1;
      case 12:
         if ((active0 & 0xc0f5b87800000ULL) != 0L || (active1 & 0xc00001000006ULL) != 0L || (active2 & 0x29e0000fc00ULL) != 0L || (active3 & 0x6940000f014e84ULL) != 0L || (active5 & 0x80000000240001ULL) != 0L || (active7 & 0x200e0010000a0ULL) != 0L || (active8 & 0x201000ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 12;
            return 154;
         }
         if ((active2 & 0x2000400000000000ULL) != 0L || (active4 & 0x8000000000000000ULL) != 0L || (active5 & 0x4000000000ULL) != 0L || (active6 & 0x8004ULL) != 0L)
            return 154;
         return -1;
      case 13:
         if ((active0 & 0xc0f5a87800000ULL) != 0L || (active1 & 0x800000000006ULL) != 0L || (active2 & 0x9e0000f000ULL) != 0L || (active3 & 0x6140000f004e84ULL) != 0L || (active5 & 0x80000000240001ULL) != 0L || (active7 & 0x6001000080ULL) != 0L || (active8 & 0x201000ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 13;
            return 154;
         }
         if ((active0 & 0x100000000ULL) != 0L || (active1 & 0x400001000000ULL) != 0L || (active2 & 0x20000000c00ULL) != 0L || (active3 & 0x8000000010000ULL) != 0L || (active7 & 0x2008000000020ULL) != 0L)
            return 154;
         return -1;
      case 14:
         if ((active0 & 0xc0a5a83800000ULL) != 0L || (active1 & 0x800000000006ULL) != 0L || (active2 & 0x1a0000f000ULL) != 0L || (active3 & 0x6100000f000e00ULL) != 0L || (active5 & 0x80000000240000ULL) != 0L || (active7 & 0x1000000ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 14;
            return 154;
         }
         if ((active0 & 0x50004000000ULL) != 0L || (active2 & 0x8400000000ULL) != 0L || (active3 & 0x400000004084ULL) != 0L || (active5 & 0x1ULL) != 0L || (active7 & 0x6000000080ULL) != 0L || (active8 & 0x201000ULL) != 0L)
            return 154;
         return -1;
      case 15:
         if ((active0 & 0xc0a0083800000ULL) != 0L || (active1 & 0x800000000000ULL) != 0L || (active2 & 0x1a0000f000ULL) != 0L || (active3 & 0x6100000f000e00ULL) != 0L || (active5 & 0x240000ULL) != 0L || (active7 & 0x1000000ULL) != 0L)
         {
            if (jjmatchedPos != 15)
            {
               jjmatchedKind = 589;
               jjmatchedPos = 15;
            }
            return 154;
         }
         if ((active0 & 0x5a00000000ULL) != 0L || (active1 & 0x6ULL) != 0L || (active5 & 0x80000000000000ULL) != 0L)
            return 154;
         return -1;
      case 16:
         if ((active0 & 0xc021003800000ULL) != 0L || (active1 & 0x800000000004ULL) != 0L || (active2 & 0xa0000f000ULL) != 0L || (active3 & 0x4100000f000e00ULL) != 0L || (active5 & 0x200000ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 16;
            return 154;
         }
         if ((active0 & 0x80080000000ULL) != 0L || (active2 & 0x1000000000ULL) != 0L || (active3 & 0x20000000000000ULL) != 0L || (active5 & 0x40000ULL) != 0L || (active7 & 0x1000000ULL) != 0L)
            return 154;
         return -1;
      case 17:
         if ((active0 & 0xc001002800000ULL) != 0L || (active1 & 0x800000000004ULL) != 0L || (active2 & 0xa0000f000ULL) != 0L || (active3 & 0x4100000f000c00ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 17;
            return 154;
         }
         if ((active0 & 0x20001000000ULL) != 0L || (active3 & 0x200ULL) != 0L || (active5 & 0x200000ULL) != 0L)
            return 154;
         return -1;
      case 18:
         if ((active0 & 0xc001002800000ULL) != 0L || (active1 & 0x800000000004ULL) != 0L || (active2 & 0xa0000f000ULL) != 0L || (active3 & 0x4100000f000c00ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 18;
            return 154;
         }
         return -1;
      case 19:
         if ((active0 & 0x2000000ULL) != 0L || (active1 & 0x800000000000ULL) != 0L || (active2 & 0x200000000ULL) != 0L)
            return 154;
         if ((active0 & 0xc001000800000ULL) != 0L || (active1 & 0x4ULL) != 0L || (active2 & 0x80000f000ULL) != 0L || (active3 & 0x4100000f000c00ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 19;
            return 154;
         }
         return -1;
      case 20:
         if ((active0 & 0xc000000000000ULL) != 0L || (active2 & 0xf000ULL) != 0L || (active3 & 0x4100000f000c00ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 20;
            return 154;
         }
         if ((active0 & 0x1000800000ULL) != 0L || (active1 & 0x4ULL) != 0L || (active2 & 0x800000000ULL) != 0L)
            return 154;
         return -1;
      case 21:
         if ((active0 & 0x8000000000000ULL) != 0L || (active2 & 0xf000ULL) != 0L || (active3 & 0x41000009000800ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 21;
            return 154;
         }
         if ((active0 & 0x4000000000000ULL) != 0L || (active3 & 0x6000400ULL) != 0L)
            return 154;
         return -1;
      case 22:
         if ((active2 & 0x4000ULL) != 0L)
            return 154;
         if ((active0 & 0x8000000000000ULL) != 0L || (active2 & 0xb000ULL) != 0L || (active3 & 0x41000009000800ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 22;
            return 154;
         }
         return -1;
      case 23:
         if ((active0 & 0x8000000000000ULL) != 0L || (active2 & 0xb000ULL) != 0L || (active3 & 0x41000001000000ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 23;
            return 154;
         }
         if ((active3 & 0x8000800ULL) != 0L)
            return 154;
         return -1;
      case 24:
         if ((active0 & 0x8000000000000ULL) != 0L || (active2 & 0x3000ULL) != 0L || (active3 & 0x41000000000000ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 24;
            return 154;
         }
         if ((active2 & 0x8000ULL) != 0L || (active3 & 0x1000000ULL) != 0L)
            return 154;
         return -1;
      case 25:
         if ((active2 & 0x3000ULL) != 0L)
            return 154;
         if ((active0 & 0x8000000000000ULL) != 0L || (active3 & 0x41000000000000ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 25;
            return 154;
         }
         return -1;
      case 26:
         if ((active3 & 0x41000000000000ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 26;
            return 154;
         }
         if ((active0 & 0x8000000000000ULL) != 0L)
            return 154;
         return -1;
      case 27:
         if ((active3 & 0x41000000000000ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 27;
            return 154;
         }
         return -1;
      case 28:
         if ((active3 & 0x41000000000000ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 28;
            return 154;
         }
         return -1;
      case 29:
         if ((active3 & 0x41000000000000ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 29;
            return 154;
         }
         return -1;
      case 30:
         if ((active3 & 0x40000000000000ULL) != 0L)
         {
            jjmatchedKind = 589;
            jjmatchedPos = 30;
            return 154;
         }
         if ((active3 & 0x1000000000000ULL) != 0L)
            return 154;
         return -1;
      default :
         return -1;
   }
}

int  SqlParserTokenManager::jjStartNfa_0(int pos, unsigned long long active0, unsigned long long active1, unsigned long long active2, unsigned long long active3, unsigned long long active4, unsigned long long active5, unsigned long long active6, unsigned long long active7, unsigned long long active8, unsigned long long active9){
   return jjMoveNfa_0(jjStopStringLiteralDfa_0(pos, active0, active1, active2, active3, active4, active5, active6, active7, active8, active9), pos + 1);
}

 int  SqlParserTokenManager::jjStopAtPos(int pos, int kind){
   jjmatchedKind = kind;
   jjmatchedPos = pos;
   return pos + 1;
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa0_0(){
   switch(curChar)
   {
      case 33:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x80ULL);
      case 34:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x2000000ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL);
      case 37:
         return jjStopAtPos(0, 586);
      case 40:
         return jjStopAtPos(0, 559);
      case 41:
         return jjStopAtPos(0, 560);
      case 42:
         return jjStopAtPos(0, 574);
      case 43:
         return jjStopAtPos(0, 566);
      case 44:
         return jjStopAtPos(0, 570);
      case 45:
         jjmatchedKind = 567;
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x2000000000000000ULL, 0x0ULL);
      case 46:
         return jjStartNfaWithStates_0(0, 569, 152);
      case 47:
         jjmatchedKind = 575;
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x20000000000ULL);
      case 58:
         jjmatchedKind = 568;
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x1000000000000000ULL, 0x0ULL);
      case 59:
         return jjStopAtPos(0, 558);
      case 60:
         jjmatchedKind = 579;
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x24ULL);
      case 61:
         jjmatchedKind = 577;
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x100ULL);
      case 62:
         jjmatchedKind = 580;
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x40ULL);
      case 63:
         jjmatchedKind = 571;
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x28000000000000ULL, 0x0ULL);
      case 64:
         return jjStopAtPos(0, 585);
      case 91:
         return jjStopAtPos(0, 562);
      case 93:
         return jjStopAtPos(0, 564);
      case 95:
         return jjStartNfaWithStates_0(0, 561, 1);
      case 65:
      case 97:
         jjmatchedKind = 2;
         return jjMoveStringLiteralDfa1_0(0x7ff8ULL, 0x0ULL, 0x0ULL, 0x1f00000000ULL, 0x0ULL, 0x7ff0200000ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL);
      case 66:
      case 98:
         return jjMoveStringLiteralDfa1_0(0x38000ULL, 0x0ULL, 0x0ULL, 0x6000000000ULL, 0x0ULL, 0x1f8000000000ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL);
      case 67:
      case 99:
         jjmatchedKind = 18;
         return jjMoveStringLiteralDfa1_0(0x1fffffff80000ULL, 0x0ULL, 0x0ULL, 0x3ffff8000000000ULL, 0x0ULL, 0xffffe00000004000ULL, 0xffULL, 0x0ULL, 0x0ULL, 0x0ULL);
      case 68:
      case 100:
         return jjMoveStringLiteralDfa1_0(0xfffe000000000000ULL, 0x7ULL, 0x0ULL, 0x7c00000000000000ULL, 0x0ULL, 0x8000ULL, 0xfff00ULL, 0x0ULL, 0x0ULL, 0x0ULL);
      case 69:
      case 101:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0xf8ULL, 0x0ULL, 0x8000000000000000ULL, 0x1ULL, 0x0ULL, 0xfff00000ULL, 0x0ULL, 0x0ULL, 0x0ULL);
      case 70:
      case 102:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x3f00ULL, 0x0ULL, 0x0ULL, 0x1eULL, 0x0ULL, 0x3ff00000000ULL, 0x0ULL, 0x0ULL, 0x0ULL);
      case 71:
      case 103:
         jjmatchedKind = 78;
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0xf8000ULL, 0x0ULL, 0x0ULL, 0x20ULL, 0x0ULL, 0x3c0000000000ULL, 0x0ULL, 0x0ULL, 0x0ULL);
      case 72:
      case 104:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x100000ULL, 0x0ULL, 0x0ULL, 0x1c0ULL, 0x100000ULL, 0x400000000000ULL, 0x0ULL, 0x0ULL, 0x0ULL);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x3ffe00000ULL, 0x0ULL, 0x0ULL, 0x1e00ULL, 0x0ULL, 0x1ff800000000000ULL, 0x0ULL, 0x0ULL, 0x0ULL);
      case 74:
      case 106:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x200000000000000ULL, 0x0ULL, 0x0ULL, 0x0ULL);
      case 75:
      case 107:
         jjmatchedKind = 98;
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x3800000000ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x400000000000000ULL, 0x0ULL, 0x0ULL, 0x0ULL);
      case 76:
      case 108:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x3c000000000ULL, 0x0ULL, 0x0ULL, 0x1e000ULL, 0x20000ULL, 0xf800000000000000ULL, 0x3fULL, 0x0ULL, 0x0ULL);
      case 77:
      case 109:
         jjmatchedKind = 106;
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0xff80000000000ULL, 0x0ULL, 0x0ULL, 0x7fe0000ULL, 0x1400000ULL, 0x0ULL, 0x7c0ULL, 0x0ULL, 0x0ULL);
      case 78:
      case 110:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x7ff0000000000000ULL, 0x0ULL, 0x0ULL, 0x38000000ULL, 0x40000ULL, 0x0ULL, 0x7ff800ULL, 0x0ULL, 0x0ULL);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x8000000000000000ULL, 0xffULL, 0x0ULL, 0x3c0000000ULL, 0x0ULL, 0x0ULL, 0x7ff800000ULL, 0x0ULL, 0x0ULL);
      case 80:
      case 112:
         jjmatchedKind = 136;
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x7fffe00ULL, 0x0ULL, 0x3c00000000ULL, 0x0ULL, 0x0ULL, 0x7f800000000ULL, 0x0ULL, 0x0ULL);
      case 82:
      case 114:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x7fff8000000ULL, 0x0ULL, 0x3ffc000000000ULL, 0x80000ULL, 0x0ULL, 0x7fff80000000000ULL, 0x0ULL, 0x0ULL);
      case 83:
      case 115:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0xfffff80000000000ULL, 0x7ULL, 0x1ffc000000000000ULL, 0x800000ULL, 0x0ULL, 0xf800000000000000ULL, 0x7fffULL, 0x0ULL);
      case 84:
      case 116:
         jjmatchedKind = 195;
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x7fff0ULL, 0xe000000000000000ULL, 0x7ULL, 0x0ULL, 0x0ULL, 0x7ff8000ULL, 0x0ULL);
      case 85:
      case 117:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0xff80000ULL, 0x0ULL, 0x10078ULL, 0x0ULL, 0x0ULL, 0xf8000000ULL, 0x0ULL);
      case 86:
      case 118:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x10000000ULL, 0x0ULL, 0x780ULL, 0x0ULL, 0x0ULL, 0x3f00000000ULL, 0x0ULL);
      case 87:
      case 119:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x60000000ULL, 0x0ULL, 0x800ULL, 0x0ULL, 0x0ULL, 0x1fc000000000ULL, 0x0ULL);
      case 89:
      case 121:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x3000ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL);
      case 90:
      case 122:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x80000000ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL);
      case 124:
         return jjMoveStringLiteralDfa1_0(0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x0ULL, 0x1ULL);
      default :
         return jjMoveNfa_0(8, 0);
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa1_0(unsigned long long active0, unsigned long long active1, unsigned long long active2, unsigned long long active3, unsigned long long active4, unsigned long long active5, unsigned long long active6, unsigned long long active7, unsigned long long active8, unsigned long long active9){
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(0, active0, active1, active2, active3, active4, active5, active6, active7, active8, active9);
      return 1;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 42:
         if ((active9 & 0x20000000000ULL) != 0L)
            return jjStopAtPos(1, 617);
         break;
      case 58:
         if ((active8 & 0x1000000000000000ULL) != 0L)
            return jjStopAtPos(1, 572);
         break;
      case 61:
         if ((active9 & 0x20ULL) != 0L)
            return jjStopAtPos(1, 581);
         else if ((active9 & 0x40ULL) != 0L)
            return jjStopAtPos(1, 582);
         else if ((active9 & 0x80ULL) != 0L)
            return jjStopAtPos(1, 583);
         break;
      case 62:
         if ((active8 & 0x2000000000000000ULL) != 0L)
            return jjStopAtPos(1, 573);
         else if ((active9 & 0x4ULL) != 0L)
            return jjStopAtPos(1, 578);
         else if ((active9 & 0x100ULL) != 0L)
            return jjStopAtPos(1, 584);
         break;
      case 63:
         return jjMoveStringLiteralDfa2_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x28000000000000ULL, active9, 0L);
      case 65:
      case 97:
         return jjMoveStringLiteralDfa2_0(active0, 0xe000000380000ULL, active1, 0x10384000000000ULL, active2, 0x7fe00ULL, active3, 0x1c00008000000010ULL, active4, 0x400c408022000ULL, active5, 0x3e00001400180ULL, active6, 0x7800400100100000ULL, active7, 0x8000018c0ULL, active8, 0x1f00018000ULL, active9, 0L);
      case 66:
      case 98:
         return jjMoveStringLiteralDfa2_0(active0, 0x8ULL, active1, 0x8000000000000000ULL, active2, 0L, active3, 0x100000000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L, active9, 0L);
      case 67:
      case 99:
         return jjMoveStringLiteralDfa2_0(active0, 0x10ULL, active1, 0L, active2, 0x1f80000000001ULL, active3, 0L, active4, 0x8000040000000ULL, active5, 0x2000000ULL, active6, 0L, active7, 0x800000001806000ULL, active8, 0L, active9, 0L);
      case 68:
      case 100:
         return jjMoveStringLiteralDfa2_0(active0, 0xe0L, active1, 0L, active2, 0L, active3, 0L, active4, 0x200ULL, active5, 0L, active6, 0L, active7, 0L, active8, 0L, active9, 0L);
      case 69:
      case 101:
         return jjMoveStringLiteralDfa2_0(active0, 0x3ff0000000018000ULL, active1, 0x61c1b800018000ULL, active2, 0x1fe001ff8000000ULL, active3, 0x2000000000000020ULL, active4, 0xf03f00100c4000ULL, active5, 0xc01800088b600ULL, active6, 0x840004020000ff00ULL, active7, 0x71fff87000000101ULL, active8, 0x2008000000ULL, active9, 0L);
      case 70:
      case 102:
         if ((active1 & 0x200000ULL) != 0L)
            return jjStartNfaWithStates_0(1, 85, 154);
         else if ((active7 & 0x2000000ULL) != 0L)
         {
            jjmatchedKind = 473;
            jjmatchedPos = 1;
         }
         return jjMoveStringLiteralDfa2_0(active0, 0x100ULL, active1, 0x780000000000000ULL, active2, 0L, active3, 0L, active4, 0x80000000ULL, active5, 0L, active6, 0L, active7, 0L, active8, 0L, active9, 0L);
      case 71:
      case 103:
         return jjMoveStringLiteralDfa2_0(active0, 0L, active1, 0x400000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L, active9, 0L);
      case 72:
      case 104:
         return jjMoveStringLiteralDfa2_0(active0, 0xfc00000ULL, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0x1f0000000000000ULL, active6, 0L, active7, 0L, active8, 0x1c000020000ULL, active9, 0L);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa2_0(active0, 0xc000000000000000ULL, active1, 0x2000000100300ULL, active2, 0x600000000000000ULL, active3, 0x10000040ULL, active4, 0xe000000000700002ULL, active5, 0x60000120801ULL, active6, 0x400030000ULL, active7, 0x8200000000000006ULL, active8, 0x1e0000000000ULL, active9, 0L);
      case 76:
      case 108:
         return jjMoveStringLiteralDfa2_0(active0, 0x10000200ULL, active1, 0x400ULL, active2, 0x180000ULL, active3, 0x12200000000ULL, active4, 0x100000024ULL, active5, 0x200000030000000ULL, active6, 0x800600000ULL, active7, 0L, active8, 0L, active9, 0L);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa2_0(active0, 0L, active1, 0x1800000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x1ULL, active9, 0L);
      case 78:
      case 110:
         if ((active6 & 0x800000000000ULL) != 0L)
         {
            jjmatchedKind = 431;
            jjmatchedPos = 1;
         }
         else if ((active7 & 0x8ULL) != 0L)
            return jjStartNfaWithStates_0(1, 451, 154);
         else if ((active7 & 0x4000000ULL) != 0L)
         {
            jjmatchedKind = 474;
            jjmatchedPos = 1;
         }
         return jjMoveStringLiteralDfa2_0(active0, 0L, active1, 0x1fe000008ULL, active2, 0L, active3, 0x780000ULL, active4, 0x1c00ULL, active5, 0xc0000008ULL, active6, 0xff000001800000ULL, active7, 0x8000000ULL, active8, 0x70000000ULL, active9, 0L);
      case 79:
      case 111:
         if ((active1 & 0x20000ULL) != 0L)
         {
            jjmatchedKind = 81;
            jjmatchedPos = 1;
         }
         else if ((active7 & 0x8000ULL) != 0L)
         {
            jjmatchedKind = 463;
            jjmatchedPos = 1;
         }
         else if ((active8 & 0x40000ULL) != 0L)
         {
            jjmatchedKind = 530;
            jjmatchedPos = 1;
         }
         return jjMoveStringLiteralDfa2_0(active0, 0xffffe0000000ULL, active1, 0x804020000043801ULL, active2, 0x80007e000000000ULL, active3, 0xe40a0000080ULL, active4, 0x3c018278181c0ULL, active5, 0xfc00080000004000ULL, active6, 0x20000700004001fULL, active7, 0x400008000030230ULL, active8, 0x2ULL, active9, 0L);
      case 80:
      case 112:
         return jjMoveStringLiteralDfa2_0(active0, 0L, active1, 0L, active2, 0x3000000000000006ULL, active3, 0L, active4, 0x200000000ULL, active5, 0x200030ULL, active6, 0L, active7, 0L, active8, 0xcULL, active9, 0L);
      case 81:
      case 113:
         return jjMoveStringLiteralDfa2_0(active0, 0L, active1, 0x10ULL, active2, 0L, active3, 0L, active4, 0x100000000000000ULL, active5, 0L, active6, 0L, active7, 0L, active8, 0xf0ULL, active9, 0L);
      case 82:
      case 114:
         if ((active7 & 0x10000000ULL) != 0L)
         {
            jjmatchedKind = 476;
            jjmatchedPos = 1;
         }
         return jjMoveStringLiteralDfa2_0(active0, 0x20000ULL, active1, 0x80000ULL, active2, 0x3e00018ULL, active3, 0x44003ff00ULL, active4, 0x2000000008ULL, active5, 0x300000006ULL, active6, 0x388000080060ULL, active7, 0x70020000000ULL, active8, 0x7f80000ULL, active9, 0L);
      case 83:
      case 115:
         if ((active5 & 0x400000000ULL) != 0L)
         {
            jjmatchedKind = 354;
            jjmatchedPos = 1;
         }
         else if ((active6 & 0x100000000000000ULL) != 0L)
         {
            jjmatchedKind = 440;
            jjmatchedPos = 1;
         }
         return jjMoveStringLiteralDfa2_0(active0, 0x1c00ULL, active1, 0x200000000ULL, active2, 0L, active3, 0xf800000ULL, active4, 0L, active5, 0x1800010040ULL, active6, 0x2000000ULL, active7, 0L, active8, 0x80000000ULL, active9, 0L);
      case 84:
      case 116:
         if ((active3 & 0x800000000ULL) != 0L)
         {
            jjmatchedKind = 227;
            jjmatchedPos = 1;
         }
         return jjMoveStringLiteralDfa2_0(active0, 0x6000ULL, active1, 0L, active2, 0xc000000000000020ULL, active3, 0x3ULL, active4, 0x600000000000000ULL, active5, 0x2000000000ULL, active6, 0L, active7, 0xc0000ULL, active8, 0x300ULL, active9, 0L);
      case 85:
      case 117:
         return jjMoveStringLiteralDfa2_0(active0, 0x1000000000000ULL, active1, 0x7008000000000000ULL, active2, 0x4000040ULL, active3, 0x1fff00000000004ULL, active4, 0x800000000000010ULL, active5, 0x4000040000ULL, active6, 0x30000000080ULL, active7, 0xc0700400ULL, active8, 0x1c00ULL, active9, 0L);
      case 86:
      case 118:
         return jjMoveStringLiteralDfa2_0(active0, 0L, active1, 0L, active2, 0x80ULL, active3, 0x1000000000ULL, active4, 0L, active5, 0L, active6, 0x4000000ULL, active7, 0x700000000ULL, active8, 0L, active9, 0L);
      case 88:
      case 120:
         return jjMoveStringLiteralDfa2_0(active0, 0L, active1, 0xe0ULL, active2, 0L, active3, 0x8000000000000000ULL, active4, 0x1ULL, active5, 0L, active6, 0xf8000000ULL, active7, 0L, active8, 0L, active9, 0L);
      case 89:
      case 121:
         if ((active5 & 0x100000000000ULL) != 0L)
            return jjStartNfaWithStates_0(1, 364, 154);
         return jjMoveStringLiteralDfa2_0(active0, 0L, active1, 0x6ULL, active2, 0L, active3, 0x4200000000040000ULL, active4, 0x1000000000000000ULL, active5, 0L, active6, 0L, active7, 0L, active8, 0x6000ULL, active9, 0L);
      case 124:
         if ((active9 & 0x1ULL) != 0L)
            return jjStopAtPos(1, 576);
         break;
      default :
         break;
   }
   return jjStartNfa_0(0, active0, active1, active2, active3, active4, active5, active6, active7, active8, active9);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa2_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8, unsigned long long old9, unsigned long long active9){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active4 &= old4) | (active5 &= old5) | (active6 &= old6) | (active7 &= old7) | (active8 &= old8) | (active9 &= old9)) == 0L)
      return jjStartNfa_0(0, old0, old1, old2, old3, old4, old5, old6, old7, old8, old9);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(1, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
      return 2;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 40:
         if ((active8 & 0x8000000000000ULL) != 0L)
            return jjStopAtPos(2, 563);
         break;
      case 41:
         if ((active8 & 0x20000000000000ULL) != 0L)
            return jjStopAtPos(2, 565);
         break;
      case 65:
      case 97:
         if ((active0 & 0x20ULL) != 0L)
            return jjStartNfaWithStates_0(2, 5, 154);
         return jjMoveStringLiteralDfa3_0(active0, 0x400000001fc00000ULL, active1, 0x80400ULL, active2, 0xd000080008080000ULL, active3, 0x803f00ULL, active4, 0x610010000004000ULL, active5, 0x4f0000000003000ULL, active6, 0x8000080000000100ULL, active7, 0x80000000000ULL, active8, 0x780001ULL);
      case 66:
      case 98:
         return jjMoveStringLiteralDfa3_0(active0, 0x20000000ULL, active1, 0L, active2, 0x4000000ULL, active3, 0x100000080014ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x19c00ULL);
      case 67:
      case 99:
         if ((active0 & 0x400ULL) != 0L)
            return jjStartNfaWithStates_0(2, 10, 154);
         else if ((active1 & 0x80000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(2, 119, 154);
         else if ((active3 & 0x2000000000000000ULL) != 0L)
         {
            jjmatchedKind = 253;
            jjmatchedPos = 2;
         }
         return jjMoveStringLiteralDfa3_0(active0, 0L, active1, 0x20006000060ULL, active2, 0x6000000000000ULL, active3, 0x200000000100000ULL, active4, 0x60000040008000ULL, active5, 0L, active6, 0xa100600ULL, active7, 0x100001000030ULL, active8, 0L);
      case 68:
      case 100:
         if ((active0 & 0x40ULL) != 0L)
            return jjStartNfaWithStates_0(2, 6, 154);
         else if ((active1 & 0x100000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(2, 120, 154);
         else if ((active2 & 0x200ULL) != 0L)
            return jjStartNfaWithStates_0(2, 137, 154);
         else if ((active4 & 0x800000ULL) != 0L)
         {
            jjmatchedKind = 279;
            jjmatchedPos = 2;
         }
         else if ((active4 & 0x100000000ULL) != 0L)
            return jjStartNfaWithStates_0(2, 288, 154);
         else if ((active5 & 0x40000000ULL) != 0L)
            return jjStartNfaWithStates_0(2, 350, 154);
         else if ((active6 & 0x800000ULL) != 0L)
         {
            jjmatchedKind = 407;
            jjmatchedPos = 2;
         }
         return jjMoveStringLiteralDfa3_0(active0, 0L, active1, 0L, active2, 0x18ULL, active3, 0x200000ULL, active4, 0x1000400ULL, active5, 0x10ULL, active6, 0x1000000ULL, active7, 0x20000200ULL, active8, 0x20000000300ULL);
      case 69:
      case 101:
         if ((active5 & 0x10000ULL) != 0L)
         {
            jjmatchedKind = 336;
            jjmatchedPos = 2;
         }
         else if ((active5 & 0x100000000ULL) != 0L)
            return jjStartNfaWithStates_0(2, 352, 154);
         return jjMoveStringLiteralDfa3_0(active0, 0x20000ULL, active1, 0x100000ULL, active2, 0x2000000000600080ULL, active3, 0x1f000040ULL, active4, 0x2200000208ULL, active5, 0x100000800000040ULL, active6, 0x400000034200020ULL, active7, 0x10700000000ULL, active8, 0x1c00082000cULL);
      case 70:
      case 102:
         if ((active4 & 0x20000000000ULL) != 0L)
         {
            jjmatchedKind = 297;
            jjmatchedPos = 2;
         }
         return jjMoveStringLiteralDfa3_0(active0, 0x1f0000000008000ULL, active1, 0x8ULL, active2, 0L, active3, 0L, active4, 0x40080000000ULL, active5, 0x8000ULL, active6, 0L, active7, 0x200000000001ULL, active8, 0L);
      case 71:
      case 103:
         if ((active3 & 0x1000000000ULL) != 0L)
            return jjStartNfaWithStates_0(2, 228, 154);
         else if ((active6 & 0x800000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(2, 443, 154);
         return jjMoveStringLiteralDfa3_0(active0, 0x200000000000000ULL, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0x28000000000ULL, active6, 0L, active7, 0x27fc00000000000ULL, active8, 0L);
      case 72:
      case 104:
         return jjMoveStringLiteralDfa3_0(active0, 0L, active1, 0L, active2, 0x300000000020ULL, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x42000ULL, active8, 0L);
      case 73:
      case 105:
         if ((active2 & 0x100000ULL) != 0L)
            return jjStartNfaWithStates_0(2, 148, 154);
         return jjMoveStringLiteralDfa3_0(active0, 0L, active1, 0x8000000ULL, active2, 0x1800000ULL, active3, 0x4001c000ULL, active4, 0L, active5, 0xc000000000002ULL, active6, 0x200000040000000ULL, active7, 0x20000080000ULL, active8, 0xb3000000ULL);
      case 74:
      case 106:
         return jjMoveStringLiteralDfa3_0(active0, 0L, active1, 0x8000000000000000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 75:
      case 107:
         return jjMoveStringLiteralDfa3_0(active0, 0L, active1, 0x600000000000000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0x8ULL, active6, 0L, active7, 0x6ULL, active8, 0L);
      case 76:
      case 108:
         if ((active3 & 0x200000000ULL) != 0L)
         {
            jjmatchedKind = 225;
            jjmatchedPos = 2;
         }
         else if ((active4 & 0x100000000000000ULL) != 0L)
         {
            jjmatchedKind = 312;
            jjmatchedPos = 2;
         }
         return jjMoveStringLiteralDfa3_0(active0, 0x7c0000000ULL, active1, 0x3000000000000800ULL, active2, 0x8002010000000ULL, active3, 0x20000000000ULL, active4, 0x480000000042ULL, active5, 0x1800600010000180ULL, active6, 0x10100000800ULL, active7, 0x1400000008304400ULL, active8, 0x70ULL);
      case 77:
      case 109:
         if ((active4 & 0x800000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(2, 315, 154);
         return jjMoveStringLiteralDfa3_0(active0, 0x3800000080ULL, active1, 0x4018000000800001ULL, active2, 0x200000000000000ULL, active3, 0x20ULL, active4, 0xe000000008000000ULL, active5, 0x2000000000064001ULL, active6, 0x80ULL, active7, 0x8000000000400100ULL, active8, 0x2002ULL);
      case 78:
      case 110:
         if ((active4 & 0x100000ULL) != 0L)
         {
            jjmatchedKind = 276;
            jjmatchedPos = 2;
         }
         return jjMoveStringLiteralDfa3_0(active0, 0xffc000000000ULL, active1, 0x2008000418106ULL, active2, 0L, active3, 0x4000040080400000ULL, active4, 0xc026602010ULL, active5, 0xc000040000000800ULL, active6, 0x1000000001001ULL, active7, 0x2000000000000000ULL, active8, 0x40000000ULL);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa3_0(active0, 0L, active1, 0x200000000ULL, active2, 0x1c00002000000ULL, active3, 0x12000000000ULL, active4, 0x8000000000024ULL, active5, 0x200082002000000ULL, active6, 0x2308800080040ULL, active7, 0x40000000000ULL, active8, 0L);
      case 80:
      case 112:
         if ((active1 & 0x80000000000ULL) != 0L)
         {
            jjmatchedKind = 107;
            jjmatchedPos = 2;
         }
         else if ((active3 & 0x8000000000000000ULL) != 0L)
         {
            jjmatchedKind = 255;
            jjmatchedPos = 2;
         }
         return jjMoveStringLiteralDfa3_0(active0, 0x400000000000000ULL, active1, 0x11000080ULL, active2, 0x20000000ULL, active3, 0x40080ULL, active4, 0L, active5, 0x1680020ULL, active6, 0L, active7, 0L, active8, 0L);
      case 81:
      case 113:
         return jjMoveStringLiteralDfa3_0(active0, 0L, active1, 0L, active2, 0x10000000000000ULL, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 82:
      case 114:
         if ((active6 & 0x1000000000ULL) != 0L)
         {
            jjmatchedKind = 420;
            jjmatchedPos = 2;
         }
         return jjMoveStringLiteralDfa3_0(active0, 0x801000000010000ULL, active1, 0x804000000001200ULL, active2, 0x6000000001fc00ULL, active3, 0x1ffe08420000001ULL, active4, 0x400040000ULL, active5, 0x200000600ULL, active6, 0x1000006400002006ULL, active7, 0x800007800010000ULL, active8, 0x3f00000080ULL);
      case 83:
      case 115:
         if ((active3 & 0x100000000ULL) != 0L)
         {
            jjmatchedKind = 224;
            jjmatchedPos = 2;
         }
         return jjMoveStringLiteralDfa3_0(active0, 0xb000000000081808ULL, active1, 0x21c040e0000000ULL, active2, 0x800001c0020000ULL, active3, 0L, active4, 0x1080100800000000ULL, active5, 0x3800000100000ULL, active6, 0x200c020000434000ULL, active7, 0x8000000000ULL, active8, 0x8004000ULL);
      case 84:
      case 116:
         if ((active6 & 0x40000000000ULL) != 0L)
            return jjStartNfaWithStates_0(2, 426, 154);
         else if ((active6 & 0x10000000000000ULL) != 0L)
         {
            jjmatchedKind = 436;
            jjmatchedPos = 2;
         }
         else if ((active7 & 0x20000ULL) != 0L)
            return jjStartNfaWithStates_0(2, 465, 154);
         else if ((active7 & 0x40000000ULL) != 0L)
         {
            jjmatchedKind = 478;
            jjmatchedPos = 2;
         }
         else if ((active7 & 0x4000000000000000ULL) != 0L)
         {
            jjmatchedKind = 510;
            jjmatchedPos = 2;
         }
         return jjMoveStringLiteralDfa3_0(active0, 0xe000000306110ULL, active1, 0x100000040000ULL, active2, 0x100001e00040047ULL, active3, 0x400004000000000ULL, active4, 0x200000081801ULL, active5, 0x14020800000ULL, active6, 0x40e0000280008000ULL, active7, 0x80000080801840ULL, active8, 0x1c0000000000ULL);
      case 85:
      case 117:
         return jjMoveStringLiteralDfa3_0(active0, 0L, active1, 0x2010ULL, active2, 0x80003c000000000ULL, active3, 0x80000000000ULL, active4, 0x180ULL, active5, 0x4ULL, active6, 0x40000ULL, active7, 0L, active8, 0x4000000ULL);
      case 86:
      case 118:
         return jjMoveStringLiteralDfa3_0(active0, 0L, active1, 0x10100000000ULL, active2, 0L, active3, 0L, active4, 0x4000000000000ULL, active5, 0L, active6, 0x400000000018ULL, active7, 0x100000000000000ULL, active8, 0L);
      case 87:
      case 119:
         if ((active4 & 0x10000000ULL) != 0L)
            return jjStartNfaWithStates_0(2, 284, 154);
         else if ((active4 & 0x800000000000ULL) != 0L)
         {
            jjmatchedKind = 303;
            jjmatchedPos = 2;
         }
         return jjMoveStringLiteralDfa3_0(active0, 0x200ULL, active1, 0L, active2, 0x40000000000ULL, active3, 0L, active4, 0x3001000010000ULL, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 88:
      case 120:
         if ((active4 & 0x20000ULL) != 0L)
         {
            jjmatchedKind = 273;
            jjmatchedPos = 2;
         }
         return jjMoveStringLiteralDfa3_0(active0, 0L, active1, 0x40200000000000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x80ULL, active8, 0L);
      case 89:
      case 121:
         if ((active1 & 0x800000000ULL) != 0L)
         {
            jjmatchedKind = 99;
            jjmatchedPos = 2;
         }
         else if ((active3 & 0x800000000000000ULL) != 0L)
         {
            jjmatchedKind = 251;
            jjmatchedPos = 2;
         }
         else if ((active5 & 0x80000000ULL) != 0L)
            return jjStartNfaWithStates_0(2, 351, 154);
         return jjMoveStringLiteralDfa3_0(active0, 0L, active1, 0x3000000000ULL, active2, 0L, active3, 0x1000000000020002ULL, active4, 0L, active5, 0x1000000000ULL, active6, 0L, active7, 0L, active8, 0L);
      case 90:
      case 122:
         return jjMoveStringLiteralDfa3_0(active0, 0L, active1, 0L, active2, 0x400000000000000ULL, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      default :
         break;
   }
   return jjStartNfa_0(1, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa3_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active4 &= old4) | (active5 &= old5) | (active6 &= old6) | (active7 &= old7) | (active8 &= old8)) == 0L)
      return jjStartNfa_0(1, old0, old1, old2, old3, old4, old5, old6, old7, old8, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(2, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
      return 3;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 45:
         return jjMoveStringLiteralDfa4_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0x1000000ULL, active7, 0L, active8, 0L);
      case 95:
         return jjMoveStringLiteralDfa4_0(active0, 0L, active1, 0x3000000000ULL, active2, 0x40000000000ULL, active3, 0x20080ULL, active4, 0x1000000000000ULL, active5, 0x1c00000ULL, active6, 0L, active7, 0x40080ULL, active8, 0x300000000ULL);
      case 65:
      case 97:
         if ((active0 & 0x2000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 49, 154);
         return jjMoveStringLiteralDfa4_0(active0, 0x10000000320200ULL, active1, 0x20000000117ULL, active2, 0x1000fc00ULL, active3, 0x4000000400400000ULL, active4, 0x8000ULL, active5, 0x40200008010ULL, active6, 0x802000038ULL, active7, 0x800002030ULL, active8, 0x800000ULL);
      case 66:
      case 98:
         if ((active3 & 0x2000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 229, 154);
         else if ((active5 & 0x200000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 377, 154);
         return jjMoveStringLiteralDfa4_0(active0, 0L, active1, 0x4000000000000000ULL, active2, 0L, active3, 0L, active4, 0x20ULL, active5, 0L, active6, 0x40000ULL, active7, 0x100ULL, active8, 0x400000000ULL);
      case 67:
      case 99:
         if ((active0 & 0x1000000000000000ULL) != 0L)
         {
            jjmatchedKind = 60;
            jjmatchedPos = 3;
         }
         else if ((active1 & 0x200000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 121, 154);
         else if ((active6 & 0x10000000ULL) != 0L)
         {
            jjmatchedKind = 412;
            jjmatchedPos = 3;
         }
         return jjMoveStringLiteralDfa4_0(active0, 0x2000000000080000ULL, active1, 0x100000000000ULL, active2, 0x30000000002a0000ULL, active3, 0x4ULL, active4, 0x2000000010ULL, active5, 0x100800000000000ULL, active6, 0x220014000ULL, active7, 0x47000000040ULL, active8, 0x80800000cULL);
      case 68:
      case 100:
         if ((active1 & 0x400000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 122, 154);
         else if ((active2 & 0x8000000ULL) != 0L)
         {
            jjmatchedKind = 155;
            jjmatchedPos = 3;
         }
         else if ((active4 & 0x40ULL) != 0L)
            return jjStartNfaWithStates_0(3, 262, 154);
         else if ((active4 & 0x4000ULL) != 0L)
         {
            jjmatchedKind = 270;
            jjmatchedPos = 3;
         }
         return jjMoveStringLiteralDfa4_0(active0, 0x4000000000ULL, active1, 0L, active2, 0L, active3, 0x48000000000ULL, active4, 0x10000000000ULL, active5, 0x800ULL, active6, 0x8000000000000000ULL, active7, 0L, active8, 0x300ULL);
      case 69:
      case 101:
         if ((active1 & 0x4000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 114, 154);
         else if ((active2 & 0x2000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 165, 154);
         else if ((active2 & 0x400000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 186, 154);
         else if ((active3 & 0x40000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 210, 154);
         else if ((active3 & 0x80000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 223, 154);
         else if ((active3 & 0x100000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 236, 154);
         else if ((active3 & 0x400000000000000ULL) != 0L)
         {
            jjmatchedKind = 250;
            jjmatchedPos = 3;
         }
         else if ((active4 & 0x8ULL) != 0L)
            return jjStartNfaWithStates_0(3, 259, 154);
         else if ((active4 & 0x8000000ULL) != 0L)
         {
            jjmatchedKind = 283;
            jjmatchedPos = 3;
         }
         else if ((active4 & 0x20000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 285, 154);
         else if ((active4 & 0x2000000000000000ULL) != 0L)
         {
            jjmatchedKind = 317;
            jjmatchedPos = 3;
         }
         else if ((active5 & 0x1000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 368, 154);
         else if ((active6 & 0x400000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 406, 154);
         else if ((active7 & 0x2ULL) != 0L)
         {
            jjmatchedKind = 449;
            jjmatchedPos = 3;
         }
         else if ((active8 & 0x2ULL) != 0L)
            return jjStartNfaWithStates_0(3, 513, 154);
         else if ((active8 & 0x4000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 538, 154);
         return jjMoveStringLiteralDfa4_0(active0, 0x6c000000000900ULL, active1, 0x8010010000818000ULL, active2, 0x300020000029ULL, active3, 0x200000ULL, active4, 0xc0040c1000011801ULL, active5, 0x20040021ULL, active6, 0x406d00600800a880ULL, active7, 0x10002000a0c00004ULL, active8, 0x40000010ULL);
      case 70:
      case 102:
         if ((active2 & 0x8000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 179, 154);
         break;
      case 71:
      case 103:
         if ((active1 & 0x400ULL) != 0L)
            return jjStartNfaWithStates_0(3, 74, 154);
         return jjMoveStringLiteralDfa4_0(active0, 0x4000000000000000ULL, active1, 0x8000000000ULL, active2, 0L, active3, 0x81c000ULL, active4, 0x4000042000ULL, active5, 0x2ULL, active6, 0x1000000000000000ULL, active7, 0L, active8, 0L);
      case 72:
      case 104:
         if ((active2 & 0x40000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 146, 154);
         else if ((active3 & 0x4000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 230, 154);
         else if ((active6 & 0x100000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 404, 154);
         else if ((active8 & 0x40000000000ULL) != 0L)
         {
            jjmatchedKind = 554;
            jjmatchedPos = 3;
         }
         return jjMoveStringLiteralDfa4_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0x80000ULL, active5, 0x4000000000ULL, active6, 0L, active7, 0x200000000000000ULL, active8, 0x180000000000ULL);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa4_0(active0, 0x980000000401090ULL, active1, 0L, active2, 0x20000000000016ULL, active3, 0L, active4, 0x800000400ULL, active5, 0x28000020000ULL, active6, 0x420000000200ULL, active7, 0x8000008000000a00ULL, active8, 0x80000ULL);
      case 75:
      case 107:
         if ((active3 & 0x20000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 221, 154);
         else if ((active4 & 0x8000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 295, 154);
         break;
      case 76:
      case 108:
         if ((active5 & 0x200000000000ULL) != 0L)
         {
            jjmatchedKind = 365;
            jjmatchedPos = 3;
         }
         else if ((active5 & 0x4000000000000ULL) != 0L)
         {
            jjmatchedKind = 370;
            jjmatchedPos = 3;
         }
         else if ((active6 & 0x10000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 424, 154);
         else if ((active7 & 0x100000ULL) != 0L)
         {
            jjmatchedKind = 468;
            jjmatchedPos = 3;
         }
         else if ((active7 & 0x80000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 491, 154);
         return jjMoveStringLiteralDfa4_0(active0, 0x3c0000000ULL, active1, 0x3000000203000860ULL, active2, 0x80004000000ULL, active3, 0x200000000000012ULL, active4, 0x400000000000ULL, active5, 0x1c08480000080000ULL, active6, 0x500ULL, active7, 0x400000000280000ULL, active8, 0x18001ULL);
      case 77:
      case 109:
         if ((active6 & 0x8000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 423, 154);
         else if ((active8 & 0x1000000ULL) != 0L)
         {
            jjmatchedKind = 536;
            jjmatchedPos = 3;
         }
         return jjMoveStringLiteralDfa4_0(active0, 0x3800000000ULL, active1, 0x800000000000000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0x2000003000004000ULL, active6, 0x200000ULL, active7, 0x20000010000ULL, active8, 0x2002400ULL);
      case 78:
      case 110:
         if ((active4 & 0x200000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 289, 154);
         else if ((active6 & 0x200000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 441, 154);
         else if ((active8 & 0x20000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 529, 154);
         else if ((active8 & 0x4000000000ULL) != 0L)
         {
            jjmatchedKind = 550;
            jjmatchedPos = 3;
         }
         return jjMoveStringLiteralDfa4_0(active0, 0x18000010000ULL, active1, 0x82000ULL, active2, 0L, active3, 0x80000003f00ULL, active4, 0x200ULL, active5, 0x400000080000000cULL, active6, 0x80000000000ULL, active7, 0L, active8, 0x8080700000ULL);
      case 79:
      case 111:
         if ((active1 & 0x40000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 82, 154);
         else if ((active6 & 0x80000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 439, 154);
         return jjMoveStringLiteralDfa4_0(active0, 0x20008008ULL, active1, 0x100400008ULL, active2, 0x800000ULL, active3, 0x180000ULL, active4, 0x60000000000004ULL, active5, 0x10000000ULL, active6, 0L, active7, 0x900000000004000ULL, active8, 0x10000000ULL);
      case 80:
      case 112:
         if ((active6 & 0x80000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 403, 154);
         else if ((active6 & 0x400000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 442, 154);
         return jjMoveStringLiteralDfa4_0(active0, 0x8000000000000000ULL, active1, 0x8000000000000ULL, active2, 0x201c00042000040ULL, active3, 0x20ULL, active4, 0x8000000000000ULL, active5, 0L, active6, 0L, active7, 0x10000000000ULL, active8, 0L);
      case 81:
      case 113:
         return jjMoveStringLiteralDfa4_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x20000000ULL);
      case 82:
      case 114:
         if ((active4 & 0x80ULL) != 0L)
         {
            jjmatchedKind = 263;
            jjmatchedPos = 3;
         }
         else if ((active5 & 0x40ULL) != 0L)
         {
            jjmatchedKind = 326;
            jjmatchedPos = 3;
         }
         else if ((active5 & 0x1000ULL) != 0L)
         {
            jjmatchedKind = 332;
            jjmatchedPos = 3;
         }
         else if ((active5 & 0x10000000000000ULL) != 0L)
         {
            jjmatchedKind = 372;
            jjmatchedPos = 3;
         }
         else if ((active6 & 0x2ULL) != 0L)
         {
            jjmatchedKind = 385;
            jjmatchedPos = 3;
         }
         else if ((active7 & 0x100000000ULL) != 0L)
         {
            jjmatchedKind = 480;
            jjmatchedPos = 3;
         }
         return jjMoveStringLiteralDfa4_0(active0, 0x20000000f806000ULL, active1, 0x4100080ULL, active2, 0x800000000000080ULL, active3, 0xffe0000f000000ULL, active4, 0x210000000000100ULL, active5, 0xe0000000202000ULL, active6, 0x84000004ULL, active7, 0x7fc00600000000ULL, active8, 0x10000000000ULL);
      case 83:
      case 115:
         if ((active2 & 0x100000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 184, 154);
         else if ((active3 & 0x40ULL) != 0L)
            return jjStartNfaWithStates_0(3, 198, 154);
         else if ((active3 & 0x1000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 252, 154);
         else if ((active4 & 0x2000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 305, 154);
         return jjMoveStringLiteralDfa4_0(active0, 0x13e0010000000ULL, active1, 0x1c00000000200ULL, active2, 0x80000000400000ULL, active3, 0x100010000000000ULL, active4, 0x80000080000000ULL, active5, 0x8000000000000600ULL, active6, 0x540001040ULL, active7, 0x2000000000000000ULL, active8, 0x2000001820ULL);
      case 84:
      case 116:
         if ((active1 & 0x4000000000ULL) != 0L)
         {
            jjmatchedKind = 102;
            jjmatchedPos = 3;
         }
         else if ((active1 & 0x40000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 118, 154);
         else if ((active5 & 0x2000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 369, 154);
         else if ((active7 & 0x1ULL) != 0L)
            return jjStartNfaWithStates_0(3, 448, 154);
         else if ((active8 & 0x80ULL) != 0L)
            return jjStartNfaWithStates_0(3, 519, 154);
         return jjMoveStringLiteralDfa4_0(active0, 0x400c00000000000ULL, active1, 0x200000e8001000ULL, active2, 0xc00203c180010000ULL, active3, 0x40000000ULL, active4, 0x1400000406000002ULL, active5, 0x100000ULL, active6, 0x2000000000020000ULL, active7, 0x400ULL, active8, 0x20000004000ULL);
      case 85:
      case 117:
         return jjMoveStringLiteralDfa4_0(active0, 0x400000000ULL, active1, 0x10000000ULL, active2, 0x14001e00000000ULL, active3, 0x20000000001ULL, active4, 0x300041600000ULL, active5, 0x2000180ULL, active6, 0x2300000000000ULL, active7, 0x80100001001000ULL, active8, 0L);
      case 86:
      case 118:
         return jjMoveStringLiteralDfa4_0(active0, 0L, active1, 0x2200000000000ULL, active2, 0x40000001000000ULL, active3, 0L, active4, 0L, active5, 0L, active6, 0x1ULL, active7, 0L, active8, 0L);
      case 87:
      case 119:
         if ((active3 & 0x10000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 220, 154);
         return jjMoveStringLiteralDfa4_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0x10000000000ULL, active6, 0L, active7, 0L, active8, 0x40ULL);
      case 89:
      case 121:
         if ((active7 & 0x8000000ULL) != 0L)
            return jjStartNfaWithStates_0(3, 475, 154);
         return jjMoveStringLiteralDfa4_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x1000000000ULL);
      default :
         break;
   }
   return jjStartNfa_0(2, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa4_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active4 &= old4) | (active5 &= old5) | (active6 &= old6) | (active7 &= old7) | (active8 &= old8)) == 0L)
      return jjStartNfa_0(2, old0, old1, old2, old3, old4, old5, old6, old7, old8, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(3, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
      return 4;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa5_0(active0, 0L, active1, 0L, active2, 0L, active3, 0xf000000ULL, active4, 0L, active5, 0x20000000000000ULL, active6, 0x2000000000000080ULL, active7, 0x7fc00000000004ULL, active8, 0x2000000ULL);
      case 65:
      case 97:
         return jjMoveStringLiteralDfa5_0(active0, 0x8000401bcf880000ULL, active1, 0x1803e00260100000ULL, active2, 0x200000a0020000ULL, active3, 0L, active4, 0x80000000020ULL, active5, 0x8c0800000c80000ULL, active6, 0x80000400ULL, active7, 0x30000010000ULL, active8, 0x8000040ULL);
      case 66:
      case 98:
         if ((active7 & 0x4000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 462, 154);
         return jjMoveStringLiteralDfa5_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x400000000000000ULL, active8, 0L);
      case 67:
      case 99:
         return jjMoveStringLiteralDfa5_0(active0, 0L, active1, 0x8000000000000000ULL, active2, 0x800040000000000ULL, active3, 0x20001ULL, active4, 0x10000000000400ULL, active5, 0x10000004ULL, active6, 0L, active7, 0x1000000000000080ULL, active8, 0L);
      case 68:
      case 100:
         if ((active1 & 0x2000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 77, 154);
         return jjMoveStringLiteralDfa5_0(active0, 0x20000ULL, active1, 0x800000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 69:
      case 101:
         if ((active2 & 0x80000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 171, 154);
         else if ((active2 & 0x1000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 188, 154);
         else if ((active2 & 0x4000000000000000ULL) != 0L)
         {
            jjmatchedKind = 190;
            jjmatchedPos = 4;
         }
         else if ((active3 & 0x2ULL) != 0L)
            return jjStartNfaWithStates_0(4, 193, 154);
         else if ((active3 & 0x800000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 215, 154);
         else if ((active3 & 0x40000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 222, 154);
         else if ((active3 & 0x10000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 232, 154);
         else if ((active3 & 0x200000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 249, 154);
         else if ((active4 & 0x40000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 274, 154);
         else if ((active4 & 0x4000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 294, 154);
         else if ((active4 & 0x8000000000000ULL) != 0L)
         {
            jjmatchedKind = 307;
            jjmatchedPos = 4;
         }
         else if ((active5 & 0x80ULL) != 0L)
         {
            jjmatchedKind = 327;
            jjmatchedPos = 4;
         }
         else if ((active6 & 0x100000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 416, 154);
         else if ((active6 & 0x1000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 444, 154);
         else if ((active7 & 0x80000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 467, 154);
         else if ((active8 & 0x8000ULL) != 0L)
         {
            jjmatchedKind = 527;
            jjmatchedPos = 4;
         }
         else if ((active8 & 0x10000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 552, 154);
         return jjMoveStringLiteralDfa5_0(active0, 0x200018000000000ULL, active1, 0x4000000085000080ULL, active2, 0x8051c00042600000ULL, active3, 0xffe00000000010ULL, active4, 0x1000000080000002ULL, active5, 0x5400490000004100ULL, active6, 0x1201005ULL, active7, 0x47000000100ULL, active8, 0x8000016300ULL);
      case 70:
      case 102:
         if ((active6 & 0x2000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 397, 154);
         return jjMoveStringLiteralDfa5_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x200ULL, active8, 0L);
      case 71:
      case 103:
         if ((active8 & 0x80000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 543, 154);
         return jjMoveStringLiteralDfa5_0(active0, 0x1000ULL, active1, 0L, active2, 0L, active3, 0x1c000ULL, active4, 0L, active5, 0x2ULL, active6, 0x20000000000000ULL, active7, 0L, active8, 0L);
      case 72:
      case 104:
         if ((active0 & 0x400000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 58, 154);
         else if ((active4 & 0x2000000ULL) != 0L)
         {
            jjmatchedKind = 281;
            jjmatchedPos = 4;
         }
         else if ((active6 & 0x200000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 417, 154);
         else if ((active7 & 0x40ULL) != 0L)
         {
            jjmatchedKind = 454;
            jjmatchedPos = 4;
         }
         return jjMoveStringLiteralDfa5_0(active0, 0L, active1, 0x100000000000ULL, active2, 0L, active3, 0L, active4, 0x4000000ULL, active5, 0L, active6, 0L, active7, 0L, active8, 0x20800000000ULL);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa5_0(active0, 0x806000006000ULL, active1, 0x20000008000001ULL, active2, 0x208203c005090000ULL, active3, 0x48000000000ULL, active4, 0x480002400000000ULL, active5, 0x2008002000000600ULL, active6, 0x8000002000020000ULL, active7, 0x2000000000200400ULL, active8, 0x8340000000cULL);
      case 75:
      case 107:
         if ((active5 & 0x100000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 376, 154);
         return jjMoveStringLiteralDfa5_0(active0, 0L, active1, 0x100000000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x100000000000000ULL, active8, 0L);
      case 76:
      case 108:
         if ((active0 & 0x20000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 29, 154);
         else if ((active1 & 0x100ULL) != 0L)
            return jjStartNfaWithStates_0(4, 72, 154);
         else if ((active1 & 0x10000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 104, 154);
         else if ((active4 & 0x8000ULL) != 0L)
         {
            jjmatchedKind = 271;
            jjmatchedPos = 4;
         }
         return jjMoveStringLiteralDfa5_0(active0, 0x300008ULL, active1, 0x10ULL, active2, 0x200000000000000ULL, active3, 0x84ULL, active4, 0x100001000000ULL, active5, 0L, active6, 0x40100ULL, active7, 0x8800000600000030ULL, active8, 0x80001ULL);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa5_0(active0, 0x400000000ULL, active1, 0x1000000006ULL, active2, 0x30000000fc00ULL, active3, 0x4000020000500000ULL, active4, 0L, active5, 0x1000000000ULL, active6, 0x200ULL, active7, 0x800000000ULL, active8, 0L);
      case 78:
      case 110:
         if ((active0 & 0x80ULL) != 0L)
            return jjStartNfaWithStates_0(4, 7, 154);
         else if ((active0 & 0x400000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 22, 154);
         else if ((active5 & 0x8000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 359, 154);
         else if ((active8 & 0x10000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 540, 154);
         return jjMoveStringLiteralDfa5_0(active0, 0x4180000000000000ULL, active1, 0L, active2, 0x10ULL, active3, 0L, active4, 0x61000000000000ULL, active5, 0x20002000000ULL, active6, 0x4400000000000ULL, active7, 0L, active8, 0L);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa5_0(active0, 0x1000000010010ULL, active1, 0x800ULL, active2, 0x6ULL, active3, 0x100000000000020ULL, active4, 0x80000ULL, active5, 0x4000300808ULL, active6, 0x20000010000ULL, active7, 0x800ULL, active8, 0x100000000000ULL);
      case 80:
      case 112:
         if ((active6 & 0x100000000000ULL) != 0L)
         {
            jjmatchedKind = 428;
            jjmatchedPos = 4;
         }
         return jjMoveStringLiteralDfa5_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0x4000000000000ULL, active5, 0L, active6, 0x20000a000000ULL, active7, 0L, active8, 0x100000000ULL);
      case 82:
      case 114:
         if ((active0 & 0x100ULL) != 0L)
            return jjStartNfaWithStates_0(4, 8, 154);
         else if ((active2 & 0x800000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 151, 154);
         else if ((active3 & 0x200000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 213, 154);
         else if ((active4 & 0x4ULL) != 0L)
            return jjStartNfaWithStates_0(4, 258, 154);
         else if ((active4 & 0x10000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 272, 154);
         else if ((active4 & 0x1000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 292, 154);
         else if ((active5 & 0x20ULL) != 0L)
            return jjStartNfaWithStates_0(4, 325, 154);
         else if ((active5 & 0x20000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 349, 154);
         else if ((active6 & 0x1000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 432, 154);
         else if ((active7 & 0x2000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 461, 154);
         else if ((active7 & 0x20000000ULL) != 0L)
         {
            jjmatchedKind = 477;
            jjmatchedPos = 4;
         }
         else if ((active7 & 0x80000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 479, 154);
         return jjMoveStringLiteralDfa5_0(active0, 0x2060000000008800ULL, active1, 0x419008ULL, active2, 0x4001f000000a8ULL, active3, 0L, active4, 0x240040001801ULL, active5, 0x40000040000ULL, active6, 0x404800000000c018ULL, active7, 0x80300001401000ULL, active8, 0L);
      case 83:
      case 115:
         if ((active1 & 0x8000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 115, 154);
         else if ((active1 & 0x10000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 116, 154);
         else if ((active1 & 0x2000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 125, 154);
         else if ((active4 & 0x100ULL) != 0L)
            return jjStartNfaWithStates_0(4, 264, 154);
         else if ((active4 & 0x10000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 296, 154);
         else if ((active5 & 0x2000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 333, 154);
         else if ((active6 & 0x40ULL) != 0L)
            return jjStartNfaWithStates_0(4, 390, 154);
         return jjMoveStringLiteralDfa5_0(active0, 0x10000000ULL, active1, 0L, active2, 0L, active3, 0x3f00ULL, active4, 0x4000000000000000ULL, active5, 0x800000000ULL, active6, 0L, active7, 0L, active8, 0x240700000ULL);
      case 84:
      case 116:
         if ((active1 & 0x200ULL) != 0L)
         {
            jjmatchedKind = 73;
            jjmatchedPos = 4;
         }
         else if ((active1 & 0x10000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 92, 154);
         else if ((active3 & 0x80000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 235, 154);
         else if ((active4 & 0x200000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 313, 154);
         else if ((active5 & 0x20000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 337, 154);
         else if ((active6 & 0x800000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 419, 154);
         else if ((active6 & 0x80000000000ULL) != 0L)
         {
            jjmatchedKind = 427;
            jjmatchedPos = 4;
         }
         else if ((active6 & 0x2000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 433, 154);
         else if ((active7 & 0x200000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 505, 154);
         else if ((active8 & 0x800000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 535, 154);
         return jjMoveStringLiteralDfa5_0(active0, 0xc3e0000000000ULL, active1, 0x2a000080000ULL, active2, 0x10000001ULL, active3, 0L, active4, 0x800600210ULL, active5, 0x8000000000000010ULL, active6, 0x440000820ULL, active7, 0x8000800000ULL, active8, 0x1820ULL);
      case 85:
      case 117:
         return jjMoveStringLiteralDfa5_0(active0, 0x10000000000000ULL, active1, 0x2000060ULL, active2, 0x40ULL, active3, 0x80000ULL, active4, 0x400000002000ULL, active5, 0x1008000ULL, active6, 0x20000000ULL, active7, 0L, active8, 0x20000400ULL);
      case 86:
      case 118:
         return jjMoveStringLiteralDfa5_0(active0, 0x800000000000000ULL, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0x4000000000ULL, active7, 0x40000ULL, active8, 0L);
      case 88:
      case 120:
         return jjMoveStringLiteralDfa5_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x10ULL);
      case 89:
      case 121:
         if ((active5 & 0x200000000ULL) != 0L)
         {
            jjmatchedKind = 353;
            jjmatchedPos = 4;
         }
         else if ((active6 & 0x4000000ULL) != 0L)
            return jjStartNfaWithStates_0(4, 410, 154);
         return jjMoveStringLiteralDfa5_0(active0, 0x200ULL, active1, 0L, active2, 0L, active3, 0x400000000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 90:
      case 122:
         return jjMoveStringLiteralDfa5_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0x8000000000000000ULL, active5, 0x1ULL, active6, 0L, active7, 0L, active8, 0L);
      default :
         break;
   }
   return jjStartNfa_0(3, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa5_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active4 &= old4) | (active5 &= old5) | (active6 &= old6) | (active7 &= old7) | (active8 &= old8)) == 0L)
      return jjStartNfa_0(3, old0, old1, old2, old3, old4, old5, old6, old7, old8, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(4, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
      return 5;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa6_0(active0, 0x10000000ULL, active1, 0L, active2, 0x1c00000000000ULL, active3, 0x400000010ULL, active4, 0L, active5, 0L, active6, 0x400001018ULL, active7, 0x800000ULL, active8, 0x20000000000ULL);
      case 65:
      case 97:
         if ((active2 & 0x100000000000ULL) != 0L)
         {
            jjmatchedKind = 172;
            jjmatchedPos = 5;
         }
         return jjMoveStringLiteralDfa6_0(active0, 0L, active1, 0x88019000ULL, active2, 0x200000010010ULL, active3, 0x20f04ULL, active4, 0x2400ULL, active5, 0x80010000004ULL, active6, 0x4000000000000200ULL, active7, 0x8400c00600041080ULL, active8, 0xa02000020ULL);
      case 66:
      case 98:
         return jjMoveStringLiteralDfa6_0(active0, 0x6000ULL, active1, 0x1000000000000000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 67:
      case 99:
         if ((active2 & 0x4000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 154, 154);
         else if ((active4 & 0x400000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 314, 154);
         else if ((active5 & 0x2000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 357, 154);
         return jjMoveStringLiteralDfa6_0(active0, 0x1800f800000ULL, active1, 0x8ULL, active2, 0x40000000ULL, active3, 0L, active4, 0L, active5, 0x50c0000000080000ULL, active6, 0x80000000ULL, active7, 0x1000000000000ULL, active8, 0x10ULL);
      case 68:
      case 100:
         if ((active4 & 0x80000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 275, 154);
         else if ((active4 & 0x20000000000000ULL) != 0L)
         {
            jjmatchedKind = 309;
            jjmatchedPos = 5;
         }
         else if ((active5 & 0x400000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 366, 154);
         return jjMoveStringLiteralDfa6_0(active0, 0x80000ULL, active1, 0x2000060ULL, active2, 0x200000ULL, active3, 0xf000000ULL, active4, 0x40000000000000ULL, active5, 0x800000000000ULL, active6, 0x80ULL, active7, 0x40000000000ULL, active8, 0L);
      case 69:
      case 101:
         if ((active0 & 0x8000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 15, 154);
         else if ((active0 & 0x200000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 57, 154);
         else if ((active1 & 0x400000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 86, 154);
         else if ((active2 & 0x200000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 185, 154);
         else if ((active2 & 0x800000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 187, 154);
         else if ((active4 & 0x200000ULL) != 0L)
         {
            jjmatchedKind = 277;
            jjmatchedPos = 5;
         }
         else if ((active4 & 0x1000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 280, 154);
         else if ((active5 & 0x10ULL) != 0L)
            return jjStartNfaWithStates_0(5, 324, 154);
         else if ((active6 & 0x20ULL) != 0L)
            return jjStartNfaWithStates_0(5, 389, 154);
         else if ((active6 & 0x800ULL) != 0L)
            return jjStartNfaWithStates_0(5, 395, 154);
         else if ((active6 & 0x40000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 402, 154);
         else if ((active6 & 0x2000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 409, 154);
         else if ((active7 & 0x100000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 504, 154);
         else if ((active8 & 0x20000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 541, 154);
         return jjMoveStringLiteralDfa6_0(active0, 0x980000000000000ULL, active1, 0x101100080000ULL, active2, 0xfc00ULL, active3, 0x41c080ULL, active4, 0x40000400000ULL, active5, 0x11000200002ULL, active6, 0x20004000000000ULL, active7, 0x200800000000ULL, active8, 0L);
      case 70:
      case 102:
         if ((active7 & 0x200000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 469, 154);
         return jjMoveStringLiteralDfa6_0(active0, 0L, active1, 0L, active2, 0x2000000000000000ULL, active3, 0x3000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0xcULL);
      case 71:
      case 103:
         if ((active6 & 0x400000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 430, 154);
         return jjMoveStringLiteralDfa6_0(active0, 0L, active1, 0x1c00000000000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0xd00000ULL, active6, 0x2000000000ULL, active7, 0L, active8, 0L);
      case 72:
      case 104:
         if ((active1 & 0x8000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 103, 154);
         else if ((active4 & 0x10000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 308, 154);
         break;
      case 73:
      case 105:
         return jjMoveStringLiteralDfa6_0(active0, 0x200c400000000000ULL, active1, 0x800006ULL, active2, 0x4000110000088ULL, active3, 0x4000000000000000ULL, active4, 0x800000210ULL, active5, 0x800040000ULL, active6, 0x200000004000ULL, active7, 0x2008000400200ULL, active8, 0x80001ULL);
      case 76:
      case 108:
         if ((active2 & 0x20000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 145, 154);
         else if ((active4 & 0x20ULL) != 0L)
            return jjStartNfaWithStates_0(5, 261, 154);
         else if ((active7 & 0x800000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 507, 154);
         return jjMoveStringLiteralDfa6_0(active0, 0x10000000000000ULL, active1, 0x802200000000000ULL, active2, 0x20000001000000ULL, active3, 0L, active4, 0L, active5, 0x20000000008000ULL, active6, 0L, active7, 0x10000ULL, active8, 0x700400ULL);
      case 77:
      case 109:
         if ((active4 & 0x1000000000000000ULL) != 0L)
         {
            jjmatchedKind = 316;
            jjmatchedPos = 5;
         }
         return jjMoveStringLiteralDfa6_0(active0, 0L, active1, 0x5000000ULL, active2, 0x8000000000000000ULL, active3, 0x100000ULL, active4, 0L, active5, 0L, active6, 0x8000ULL, active7, 0L, active8, 0x4000ULL);
      case 78:
      case 110:
         if ((active0 & 0x10ULL) != 0L)
            return jjStartNfaWithStates_0(5, 4, 154);
         else if ((active1 & 0x1ULL) != 0L)
            return jjStartNfaWithStates_0(5, 64, 154);
         else if ((active2 & 0x2ULL) != 0L)
         {
            jjmatchedKind = 129;
            jjmatchedPos = 5;
         }
         else if ((active3 & 0x20000000000ULL) != 0L)
         {
            jjmatchedKind = 233;
            jjmatchedPos = 5;
         }
         else if ((active6 & 0x20000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 425, 154);
         else if ((active7 & 0x80000000000000ULL) != 0L)
         {
            jjmatchedKind = 503;
            jjmatchedPos = 5;
         }
         else if ((active8 & 0x80000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 555, 154);
         return jjMoveStringLiteralDfa6_0(active0, 0x801c00001000ULL, active1, 0x20000060000000ULL, active2, 0x1003de00080004ULL, active3, 0xffe08000080000ULL, active4, 0x200000000001ULL, active5, 0x8000001004000ULL, active6, 0x8000000000230000ULL, active7, 0x7000000800ULL, active8, 0x1400000000ULL);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa6_0(active0, 0x4000000000300000ULL, active1, 0x20000000000ULL, active2, 0x82040000000000ULL, active3, 0L, active4, 0x8084000000000000ULL, active5, 0x601ULL, active6, 0x100ULL, active7, 0L, active8, 0x2100000000ULL);
      case 80:
      case 112:
         if ((active4 & 0x400000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 302, 154);
         return jjMoveStringLiteralDfa6_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x8000000ULL);
      case 82:
      case 114:
         if ((active1 & 0x4000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 126, 154);
         else if ((active3 & 0x100000000000000ULL) != 0L)
         {
            jjmatchedKind = 248;
            jjmatchedPos = 5;
         }
         else if ((active4 & 0x2ULL) != 0L)
            return jjStartNfaWithStates_0(5, 257, 154);
         else if ((active7 & 0x100ULL) != 0L)
            return jjStartNfaWithStates_0(5, 456, 154);
         return jjMoveStringLiteralDfa6_0(active0, 0x613e0000000000ULL, active1, 0x100000ULL, active2, 0x40000082400000ULL, active3, 0x20ULL, active4, 0x40000000ULL, active5, 0x8000004000000000ULL, active6, 0x401ULL, active7, 0x4030001000004ULL, active8, 0x1840ULL);
      case 83:
      case 115:
         if ((active0 & 0x200ULL) != 0L)
            return jjStartNfaWithStates_0(5, 9, 154);
         else if ((active1 & 0x10ULL) != 0L)
            return jjStartNfaWithStates_0(5, 68, 154);
         else if ((active2 & 0x1ULL) != 0L)
            return jjStartNfaWithStates_0(5, 128, 154);
         else if ((active2 & 0x20ULL) != 0L)
            return jjStartNfaWithStates_0(5, 133, 154);
         else if ((active4 & 0x4000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 282, 154);
         else if ((active5 & 0x100ULL) != 0L)
            return jjStartNfaWithStates_0(5, 328, 154);
         else if ((active6 & 0x40000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 414, 154);
         return jjMoveStringLiteralDfa6_0(active0, 0L, active1, 0x80ULL, active2, 0L, active3, 0L, active4, 0x82000000800ULL, active5, 0x400000000000000ULL, active6, 0x44000000000004ULL, active7, 0x78100000000400ULL, active8, 0x10000ULL);
      case 84:
      case 116:
         if ((active1 & 0x8000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 127, 154);
         else if ((active2 & 0x40ULL) != 0L)
            return jjStartNfaWithStates_0(5, 134, 154);
         else if ((active4 & 0x80000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 287, 154);
         else if ((active4 & 0x100000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 300, 154);
         else if ((active5 & 0x20000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 361, 154);
         else if ((active5 & 0x2000000000000000ULL) != 0L)
         {
            jjmatchedKind = 381;
            jjmatchedPos = 5;
         }
         else if ((active6 & 0x8000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 411, 154);
         else if ((active6 & 0x8000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 435, 154);
         else if ((active7 & 0x1000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 508, 154);
         else if ((active8 & 0x40000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 542, 154);
         return jjMoveStringLiteralDfa6_0(active0, 0x80000063c0020800ULL, active1, 0x200000000ULL, active2, 0x20000000ULL, active3, 0x40000000001ULL, active4, 0x4000000400000000ULL, active5, 0x800000002000000ULL, active6, 0x20000000ULL, active7, 0x2000000000000030ULL, active8, 0x2000ULL);
      case 85:
      case 117:
         return jjMoveStringLiteralDfa6_0(active0, 0x10008ULL, active1, 0L, active2, 0L, active3, 0L, active4, 0x1000000000000ULL, active5, 0L, active6, 0L, active7, 0L, active8, 0x100000000000ULL);
      case 86:
      case 118:
         return jjMoveStringLiteralDfa6_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0x1000ULL, active5, 0L, active6, 0x2000000000000000ULL, active7, 0L, active8, 0x8000000300ULL);
      case 87:
      case 119:
         if ((active5 & 0x800ULL) != 0L)
            return jjStartNfaWithStates_0(5, 331, 154);
         return jjMoveStringLiteralDfa6_0(active0, 0L, active1, 0x800ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0x8ULL, active6, 0L, active7, 0L, active8, 0L);
      case 88:
      case 120:
         return jjMoveStringLiteralDfa6_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0x1000000ULL, active7, 0L, active8, 0L);
      case 89:
      case 121:
         if ((active5 & 0x40000000000ULL) != 0L)
            return jjStartNfaWithStates_0(5, 362, 154);
         return jjMoveStringLiteralDfa6_0(active0, 0L, active1, 0x2000000000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      default :
         break;
   }
   return jjStartNfa_0(4, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa6_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active4 &= old4) | (active5 &= old5) | (active6 &= old6) | (active7 &= old7) | (active8 &= old8)) == 0L)
      return jjStartNfa_0(4, old0, old1, old2, old3, old4, old5, old6, old7, old8, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(5, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
      return 6;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 34:
         if ((active5 & 0x2000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 345, 12);
         break;
      case 50:
         if ((active7 & 0x4000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 498, 154);
         break;
      case 95:
         return jjMoveStringLiteralDfa7_0(active0, 0x1000400000000ULL, active1, 0L, active2, 0x40200000000000ULL, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x4300ULL);
      case 65:
      case 97:
         return jjMoveStringLiteralDfa7_0(active0, 0x201e0000000000ULL, active1, 0x800000ULL, active2, 0x20000000ULL, active3, 0x8400000020ULL, active4, 0x4000000000001001ULL, active5, 0x8000000000000000ULL, active6, 0x2000000000000000ULL, active7, 0x800ULL, active8, 0x400710000ULL);
      case 66:
      case 98:
         return jjMoveStringLiteralDfa7_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0x4000ULL, active7, 0L, active8, 0x20000000000ULL);
      case 67:
      case 99:
         if ((active3 & 0x4000000000000000ULL) != 0L)
         {
            jjmatchedKind = 254;
            jjmatchedPos = 6;
         }
         else if ((active7 & 0x400000ULL) != 0L)
         {
            jjmatchedKind = 470;
            jjmatchedPos = 6;
         }
         return jjMoveStringLiteralDfa7_0(active0, 0x8000000000000000ULL, active1, 0x20100006ULL, active2, 0x10400100000000ULL, active3, 0xf00ULL, active4, 0L, active5, 0x400000000040000ULL, active6, 0x20100ULL, active7, 0x400000000000000ULL, active8, 0L);
      case 68:
      case 100:
         if ((active0 & 0x80000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 55, 154);
         else if ((active0 & 0x800000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 59, 154);
         else if ((active1 & 0x80000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 83, 154);
         else if ((active1 & 0x80000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 95, 154);
         else if ((active1 & 0x100000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 108, 154);
         else if ((active3 & 0x400000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 214, 154);
         return jjMoveStringLiteralDfa7_0(active0, 0x1800000000ULL, active1, 0L, active2, 0x80ULL, active3, 0x80000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 69:
      case 101:
         if ((active0 & 0x80000ULL) != 0L)
         {
            jjmatchedKind = 19;
            jjmatchedPos = 6;
         }
         else if ((active1 & 0x20ULL) != 0L)
            return jjStartNfaWithStates_0(6, 69, 154);
         else if ((active2 & 0x4000000000ULL) != 0L)
         {
            jjmatchedKind = 166;
            jjmatchedPos = 6;
         }
         else if ((active4 & 0x80000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 299, 154);
         else if ((active5 & 0x80000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 339, 154);
         else if ((active5 & 0x800000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 379, 154);
         else if ((active6 & 0x400ULL) != 0L)
            return jjStartNfaWithStates_0(6, 394, 154);
         else if ((active6 & 0x20000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 413, 154);
         else if ((active7 & 0x10000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 488, 154);
         else if ((active8 & 0x8000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 539, 154);
         return jjMoveStringLiteralDfa7_0(active0, 0x40000000000000ULL, active1, 0x1c00005000008ULL, active2, 0x8000039e01000000ULL, active3, 0xf000000ULL, active4, 0x40000800ULL, active5, 0x20800000000000ULL, active6, 0x40000001000000ULL, active7, 0x1000604ULL, active8, 0x8000000010ULL);
      case 71:
      case 103:
         if ((active0 & 0x100000ULL) != 0L)
         {
            jjmatchedKind = 20;
            jjmatchedPos = 6;
         }
         else if ((active1 & 0x20000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 117, 154);
         else if ((active2 & 0x80000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 147, 154);
         else if ((active5 & 0x400000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 342, 154);
         else if ((active5 & 0x800000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 343, 154);
         else if ((active5 & 0x8000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 371, 154);
         else if ((active6 & 0x8000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 447, 154);
         else if ((active8 & 0x1000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 548, 154);
         return jjMoveStringLiteralDfa7_0(active0, 0x200000ULL, active1, 0L, active2, 0L, active3, 0L, active4, 0x2000ULL, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 72:
      case 104:
         if ((active0 & 0x20000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 17, 154);
         break;
      case 73:
      case 105:
         return jjMoveStringLiteralDfa7_0(active0, 0x43c0000800ULL, active1, 0x800000202000840ULL, active2, 0x2020000000200000ULL, active3, 0x40000100000ULL, active4, 0x4002400000000ULL, active5, 0x4001000000ULL, active6, 0x4000000008080ULL, active7, 0x2000100000010030ULL, active8, 0x180cULL);
      case 76:
      case 108:
         if ((active1 & 0x8000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 79, 154);
         else if ((active2 & 0x10000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 144, 154);
         else if ((active6 & 0x200ULL) != 0L)
            return jjStartNfaWithStates_0(6, 393, 154);
         else if ((active6 & 0x4000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 446, 154);
         else if ((active7 & 0x1000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 460, 154);
         return jjMoveStringLiteralDfa7_0(active0, 0x10000ULL, active1, 0x1000000008000000ULL, active2, 0x10ULL, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x8000000840000ULL, active8, 0L);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa7_0(active0, 0xc000000001000ULL, active1, 0x1000000000ULL, active2, 0L, active3, 0L, active4, 0x1000000000000ULL, active5, 0L, active6, 0L, active7, 0L, active8, 0x200000000ULL);
      case 78:
      case 110:
         if ((active1 & 0x1000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 76, 154);
         else if ((active2 & 0x2000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 177, 154);
         else if ((active2 & 0x80000000000000ULL) != 0L)
         {
            jjmatchedKind = 183;
            jjmatchedPos = 6;
         }
         else if ((active5 & 0x8ULL) != 0L)
            return jjStartNfaWithStates_0(6, 323, 154);
         else if ((active5 & 0x200ULL) != 0L)
         {
            jjmatchedKind = 329;
            jjmatchedPos = 6;
         }
         else if ((active5 & 0x10000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 360, 154);
         else if ((active5 & 0x80000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 363, 154);
         else if ((active6 & 0x2000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 421, 154);
         return jjMoveStringLiteralDfa7_0(active0, 0x400000000000ULL, active1, 0L, active2, 0x800000000008ULL, active3, 0x10ULL, active4, 0x8080040000000000ULL, active5, 0x401ULL, active6, 0x200000010000ULL, active7, 0x2200000000000ULL, active8, 0x2000080041ULL);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa7_0(active0, 0x10000000ULL, active1, 0L, active2, 0L, active3, 0x3000ULL, active4, 0x800000010ULL, active5, 0L, active6, 0L, active7, 0x1008000000000ULL, active8, 0L);
      case 80:
      case 112:
         if ((active8 & 0x100000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 544, 154);
         return jjMoveStringLiteralDfa7_0(active0, 0x2000000000000000ULL, active1, 0x2000000000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0xcULL, active7, 0x200000000ULL, active8, 0L);
      case 82:
      case 114:
         if ((active0 & 0x100000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 56, 154);
         else if ((active1 & 0x100000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 96, 154);
         else if ((active1 & 0x20000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 105, 154);
         else if ((active5 & 0x2ULL) != 0L)
         {
            jjmatchedKind = 321;
            jjmatchedPos = 6;
         }
         else if ((active6 & 0x4000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 422, 154);
         else if ((active6 & 0x20000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 437, 154);
         else if ((active7 & 0x8000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 511, 154);
         else if ((active8 & 0x800000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 547, 154);
         return jjMoveStringLiteralDfa7_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x1c000ULL, active4, 0L, active5, 0x100000ULL, active6, 0x1000ULL, active7, 0x80ULL, active8, 0x2002000ULL);
      case 83:
      case 115:
         if ((active2 & 0x4ULL) != 0L)
            return jjStartNfaWithStates_0(6, 130, 154);
         else if ((active4 & 0x400000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 278, 154);
         else if ((active4 & 0x200000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 301, 154);
         else if ((active4 & 0x40000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 310, 154);
         return jjMoveStringLiteralDfa7_0(active0, 0x4000000000000000ULL, active1, 0x80ULL, active2, 0x1000000000000ULL, active3, 0x20004ULL, active4, 0L, active5, 0L, active6, 0x10ULL, active7, 0L, active8, 0L);
      case 84:
      case 116:
         if ((active2 & 0x40000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 158, 154);
         else if ((active2 & 0x80000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 159, 154);
         else if ((active3 & 0x200000000000ULL) != 0L)
         {
            jjmatchedKind = 237;
            jjmatchedPos = 6;
         }
         else if ((active5 & 0x4000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 334, 154);
         else if ((active5 & 0x8000ULL) != 0L)
         {
            jjmatchedKind = 335;
            jjmatchedPos = 6;
         }
         else if ((active5 & 0x1000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 380, 154);
         else if ((active5 & 0x4000000000000000ULL) != 0L)
         {
            jjmatchedKind = 382;
            jjmatchedPos = 6;
         }
         else if ((active6 & 0x1ULL) != 0L)
            return jjStartNfaWithStates_0(6, 384, 154);
         else if ((active6 & 0x200000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 405, 154);
         else if ((active6 & 0x80000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 415, 154);
         else if ((active8 & 0x100000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 556, 154);
         return jjMoveStringLiteralDfa7_0(active0, 0x1001a00f800008ULL, active1, 0x40010000ULL, active2, 0x400000200fc00ULL, active3, 0xffc00000000000ULL, active4, 0x600ULL, active5, 0xc0001810000004ULL, active6, 0L, active7, 0x7800000000ULL, active8, 0x420ULL);
      case 85:
      case 117:
         return jjMoveStringLiteralDfa7_0(active0, 0xa00000006000ULL, active1, 0x2200000000000ULL, active2, 0x40000000000ULL, active3, 0x1ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0x40000000000ULL, active8, 0L);
      case 86:
      case 118:
         return jjMoveStringLiteralDfa7_0(active0, 0L, active1, 0L, active2, 0x10400000ULL, active3, 0x80ULL, active4, 0L, active5, 0L, active6, 0x400000000ULL, active7, 0xc00000000000ULL, active8, 0L);
      case 88:
      case 120:
         return jjMoveStringLiteralDfa7_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0x200000ULL, active6, 0L, active7, 0x30000000000000ULL, active8, 0L);
      case 89:
      case 121:
         if ((active7 & 0x400000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 482, 154);
         else if ((active7 & 0x20000000000ULL) != 0L)
            return jjStartNfaWithStates_0(6, 489, 154);
         return jjMoveStringLiteralDfa7_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x40000000000000ULL, active8, 0L);
      default :
         break;
   }
   return jjStartNfa_0(5, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa7_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active4 &= old4) | (active5 &= old5) | (active6 &= old6) | (active7 &= old7) | (active8 &= old8)) == 0L)
      return jjStartNfa_0(5, old0, old1, old2, old3, old4, old5, old6, old7, old8, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(6, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
      return 7;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa8_0(active0, 0x1800200000ULL, active1, 0x1c00000000006ULL, active2, 0x38000000000ULL, active3, 0xffc0000001c000ULL, active4, 0x80000000000000ULL, active5, 0x240000ULL, active6, 0L, active7, 0x1000000000ULL, active8, 0L);
      case 65:
      case 97:
         return jjMoveStringLiteralDfa8_0(active0, 0L, active1, 0L, active2, 0xc00000000000ULL, active3, 0x10ULL, active4, 0L, active5, 0x100000ULL, active6, 0x400001110ULL, active7, 0L, active8, 0L);
      case 66:
      case 98:
         return jjMoveStringLiteralDfa8_0(active0, 0x20000000000000ULL, active1, 0x1000000000ULL, active2, 0x20000000ULL, active3, 0L, active4, 0x1000000000000ULL, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 67:
      case 99:
         if ((active6 & 0x1000000ULL) != 0L)
            return jjStopAtPos(7, 408);
         else if ((active8 & 0x4ULL) != 0L)
         {
            jjmatchedKind = 514;
            jjmatchedPos = 7;
         }
         return jjMoveStringLiteralDfa8_0(active0, 0x200000000000ULL, active1, 0L, active2, 0x2001000000000000ULL, active3, 0L, active4, 0x40000000800ULL, active5, 0L, active6, 0x40000000000000ULL, active7, 0x200000000000ULL, active8, 0x8ULL);
      case 68:
      case 100:
         if ((active0 & 0x40000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 54, 154);
         else if ((active1 & 0x8ULL) != 0L)
            return jjStartNfaWithStates_0(7, 67, 154);
         else if ((active5 & 0x800000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 367, 154);
         return jjMoveStringLiteralDfa8_0(active0, 0L, active1, 0L, active2, 0x1e00000000ULL, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x80ULL, active8, 0L);
      case 69:
      case 101:
         if ((active0 & 0x8ULL) != 0L)
            return jjStartNfaWithStates_0(7, 3, 154);
         else if ((active0 & 0x800000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 47, 154);
         else if ((active1 & 0x20000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 93, 154);
         else if ((active1 & 0x2000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 101, 154);
         else if ((active1 & 0x200000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 109, 154);
         else if ((active1 & 0x2000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 113, 154);
         else if ((active1 & 0x1000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 124, 154);
         else if ((active2 & 0x400000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 150, 154);
         else if ((active2 & 0x10000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 156, 154);
         else if ((active2 & 0x10000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 180, 154);
         else if ((active4 & 0x2000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 269, 154);
         else if ((active5 & 0x4ULL) != 0L)
            return jjStartNfaWithStates_0(7, 322, 154);
         else if ((active5 & 0x10000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 348, 154);
         else if ((active5 & 0x400000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 378, 154);
         else if ((active6 & 0x4000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 398, 154);
         else if ((active8 & 0x20ULL) != 0L)
            return jjStartNfaWithStates_0(7, 517, 154);
         return jjMoveStringLiteralDfa8_0(active0, 0xc00200f801000ULL, active1, 0x10000ULL, active2, 0xfc00ULL, active3, 0x80080ULL, active4, 0x8000000000000000ULL, active5, 0xc0000000000001ULL, active6, 0x10000ULL, active7, 0x800800000ULL, active8, 0L);
      case 70:
      case 102:
         return jjMoveStringLiteralDfa8_0(active0, 0L, active1, 0L, active2, 0L, active3, 0xf000000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 71:
      case 103:
         if ((active2 & 0x8ULL) != 0L)
            return jjStartNfaWithStates_0(7, 131, 154);
         else if ((active6 & 0x200000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 429, 154);
         else if ((active8 & 0x80000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 531, 154);
         return jjMoveStringLiteralDfa8_0(active0, 0L, active1, 0L, active2, 0x1000000ULL, active3, 0x400000000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0xc00000000004ULL, active8, 0L);
      case 72:
      case 104:
         if ((active0 & 0x8000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 63, 154);
         return jjMoveStringLiteralDfa8_0(active0, 0L, active1, 0x100000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa8_0(active0, 0x1f8000000000ULL, active1, 0x40000080ULL, active2, 0x2000090ULL, active3, 0L, active4, 0L, active5, 0x8000000800000000ULL, active6, 0L, active7, 0x6000000000ULL, active8, 0x2000002440ULL);
      case 75:
      case 107:
         if ((active7 & 0x400000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 506, 154);
         break;
      case 76:
      case 108:
         if ((active4 & 0x1ULL) != 0L)
            return jjStartNfaWithStates_0(7, 256, 154);
         else if ((active4 & 0x1000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 268, 154);
         else if ((active7 & 0x800ULL) != 0L)
            return jjStartNfaWithStates_0(7, 459, 154);
         return jjMoveStringLiteralDfa8_0(active0, 0x10000ULL, active1, 0x8000000ULL, active2, 0L, active3, 0x8000000000ULL, active4, 0L, active5, 0L, active6, 0x2000000000000000ULL, active7, 0L, active8, 0L);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa8_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0x4000000000000000ULL, active5, 0L, active6, 0L, active7, 0x30ULL, active8, 0x10000ULL);
      case 78:
      case 110:
         if ((active4 & 0x10ULL) != 0L)
            return jjStartNfaWithStates_0(7, 260, 154);
         else if ((active4 & 0x800000000ULL) != 0L)
         {
            jjmatchedKind = 291;
            jjmatchedPos = 7;
         }
         return jjMoveStringLiteralDfa8_0(active0, 0x1000400000000ULL, active1, 0x7000840ULL, active2, 0x8040240000200000ULL, active3, 0L, active4, 0x4000040000000ULL, active5, 0x20000000000000ULL, active6, 0x8000ULL, active7, 0x8001000000ULL, active8, 0x1800ULL);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa8_0(active0, 0x43c0000800ULL, active1, 0x200000000ULL, active2, 0L, active3, 0x40000000000ULL, active4, 0x2400000400ULL, active5, 0x1000000ULL, active6, 0xcULL, active7, 0x8000000000000ULL, active8, 0L);
      case 80:
      case 112:
         if ((active8 & 0x200000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 545, 154);
         return jjMoveStringLiteralDfa8_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x110ULL);
      case 82:
      case 114:
         if ((active8 & 0x8000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 551, 154);
         return jjMoveStringLiteralDfa8_0(active0, 0x10000000ULL, active1, 0L, active2, 0L, active3, 0x3021ULL, active4, 0L, active5, 0x1000000000ULL, active6, 0L, active7, 0x40000000000ULL, active8, 0x402000000ULL);
      case 83:
      case 115:
         if ((active0 & 0x400000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 46, 154);
         else if ((active0 & 0x10000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 52, 154);
         else if ((active5 & 0x400ULL) != 0L)
            return jjStartNfaWithStates_0(7, 330, 154);
         else if ((active7 & 0x200ULL) != 0L)
            return jjStartNfaWithStates_0(7, 457, 154);
         else if ((active7 & 0x200000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 481, 154);
         return jjMoveStringLiteralDfa8_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x4ULL, active4, 0L, active5, 0L, active6, 0x80ULL, active7, 0L, active8, 0x200ULL);
      case 84:
      case 116:
         if ((active2 & 0x100000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 160, 154);
         else if ((active3 & 0x20000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 209, 154);
         else if ((active6 & 0x20000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 401, 154);
         else if ((active7 & 0x400ULL) != 0L)
            return jjStartNfaWithStates_0(7, 458, 154);
         else if ((active8 & 0x1ULL) != 0L)
            return jjStartNfaWithStates_0(7, 512, 154);
         return jjMoveStringLiteralDfa8_0(active0, 0x6000000000006000ULL, active1, 0x800000ULL, active2, 0L, active3, 0x100f00ULL, active4, 0L, active5, 0L, active6, 0x4000000000000ULL, active7, 0x2000000000000ULL, active8, 0x700000ULL);
      case 85:
      case 117:
         return jjMoveStringLiteralDfa8_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x1000000040000ULL, active8, 0x20000004000ULL);
      case 86:
      case 118:
         return jjMoveStringLiteralDfa8_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x2000100000000000ULL, active8, 0L);
      case 88:
      case 120:
         if ((active7 & 0x10000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 500, 154);
         break;
      case 89:
      case 121:
         if ((active2 & 0x4000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 178, 154);
         else if ((active4 & 0x200ULL) != 0L)
            return jjStartNfaWithStates_0(7, 265, 154);
         else if ((active7 & 0x20000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 501, 154);
         else if ((active7 & 0x40000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(7, 502, 154);
         break;
      case 90:
      case 122:
         return jjMoveStringLiteralDfa8_0(active0, 0L, active1, 0x800000000000000ULL, active2, 0x20000000000000ULL, active3, 0L, active4, 0L, active5, 0x4000000000ULL, active6, 0L, active7, 0x10000ULL, active8, 0L);
      default :
         break;
   }
   return jjStartNfa_0(6, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa8_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active4 &= old4) | (active5 &= old5) | (active6 &= old6) | (active7 &= old7) | (active8 &= old8)) == 0L)
      return jjStartNfa_0(6, old0, old1, old2, old3, old4, old5, old6, old7, old8, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(7, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
      return 8;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa9_0(active0, 0xc000000000000ULL, active1, 0L, active2, 0x2000001e00000000ULL, active3, 0x4ULL, active4, 0x8000000000000000ULL, active5, 0x1ULL, active6, 0L, active7, 0x8000000000ULL, active8, 0L);
      case 65:
      case 97:
         return jjMoveStringLiteralDfa9_0(active0, 0x1000400000000ULL, active1, 0x40000000ULL, active2, 0x60200000000000ULL, active3, 0L, active4, 0L, active5, 0x4000000000ULL, active6, 0L, active7, 0L, active8, 0x2000200ULL);
      case 67:
      case 99:
         if ((active8 & 0x2000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 525, 154);
         return jjMoveStringLiteralDfa9_0(active0, 0L, active1, 0L, active2, 0x8000000000ULL, active3, 0x400000004000ULL, active4, 0x40000000ULL, active5, 0L, active6, 0x10000ULL, active7, 0x1000000ULL, active8, 0x20000000000ULL);
      case 68:
      case 100:
         if ((active0 & 0x2000000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 37, 154);
         else if ((active1 & 0x10000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 80, 154);
         else if ((active3 & 0x80000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 211, 154);
         return jjMoveStringLiteralDfa9_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x1800000000000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 69:
      case 101:
         if ((active0 & 0x2000ULL) != 0L)
         {
            jjmatchedKind = 13;
            jjmatchedPos = 8;
         }
         else if ((active1 & 0x800000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 87, 154);
         else if ((active3 & 0x1ULL) != 0L)
            return jjStartNfaWithStates_0(8, 192, 154);
         else if ((active7 & 0x10ULL) != 0L)
         {
            jjmatchedKind = 452;
            jjmatchedPos = 8;
         }
         else if ((active7 & 0x10000ULL) != 0L)
         {
            jjmatchedKind = 464;
            jjmatchedPos = 8;
         }
         else if ((active7 & 0x40000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 466, 154);
         else if ((active7 & 0x40000000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 490, 154);
         else if ((active7 & 0x100000000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 492, 154);
         else if ((active7 & 0x2000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 509, 154);
         else if ((active8 & 0x100000ULL) != 0L)
         {
            jjmatchedKind = 532;
            jjmatchedPos = 8;
         }
         return jjMoveStringLiteralDfa9_0(active0, 0x4000ULL, active1, 0x800001000000000ULL, active2, 0x3000000ULL, active3, 0L, active4, 0x1040000000000ULL, active5, 0L, active6, 0L, active7, 0x2000000000024ULL, active8, 0x200000ULL);
      case 70:
      case 102:
         return jjMoveStringLiteralDfa9_0(active0, 0x1800000000ULL, active1, 0x6ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 71:
      case 103:
         if ((active1 & 0x40ULL) != 0L)
            return jjStartNfaWithStates_0(8, 70, 154);
         else if ((active1 & 0x800ULL) != 0L)
            return jjStartNfaWithStates_0(8, 75, 154);
         else if ((active1 & 0x2000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 89, 154);
         else if ((active2 & 0x200000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 149, 154);
         else if ((active3 & 0x400000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 226, 154);
         else if ((active8 & 0x800ULL) != 0L)
         {
            jjmatchedKind = 523;
            jjmatchedPos = 8;
         }
         return jjMoveStringLiteralDfa9_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0x20000000000000ULL, active6, 0L, active7, 0L, active8, 0x1000ULL);
      case 72:
      case 104:
         return jjMoveStringLiteralDfa9_0(active0, 0L, active1, 0L, active2, 0x1000000000000ULL, active3, 0L, active4, 0L, active5, 0x40000ULL, active6, 0L, active7, 0L, active8, 0L);
      case 73:
      case 105:
         if ((active0 & 0x10000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 16, 154);
         return jjMoveStringLiteralDfa9_0(active0, 0x4000000010000000ULL, active1, 0L, active2, 0L, active3, 0x800f000f00ULL, active4, 0L, active5, 0x1000000000ULL, active6, 0x4000000008000ULL, active7, 0x200000000080ULL, active8, 0x400000ULL);
      case 76:
      case 108:
         return jjMoveStringLiteralDfa9_0(active0, 0x20000000000000ULL, active1, 0x400000000000ULL, active2, 0x20000000ULL, active3, 0x80ULL, active4, 0L, active5, 0L, active6, 0x400000000ULL, active7, 0x6000000000ULL, active8, 0L);
      case 77:
      case 109:
         if ((active3 & 0x1000ULL) != 0L)
         {
            jjmatchedKind = 204;
            jjmatchedPos = 8;
         }
         else if ((active5 & 0x100000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 340, 154);
         return jjMoveStringLiteralDfa9_0(active0, 0L, active1, 0L, active2, 0x800000000000ULL, active3, 0x2010ULL, active4, 0L, active5, 0L, active6, 0x10ULL, active7, 0L, active8, 0L);
      case 78:
      case 110:
         if ((active0 & 0x800ULL) != 0L)
            return jjStartNfaWithStates_0(8, 11, 154);
         else if ((active0 & 0x40000000ULL) != 0L)
         {
            jjmatchedKind = 30;
            jjmatchedPos = 8;
         }
         else if ((active1 & 0x200000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 97, 154);
         else if ((active3 & 0x40000000000ULL) != 0L)
         {
            jjmatchedKind = 234;
            jjmatchedPos = 8;
         }
         else if ((active4 & 0x400000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 290, 154);
         else if ((active4 & 0x2000000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 293, 154);
         else if ((active5 & 0x1000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 344, 154);
         return jjMoveStringLiteralDfa9_0(active0, 0x1e4380201000ULL, active1, 0L, active2, 0x10000000080ULL, active3, 0x8000ULL, active4, 0L, active5, 0x8000000000000000ULL, active6, 0x1004ULL, active7, 0x1000000800000ULL, active8, 0x2000000040ULL);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa9_0(active0, 0x2000018000000000ULL, active1, 0x800000000080ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x100ULL);
      case 80:
      case 112:
         if ((active4 & 0x4000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 318, 154);
         else if ((active6 & 0x8ULL) != 0L)
            return jjStartNfaWithStates_0(8, 387, 154);
         return jjMoveStringLiteralDfa9_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x2000000000000ULL, active4, 0L, active5, 0x200000ULL, active6, 0L, active7, 0x8000000000000ULL, active8, 0x10000ULL);
      case 82:
      case 114:
         if ((active4 & 0x400ULL) != 0L)
            return jjStartNfaWithStates_0(8, 266, 154);
         else if ((active5 & 0x40000000000000ULL) != 0L)
         {
            jjmatchedKind = 374;
            jjmatchedPos = 8;
         }
         else if ((active7 & 0x800000000ULL) != 0L)
         {
            jjmatchedKind = 483;
            jjmatchedPos = 8;
         }
         return jjMoveStringLiteralDfa9_0(active0, 0xf800000ULL, active1, 0L, active2, 0xfc00ULL, active3, 0x4000000000000ULL, active4, 0L, active5, 0x80000000000000ULL, active6, 0L, active7, 0x1000000000ULL, active8, 0L);
      case 83:
      case 115:
         return jjMoveStringLiteralDfa9_0(active0, 0L, active1, 0L, active2, 0x20000000000ULL, active3, 0x8000000010000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x4400ULL);
      case 84:
      case 116:
         if ((active1 & 0x4000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 90, 154);
         else if ((active2 & 0x40000000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 170, 154);
         else if ((active2 & 0x8000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 191, 154);
         else if ((active4 & 0x4000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 306, 154);
         else if ((active6 & 0x80ULL) != 0L)
            return jjStartNfaWithStates_0(8, 391, 154);
         else if ((active6 & 0x40000000000000ULL) != 0L)
         {
            jjmatchedKind = 438;
            jjmatchedPos = 8;
         }
         return jjMoveStringLiteralDfa9_0(active0, 0x200000000000ULL, active1, 0x1000001000000ULL, active2, 0x400000000010ULL, active3, 0x70000000100000ULL, active4, 0x800ULL, active5, 0L, active6, 0x100ULL, active7, 0L, active8, 0x18ULL);
      case 85:
      case 117:
         return jjMoveStringLiteralDfa9_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x80000000000000ULL, active4, 0x80000000000000ULL, active5, 0L, active6, 0x2000000000000000ULL, active7, 0L, active8, 0L);
      case 86:
      case 118:
         return jjMoveStringLiteralDfa9_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0x800000000ULL, active6, 0L, active7, 0L, active8, 0L);
      case 88:
      case 120:
         if ((active7 & 0x400000000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 494, 154);
         break;
      case 89:
      case 121:
         if ((active1 & 0x100000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 84, 154);
         else if ((active1 & 0x8000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 91, 154);
         else if ((active3 & 0x20ULL) != 0L)
            return jjStartNfaWithStates_0(8, 197, 154);
         else if ((active7 & 0x800000000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 495, 154);
         else if ((active8 & 0x400000000ULL) != 0L)
            return jjStartNfaWithStates_0(8, 546, 154);
         break;
      default :
         break;
   }
   return jjStartNfa_0(7, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa9_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active4 &= old4) | (active5 &= old5) | (active6 &= old6) | (active7 &= old7) | (active8 &= old8)) == 0L)
      return jjStartNfa_0(7, old0, old1, old2, old3, old4, old5, old6, old7, old8, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(8, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
      return 9;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa10_0(active0, 0x4383800000ULL, active1, 0L, active2, 0xfc00ULL, active3, 0x80ULL, active4, 0L, active5, 0x80000000000000ULL, active6, 0L, active7, 0L, active8, 0x201000ULL);
      case 65:
      case 97:
         return jjMoveStringLiteralDfa10_0(active0, 0x200000ULL, active1, 0x1000000ULL, active2, 0x418000000000ULL, active3, 0x2c0000000c000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0x1000000000ULL, active8, 0L);
      case 66:
      case 98:
         return jjMoveStringLiteralDfa10_0(active0, 0L, active1, 0x40000000ULL, active2, 0x20000000000000ULL, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 67:
      case 99:
         if ((active5 & 0x1000000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 356, 154);
         return jjMoveStringLiteralDfa10_0(active0, 0x4000000000000000ULL, active1, 0x800000000000ULL, active2, 0x20200000000ULL, active3, 0x8000000010000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 68:
      case 100:
         if ((active1 & 0x800000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 123, 154);
         return jjMoveStringLiteralDfa10_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0x4ULL, active7, 0L, active8, 0L);
      case 69:
      case 101:
         if ((active0 & 0x20000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 53, 154);
         else if ((active2 & 0x20000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 157, 154);
         else if ((active2 & 0x800000000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 175, 154);
         else if ((active3 & 0x10ULL) != 0L)
            return jjStartNfaWithStates_0(9, 196, 154);
         else if ((active4 & 0x40000000ULL) != 0L)
         {
            jjmatchedKind = 286;
            jjmatchedPos = 9;
         }
         else if ((active5 & 0x800000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 355, 154);
         else if ((active6 & 0x100ULL) != 0L)
            return jjStartNfaWithStates_0(9, 392, 154);
         else if ((active6 & 0x2000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 445, 154);
         else if ((active7 & 0x8000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 499, 154);
         return jjMoveStringLiteralDfa10_0(active0, 0L, active1, 0x1400000000000ULL, active2, 0x1000000000000ULL, active3, 0x1000000100000ULL, active4, 0L, active5, 0x200000ULL, active6, 0L, active7, 0x6001000000ULL, active8, 0x4400ULL);
      case 71:
      case 103:
         if ((active2 & 0x80ULL) != 0L)
            return jjStartNfaWithStates_0(9, 135, 154);
         else if ((active8 & 0x40ULL) != 0L)
            return jjStartNfaWithStates_0(9, 518, 154);
         else if ((active8 & 0x2000000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 549, 154);
         return jjMoveStringLiteralDfa10_0(active0, 0x10000000ULL, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x800000ULL, active8, 0L);
      case 72:
      case 104:
         return jjMoveStringLiteralDfa10_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0x8000000000000000ULL, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa10_0(active0, 0xc000004000000ULL, active1, 0L, active2, 0L, active3, 0x30000000000000ULL, active4, 0x800ULL, active5, 0x40000ULL, active6, 0L, active7, 0L, active8, 0x10ULL);
      case 75:
      case 107:
         if ((active6 & 0x1000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 396, 154);
         return jjMoveStringLiteralDfa10_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x20000000000ULL);
      case 76:
      case 108:
         return jjMoveStringLiteralDfa10_0(active0, 0L, active1, 0L, active2, 0x400000000ULL, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x10000ULL);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa10_0(active0, 0x1000400000000ULL, active1, 0L, active2, 0x40200000000000ULL, active3, 0L, active4, 0L, active5, 0x1ULL, active6, 0L, active7, 0L, active8, 0x200ULL);
      case 78:
      case 110:
         if ((active0 & 0x8000000000ULL) != 0L)
         {
            jjmatchedKind = 39;
            jjmatchedPos = 9;
         }
         else if ((active1 & 0x80ULL) != 0L)
            return jjStartNfaWithStates_0(9, 71, 154);
         return jjMoveStringLiteralDfa10_0(active0, 0x10000000000ULL, active1, 0L, active2, 0x2000000000000000ULL, active3, 0xf000000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0x200000000080ULL, active8, 0L);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa10_0(active0, 0x200000000000ULL, active1, 0L, active2, 0x800000000ULL, active3, 0x4000000000f04ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x400000ULL);
      case 80:
      case 112:
         if ((active6 & 0x10ULL) != 0L)
            return jjStartNfaWithStates_0(9, 388, 154);
         else if ((active8 & 0x100ULL) != 0L)
            return jjStartNfaWithStates_0(9, 520, 154);
         break;
      case 82:
      case 114:
         if ((active0 & 0x2000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 61, 154);
         else if ((active1 & 0x1000000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 100, 154);
         else if ((active4 & 0x1000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 304, 154);
         return jjMoveStringLiteralDfa10_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x40000000000000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0x2008000000000ULL, active8, 0L);
      case 83:
      case 115:
         if ((active0 & 0x4000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 14, 154);
         else if ((active0 & 0x8000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 27, 154);
         else if ((active2 & 0x1000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 152, 154);
         else if ((active2 & 0x2000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 153, 154);
         else if ((active3 & 0x2000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 205, 154);
         else if ((active4 & 0x40000000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 298, 154);
         return jjMoveStringLiteralDfa10_0(active0, 0L, active1, 0L, active2, 0x1000000000ULL, active3, 0x80000000000000ULL, active4, 0x80000000000000ULL, active5, 0L, active6, 0x8000ULL, active7, 0x20ULL, active8, 0L);
      case 84:
      case 116:
         if ((active0 & 0x1000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 12, 154);
         else if ((active5 & 0x8000000000000000ULL) != 0L)
         {
            jjmatchedKind = 383;
            jjmatchedPos = 9;
         }
         else if ((active6 & 0x10000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 400, 154);
         else if ((active7 & 0x1000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 496, 154);
         return jjMoveStringLiteralDfa10_0(active0, 0x1e0000000000ULL, active1, 0L, active2, 0L, active3, 0x8000000000ULL, active4, 0L, active5, 0x20004000000000ULL, active6, 0L, active7, 0L, active8, 0L);
      case 85:
      case 117:
         return jjMoveStringLiteralDfa10_0(active0, 0x1800000000ULL, active1, 0x6ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0x400000000ULL, active7, 0L, active8, 0L);
      case 86:
      case 118:
         return jjMoveStringLiteralDfa10_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0x4000000000000ULL, active7, 0L, active8, 0L);
      case 88:
      case 120:
         if ((active7 & 0x4ULL) != 0L)
            return jjStartNfaWithStates_0(9, 450, 154);
         break;
      case 89:
      case 121:
         if ((active2 & 0x10ULL) != 0L)
            return jjStartNfaWithStates_0(9, 132, 154);
         else if ((active8 & 0x2000000ULL) != 0L)
            return jjStartNfaWithStates_0(9, 537, 154);
         return jjMoveStringLiteralDfa10_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x8ULL);
      default :
         break;
   }
   return jjStartNfa_0(8, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa10_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active4 &= old4) | (active5 &= old5) | (active6 &= old6) | (active7 &= old7) | (active8 &= old8)) == 0L)
      return jjStartNfa_0(8, old0, old1, old2, old3, old4, old5, old6, old7, old8, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(9, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
      return 10;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa11_0(active0, 0xf0000000000ULL, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x6000000000ULL, active8, 0L);
      case 65:
      case 97:
         return jjMoveStringLiteralDfa11_0(active0, 0L, active1, 0L, active2, 0x2000000200000000ULL, active3, 0x40000000000000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0x80ULL, active8, 0L);
      case 67:
      case 99:
         return jjMoveStringLiteralDfa11_0(active0, 0x80000000ULL, active1, 0L, active2, 0x800000000ULL, active3, 0x80ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0x2000000000000ULL, active8, 0L);
      case 68:
      case 100:
         if ((active3 & 0x100000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 212, 154);
         break;
      case 69:
      case 101:
         if ((active0 & 0x400000000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 34, 154);
         else if ((active0 & 0x1000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 48, 154);
         else if ((active2 & 0x200000000000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 173, 154);
         else if ((active2 & 0x40000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 182, 154);
         else if ((active6 & 0x400000000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 418, 154);
         else if ((active6 & 0x4000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 434, 154);
         else if ((active8 & 0x10000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 528, 154);
         return jjMoveStringLiteralDfa11_0(active0, 0L, active1, 0L, active2, 0x400000000ULL, active3, 0x8000000f000000ULL, active4, 0x80000000000000ULL, active5, 0L, active6, 0L, active7, 0x8000000000ULL, active8, 0x20000000000ULL);
      case 70:
      case 102:
         return jjMoveStringLiteralDfa11_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x1000000000000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 71:
      case 103:
         if ((active7 & 0x200000000000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 493, 154);
         break;
      case 72:
      case 104:
         if ((active5 & 0x20000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 373, 154);
         return jjMoveStringLiteralDfa11_0(active0, 0L, active1, 0L, active2, 0x20000000000ULL, active3, 0x8000000010000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa11_0(active0, 0x10000000ULL, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0x4000000001ULL, active6, 0x4ULL, active7, 0L, active8, 0L);
      case 76:
      case 108:
         return jjMoveStringLiteralDfa11_0(active0, 0L, active1, 0x40000000ULL, active2, 0x20400000000000ULL, active3, 0x4000000000000ULL, active4, 0L, active5, 0x80000000000000ULL, active6, 0L, active7, 0L, active8, 0L);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa11_0(active0, 0x200000ULL, active1, 0L, active2, 0x1010000000400ULL, active3, 0x30000000008000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 78:
      case 110:
         if ((active3 & 0x100ULL) != 0L)
         {
            jjmatchedKind = 200;
            jjmatchedPos = 10;
         }
         else if ((active8 & 0x400000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 534, 154);
         return jjMoveStringLiteralDfa11_0(active0, 0xc005900000000ULL, active1, 0x400000000006ULL, active2, 0x800ULL, active3, 0xe00ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0x1000000000ULL, active8, 0L);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa11_0(active0, 0L, active1, 0L, active2, 0x1000ULL, active3, 0L, active4, 0x8000000000000800ULL, active5, 0L, active6, 0L, active7, 0L, active8, 0x10ULL);
      case 80:
      case 112:
         if ((active8 & 0x200ULL) != 0L)
            return jjStartNfaWithStates_0(10, 521, 154);
         return jjMoveStringLiteralDfa11_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x8ULL);
      case 81:
      case 113:
         return jjMoveStringLiteralDfa11_0(active0, 0L, active1, 0L, active2, 0x1000000000ULL, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 82:
      case 114:
         if ((active0 & 0x200000000000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 45, 154);
         else if ((active8 & 0x4000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 526, 154);
         return jjMoveStringLiteralDfa11_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x4ULL, active4, 0L, active5, 0x200000ULL, active6, 0L, active7, 0L, active8, 0x201000ULL);
      case 83:
      case 115:
         if ((active0 & 0x100000000000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 44, 154);
         else if ((active0 & 0x4000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 62, 154);
         return jjMoveStringLiteralDfa11_0(active0, 0x207800000ULL, active1, 0L, active2, 0xe000ULL, active3, 0L, active4, 0L, active5, 0x40000ULL, active6, 0L, active7, 0x1000000ULL, active8, 0L);
      case 84:
      case 116:
         if ((active8 & 0x400ULL) != 0L)
            return jjStartNfaWithStates_0(10, 522, 154);
         return jjMoveStringLiteralDfa11_0(active0, 0L, active1, 0x800001000000ULL, active2, 0x8000000000ULL, active3, 0x2c00000004000ULL, active4, 0L, active5, 0L, active6, 0x8000ULL, active7, 0x800020ULL, active8, 0L);
      case 88:
      case 120:
         return jjMoveStringLiteralDfa11_0(active0, 0L, active1, 0x1000000000000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 89:
      case 121:
         if ((active3 & 0x8000000000ULL) != 0L)
            return jjStartNfaWithStates_0(10, 231, 154);
         break;
      default :
         break;
   }
   return jjStartNfa_0(9, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa11_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active4 &= old4) | (active5 &= old5) | (active6 &= old6) | (active7 &= old7) | (active8 &= old8)) == 0L)
      return jjStartNfa_0(9, old0, old1, old2, old3, old4, old5, old6, old7, old8, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(10, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
      return 11;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa12_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x200ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0x1000000ULL, active8, 0L);
      case 65:
      case 97:
         if ((active2 & 0x1000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 176, 154);
         return jjMoveStringLiteralDfa12_0(active0, 0x180000000ULL, active1, 0L, active2, 0x8000000800ULL, active3, 0x1400000004000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0x20ULL, active8, 0L);
      case 67:
      case 99:
         return jjMoveStringLiteralDfa12_0(active0, 0x21a00000000ULL, active1, 0x6ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0x200000ULL, active6, 0L, active7, 0x2000000000ULL, active8, 0L);
      case 68:
      case 100:
         return jjMoveStringLiteralDfa12_0(active0, 0L, active1, 0L, active2, 0L, active3, 0xf000000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0x4000000000ULL, active8, 0L);
      case 69:
      case 101:
         if ((active0 & 0x200000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 21, 154);
         else if ((active1 & 0x40000000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 94, 154);
         else if ((active2 & 0x10000000000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 168, 154);
         else if ((active2 & 0x20000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 181, 154);
         else if ((active3 & 0x8000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 207, 154);
         else if ((active3 & 0x800000000000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 239, 154);
         else if ((active3 & 0x4000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 242, 154);
         else if ((active3 & 0x10000000000000ULL) != 0L)
         {
            jjmatchedKind = 244;
            jjmatchedPos = 11;
         }
         else if ((active8 & 0x8ULL) != 0L)
            return jjStartNfaWithStates_0(11, 515, 154);
         return jjMoveStringLiteralDfa12_0(active0, 0x3800000ULL, active1, 0x800000000000ULL, active2, 0x20000000000ULL, active3, 0x28000000010000ULL, active4, 0L, active5, 0x80000000000000ULL, active6, 0L, active7, 0x2000000000000ULL, active8, 0x201000ULL);
      case 71:
      case 103:
         return jjMoveStringLiteralDfa12_0(active0, 0L, active1, 0x400000000000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x8000000000ULL, active8, 0L);
      case 72:
      case 104:
         if ((active3 & 0x2000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 241, 154);
         else if ((active7 & 0x800000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 471, 154);
         break;
      case 73:
      case 105:
         return jjMoveStringLiteralDfa12_0(active0, 0L, active1, 0x1000000ULL, active2, 0L, active3, 0x4ULL, active4, 0L, active5, 0L, active6, 0x8000ULL, active7, 0L, active8, 0L);
      case 75:
      case 107:
         if ((active7 & 0x1000000000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 484, 154);
         break;
      case 76:
      case 108:
         return jjMoveStringLiteralDfa12_0(active0, 0L, active1, 0L, active2, 0x1000000000ULL, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x80ULL, active8, 0L);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa12_0(active0, 0L, active1, 0L, active2, 0x2000000000000000ULL, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 78:
      case 110:
         if ((active0 & 0x10000000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 28, 154);
         else if ((active4 & 0x800ULL) != 0L)
            return jjStartNfaWithStates_0(11, 267, 154);
         else if ((active8 & 0x10ULL) != 0L)
            return jjStartNfaWithStates_0(11, 516, 154);
         return jjMoveStringLiteralDfa12_0(active0, 0x50000000000ULL, active1, 0L, active2, 0x400000000ULL, active3, 0x40000000000000ULL, active4, 0L, active5, 0x1ULL, active6, 0x4ULL, active7, 0L, active8, 0L);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa12_0(active0, 0L, active1, 0L, active2, 0x400000000400ULL, active3, 0x80ULL, active4, 0L, active5, 0x4000000000ULL, active6, 0L, active7, 0L, active8, 0L);
      case 80:
      case 112:
         return jjMoveStringLiteralDfa12_0(active0, 0L, active1, 0L, active2, 0xe000ULL, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 82:
      case 114:
         if ((active3 & 0x80000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 247, 154);
         else if ((active4 & 0x80000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 311, 154);
         return jjMoveStringLiteralDfa12_0(active0, 0L, active1, 0L, active2, 0x200001000ULL, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 83:
      case 115:
         return jjMoveStringLiteralDfa12_0(active0, 0x80000000000ULL, active1, 0L, active2, 0L, active3, 0xc00ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 84:
      case 116:
         if ((active1 & 0x1000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 112, 154);
         else if ((active8 & 0x20000000000ULL) != 0L)
            return jjStartNfaWithStates_0(11, 553, 154);
         return jjMoveStringLiteralDfa12_0(active0, 0xc000004000000ULL, active1, 0L, active2, 0x800000000ULL, active3, 0L, active4, 0L, active5, 0x40000ULL, active6, 0L, active7, 0L, active8, 0L);
      case 85:
      case 117:
         return jjMoveStringLiteralDfa12_0(active0, 0x4000000000ULL, active1, 0L, active2, 0L, active3, 0L, active4, 0x8000000000000000ULL, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      default :
         break;
   }
   return jjStartNfa_0(10, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa12_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active4 &= old4) | (active5 &= old5) | (active6 &= old6) | (active7 &= old7) | (active8 &= old8)) == 0L)
      return jjStartNfa_0(10, old0, old1, old2, old3, old4, old5, old6, old7, old8, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(11, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
      return 12;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa13_0(active0, 0L, active1, 0L, active2, 0L, active3, 0xf000c00ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 65:
      case 97:
         return jjMoveStringLiteralDfa13_0(active0, 0x70000000000ULL, active1, 0L, active2, 0L, active3, 0x200ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 67:
      case 99:
         if ((active6 & 0x8000ULL) != 0L)
            return jjStartNfaWithStates_0(12, 399, 154);
         return jjMoveStringLiteralDfa13_0(active0, 0x80000000000ULL, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 68:
      case 100:
         return jjMoveStringLiteralDfa13_0(active0, 0L, active1, 0L, active2, 0x200001400ULL, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 69:
      case 101:
         if ((active2 & 0x2000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(12, 189, 154);
         return jjMoveStringLiteralDfa13_0(active0, 0xc000000000000ULL, active1, 0L, active2, 0x80000e000ULL, active3, 0L, active4, 0L, active5, 0x200000ULL, active6, 0L, active7, 0x8000000000ULL, active8, 0L);
      case 71:
      case 103:
         if ((active2 & 0x400000000000ULL) != 0L)
            return jjStartNfaWithStates_0(12, 174, 154);
         else if ((active6 & 0x4ULL) != 0L)
            return jjStartNfaWithStates_0(12, 386, 154);
         return jjMoveStringLiteralDfa13_0(active0, 0L, active1, 0L, active2, 0x400000000ULL, active3, 0x4ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0x201000ULL);
      case 72:
      case 104:
         return jjMoveStringLiteralDfa13_0(active0, 0x200000000ULL, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa13_0(active0, 0x4000000ULL, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x4000000080ULL, active8, 0L);
      case 76:
      case 108:
         return jjMoveStringLiteralDfa13_0(active0, 0L, active1, 0L, active2, 0x8000000000ULL, active3, 0x400000004000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa13_0(active0, 0x4100000000ULL, active1, 0L, active2, 0x20000000800ULL, active3, 0x8000000010000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0x20ULL, active8, 0L);
      case 78:
      case 110:
         if ((active5 & 0x4000000000ULL) != 0L)
            return jjStartNfaWithStates_0(12, 358, 154);
         return jjMoveStringLiteralDfa13_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0x80000000000000ULL, active6, 0L, active7, 0L, active8, 0L);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa13_0(active0, 0L, active1, 0x1000000ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0x40000ULL, active6, 0L, active7, 0x2000000000ULL, active8, 0L);
      case 80:
      case 112:
         return jjMoveStringLiteralDfa13_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x2000000000000ULL, active8, 0L);
      case 82:
      case 114:
         if ((active4 & 0x8000000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(12, 319, 154);
         return jjMoveStringLiteralDfa13_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0x1000000ULL, active8, 0L);
      case 83:
      case 115:
         return jjMoveStringLiteralDfa13_0(active0, 0L, active1, 0L, active2, 0x1000000000ULL, active3, 0x60000000000000ULL, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 84:
      case 116:
         return jjMoveStringLiteralDfa13_0(active0, 0x1883800000ULL, active1, 0xc00000000006ULL, active2, 0L, active3, 0L, active4, 0L, active5, 0L, active6, 0L, active7, 0L, active8, 0L);
      case 85:
      case 117:
         return jjMoveStringLiteralDfa13_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x1000000000080ULL, active4, 0L, active5, 0x1ULL, active6, 0L, active7, 0L, active8, 0L);
      default :
         break;
   }
   return jjStartNfa_0(11, active0, active1, active2, active3, active4, active5, active6, active7, active8, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa13_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old4, unsigned long long active4, unsigned long long old5, unsigned long long active5, unsigned long long old6, unsigned long long active6, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active4 &= old4) | (active5 &= old5) | (active6 &= old6) | (active7 &= old7) | (active8 &= old8)) == 0L)
      return jjStartNfa_0(11, old0, old1, old2, old3, old4, old5, old6, old7, old8, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(12, active0, active1, active2, active3, 0L, active5, 0L, active7, active8, 0L);
      return 13;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa14_0(active0, 0x3800000ULL, active1, 0x800000000000ULL, active2, 0L, active3, 0L, active5, 0L, active7, 0L, active8, 0L);
      case 65:
      case 97:
         if ((active2 & 0x20000000000ULL) != 0L)
            return jjStartNfaWithStates_0(13, 169, 154);
         else if ((active3 & 0x10000ULL) != 0L)
            return jjStartNfaWithStates_0(13, 208, 154);
         else if ((active3 & 0x8000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(13, 243, 154);
         return jjMoveStringLiteralDfa14_0(active0, 0x80000000ULL, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0L, active8, 0L);
      case 66:
      case 98:
         return jjMoveStringLiteralDfa14_0(active0, 0x4000000000ULL, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0L, active8, 0L);
      case 67:
      case 99:
         return jjMoveStringLiteralDfa14_0(active0, 0x4000000ULL, active1, 0L, active2, 0xe000ULL, active3, 0x600ULL, active5, 0L, active7, 0L, active8, 0L);
      case 69:
      case 101:
         if ((active0 & 0x100000000ULL) != 0L)
            return jjStartNfaWithStates_0(13, 32, 154);
         else if ((active2 & 0x400ULL) != 0L)
            return jjStartNfaWithStates_0(13, 138, 154);
         else if ((active2 & 0x800ULL) != 0L)
            return jjStartNfaWithStates_0(13, 139, 154);
         return jjMoveStringLiteralDfa14_0(active0, 0x200000000ULL, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0x1000000ULL, active8, 0x201000ULL);
      case 70:
      case 102:
         return jjMoveStringLiteralDfa14_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x40000000000000ULL, active5, 0L, active7, 0L, active8, 0L);
      case 71:
      case 103:
         return jjMoveStringLiteralDfa14_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active5, 0x80000000040000ULL, active7, 0L, active8, 0L);
      case 72:
      case 104:
         if ((active1 & 0x400000000000ULL) != 0L)
            return jjStartNfaWithStates_0(13, 110, 154);
         return jjMoveStringLiteralDfa14_0(active0, 0x80000000000ULL, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0L, active8, 0L);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa14_0(active0, 0x1800000000ULL, active1, 0x6ULL, active2, 0x200001000ULL, active3, 0x4ULL, active5, 0L, active7, 0L, active8, 0L);
      case 76:
      case 108:
         return jjMoveStringLiteralDfa14_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x1000000000000ULL, active5, 0L, active7, 0L, active8, 0L);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa14_0(active0, 0x50000000000ULL, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0L, active8, 0L);
      case 78:
      case 110:
         if ((active1 & 0x1000000ULL) != 0L)
            return jjStartNfaWithStates_0(13, 88, 154);
         return jjMoveStringLiteralDfa14_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x80ULL, active5, 0x200000ULL, active7, 0x2000000000ULL, active8, 0L);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa14_0(active0, 0L, active1, 0L, active2, 0x8000000000ULL, active3, 0x400000004000ULL, active5, 0L, active7, 0L, active8, 0L);
      case 80:
      case 112:
         if ((active7 & 0x20ULL) != 0L)
            return jjStartNfaWithStates_0(13, 453, 154);
         break;
      case 82:
      case 114:
         return jjMoveStringLiteralDfa14_0(active0, 0xc000000000000ULL, active1, 0L, active2, 0L, active3, 0x800ULL, active5, 0L, active7, 0L, active8, 0L);
      case 83:
      case 115:
         return jjMoveStringLiteralDfa14_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0x4000000000ULL, active8, 0L);
      case 84:
      case 116:
         if ((active7 & 0x2000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(13, 497, 154);
         return jjMoveStringLiteralDfa14_0(active0, 0x20000000000ULL, active1, 0L, active2, 0x1c00000000ULL, active3, 0x2000000f000000ULL, active5, 0x1ULL, active7, 0x80ULL, active8, 0L);
      case 88:
      case 120:
         if ((active7 & 0x8000000000ULL) != 0L)
            return jjStartNfaWithStates_0(13, 487, 154);
         break;
      default :
         break;
   }
   return jjStartNfa_0(12, active0, active1, active2, active3, 0L, active5, 0L, active7, active8, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa14_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old5, unsigned long long active5, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active5 &= old5) | (active7 &= old7) | (active8 &= old8)) == 0L)
      return jjStartNfa_0(12, old0, old1, old2, old3, 0L, old5, 0L, old7, old8, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(13, active0, active1, active2, active3, 0L, active5, 0L, active7, active8, 0L);
      return 14;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa15_0(active0, 0L, active1, 0L, active2, 0x800000000ULL, active3, 0L, active5, 0L, active7, 0L, active8, 0L);
      case 65:
      case 97:
         return jjMoveStringLiteralDfa15_0(active0, 0x20000000000ULL, active1, 0L, active2, 0x1000000000ULL, active3, 0x20000000000000ULL, active5, 0L, active7, 0L, active8, 0L);
      case 67:
      case 99:
         if ((active7 & 0x4000000000ULL) != 0L)
            return jjStartNfaWithStates_0(14, 486, 154);
         return jjMoveStringLiteralDfa15_0(active0, 0x800000ULL, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0L, active8, 0L);
      case 69:
      case 101:
         if ((active0 & 0x10000000000ULL) != 0L)
            return jjStartNfaWithStates_0(14, 40, 154);
         else if ((active0 & 0x40000000000ULL) != 0L)
            return jjStartNfaWithStates_0(14, 42, 154);
         else if ((active5 & 0x1ULL) != 0L)
            return jjStartNfaWithStates_0(14, 320, 154);
         return jjMoveStringLiteralDfa15_0(active0, 0x84000000000ULL, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0L, active8, 0L);
      case 71:
      case 103:
         if ((active2 & 0x8000000000ULL) != 0L)
            return jjStartNfaWithStates_0(14, 167, 154);
         else if ((active3 & 0x4000ULL) != 0L)
            return jjStartNfaWithStates_0(14, 206, 154);
         else if ((active3 & 0x400000000000ULL) != 0L)
            return jjStartNfaWithStates_0(14, 238, 154);
         return jjMoveStringLiteralDfa15_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0x1000000ULL, active8, 0L);
      case 72:
      case 104:
         if ((active2 & 0x400000000ULL) != 0L)
            return jjStartNfaWithStates_0(14, 162, 154);
         break;
      case 73:
      case 105:
         return jjMoveStringLiteralDfa15_0(active0, 0L, active1, 0L, active2, 0xe000ULL, active3, 0L, active5, 0L, active7, 0L, active8, 0L);
      case 76:
      case 108:
         return jjMoveStringLiteralDfa15_0(active0, 0x80000000ULL, active1, 0x800000000000ULL, active2, 0L, active3, 0L, active5, 0L, active7, 0L, active8, 0L);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa15_0(active0, 0x200000000ULL, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0L, active8, 0L);
      case 78:
      case 110:
         if ((active3 & 0x4ULL) != 0L)
            return jjStartNfaWithStates_0(14, 194, 154);
         return jjMoveStringLiteralDfa15_0(active0, 0x1000000ULL, active1, 0L, active2, 0x200001000ULL, active3, 0L, active5, 0L, active7, 0L, active8, 0L);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa15_0(active0, 0x1800000000ULL, active1, 0x6ULL, active2, 0L, active3, 0x40000000000c00ULL, active5, 0L, active7, 0L, active8, 0L);
      case 82:
      case 114:
         return jjMoveStringLiteralDfa15_0(active0, 0L, active1, 0L, active2, 0L, active3, 0L, active5, 0x40000ULL, active7, 0L, active8, 0L);
      case 83:
      case 115:
         if ((active0 & 0x4000000ULL) != 0L)
            return jjStartNfaWithStates_0(14, 26, 154);
         return jjMoveStringLiteralDfa15_0(active0, 0x2000000ULL, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0L, active8, 0L);
      case 84:
      case 116:
         if ((active3 & 0x80ULL) != 0L)
            return jjStartNfaWithStates_0(14, 199, 154);
         else if ((active7 & 0x2000000000ULL) != 0L)
            return jjStartNfaWithStates_0(14, 485, 154);
         return jjMoveStringLiteralDfa15_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x1000000000200ULL, active5, 0x80000000200000ULL, active7, 0L, active8, 0L);
      case 86:
      case 118:
         return jjMoveStringLiteralDfa15_0(active0, 0xc000000000000ULL, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0L, active8, 0L);
      case 88:
      case 120:
         if ((active8 & 0x1000ULL) != 0L)
            return jjStartNfaWithStates_0(14, 524, 154);
         else if ((active8 & 0x200000ULL) != 0L)
            return jjStartNfaWithStates_0(14, 533, 154);
         break;
      case 89:
      case 121:
         if ((active7 & 0x80ULL) != 0L)
            return jjStartNfaWithStates_0(14, 455, 154);
         return jjMoveStringLiteralDfa15_0(active0, 0L, active1, 0L, active2, 0L, active3, 0xf000000ULL, active5, 0L, active7, 0L, active8, 0L);
      default :
         break;
   }
   return jjStartNfa_0(13, active0, active1, active2, active3, 0L, active5, 0L, active7, active8, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa15_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old5, unsigned long long active5, unsigned long long old7, unsigned long long active7, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active5 &= old5) | (active7 &= old7) | (active8 &= old8)) == 0L)
      return jjStartNfa_0(13, old0, old1, old2, old3, 0L, old5, 0L, old7, old8, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(14, active0, active1, active2, active3, 0L, active5, 0L, active7, 0L, 0L);
      return 15;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa16_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x1000000000000ULL, active5, 0L, active7, 0L);
      case 65:
      case 97:
         if ((active0 & 0x200000000ULL) != 0L)
            return jjStartNfaWithStates_0(15, 33, 154);
         return jjMoveStringLiteralDfa16_0(active0, 0xc000001800000ULL, active1, 0L, active2, 0x200001000ULL, active3, 0L, active5, 0x40000ULL, active7, 0L);
      case 67:
      case 99:
         return jjMoveStringLiteralDfa16_0(active0, 0x2000000ULL, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0L);
      case 69:
      case 101:
         return jjMoveStringLiteralDfa16_0(active0, 0L, active1, 0x800000000000ULL, active2, 0L, active3, 0L, active5, 0L, active7, 0x1000000ULL);
      case 70:
      case 102:
         return jjMoveStringLiteralDfa16_0(active0, 0L, active1, 0L, active2, 0xe000ULL, active3, 0L, active5, 0L, active7, 0L);
      case 72:
      case 104:
         if ((active5 & 0x80000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(15, 375, 154);
         break;
      case 73:
      case 105:
         return jjMoveStringLiteralDfa16_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x200ULL, active5, 0x200000ULL, active7, 0L);
      case 76:
      case 108:
         return jjMoveStringLiteralDfa16_0(active0, 0x20000000000ULL, active1, 0L, active2, 0x800000000ULL, active3, 0x800ULL, active5, 0L, active7, 0L);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa16_0(active0, 0x80000000000ULL, active1, 0L, active2, 0L, active3, 0x20000000000400ULL, active5, 0L, active7, 0L);
      case 78:
      case 110:
         if ((active0 & 0x800000000ULL) != 0L)
         {
            jjmatchedKind = 35;
            jjmatchedPos = 15;
         }
         else if ((active1 & 0x2ULL) != 0L)
         {
            jjmatchedKind = 65;
            jjmatchedPos = 15;
         }
         return jjMoveStringLiteralDfa16_0(active0, 0x1000000000ULL, active1, 0x4ULL, active2, 0L, active3, 0L, active5, 0L, active7, 0L);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa16_0(active0, 0x80000000ULL, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0L);
      case 80:
      case 112:
         return jjMoveStringLiteralDfa16_0(active0, 0L, active1, 0L, active2, 0L, active3, 0xf000000ULL, active5, 0L, active7, 0L);
      case 82:
      case 114:
         if ((active0 & 0x4000000000ULL) != 0L)
            return jjStartNfaWithStates_0(15, 38, 154);
         return jjMoveStringLiteralDfa16_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x40000000000000ULL, active5, 0L, active7, 0L);
      case 84:
      case 116:
         return jjMoveStringLiteralDfa16_0(active0, 0L, active1, 0L, active2, 0x1000000000ULL, active3, 0L, active5, 0L, active7, 0L);
      default :
         break;
   }
   return jjStartNfa_0(14, active0, active1, active2, active3, 0L, active5, 0L, active7, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa16_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old5, unsigned long long active5, unsigned long long old7, unsigned long long active7){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active5 &= old5) | (active7 &= old7)) == 0L)
      return jjStartNfa_0(14, old0, old1, old2, old3, 0L, old5, 0L, old7, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(15, active0, active1, active2, active3, 0L, active5, 0L, active7, 0L, 0L);
      return 16;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa17_0(active0, 0x1000000000ULL, active1, 0x4ULL, active2, 0L, active3, 0L, active5, 0L, active7, 0L);
      case 65:
      case 97:
         if ((active0 & 0x80000000000ULL) != 0L)
            return jjStartNfaWithStates_0(16, 43, 154);
         break;
      case 69:
      case 101:
         if ((active2 & 0x1000000000ULL) != 0L)
            return jjStartNfaWithStates_0(16, 164, 154);
         return jjMoveStringLiteralDfa17_0(active0, 0L, active1, 0L, active2, 0x800000000ULL, active3, 0xf000000ULL, active5, 0L, active7, 0L);
      case 71:
      case 103:
         if ((active0 & 0x80000000ULL) != 0L)
            return jjStartNfaWithStates_0(16, 31, 154);
         break;
      case 72:
      case 104:
         return jjMoveStringLiteralDfa17_0(active0, 0x2000000ULL, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0L);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa17_0(active0, 0L, active1, 0L, active2, 0xe000ULL, active3, 0L, active5, 0L, active7, 0L);
      case 76:
      case 108:
         return jjMoveStringLiteralDfa17_0(active0, 0xc000000000000ULL, active1, 0L, active2, 0x200001000ULL, active3, 0x800ULL, active5, 0x200000ULL, active7, 0L);
      case 77:
      case 109:
         if ((active5 & 0x40000ULL) != 0L)
            return jjStartNfaWithStates_0(16, 338, 154);
         return jjMoveStringLiteralDfa17_0(active0, 0x1000000ULL, active1, 0L, active2, 0L, active3, 0x40000000000400ULL, active5, 0L, active7, 0L);
      case 78:
      case 110:
         return jjMoveStringLiteralDfa17_0(active0, 0L, active1, 0x800000000000ULL, active2, 0L, active3, 0L, active5, 0L, active7, 0L);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa17_0(active0, 0x20000000000ULL, active1, 0L, active2, 0L, active3, 0L, active5, 0L, active7, 0L);
      case 80:
      case 112:
         if ((active3 & 0x20000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(16, 245, 154);
         break;
      case 84:
      case 116:
         return jjMoveStringLiteralDfa17_0(active0, 0x800000ULL, active1, 0L, active2, 0L, active3, 0x1000000000000ULL, active5, 0L, active7, 0L);
      case 86:
      case 118:
         return jjMoveStringLiteralDfa17_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x200ULL, active5, 0L, active7, 0L);
      case 88:
      case 120:
         if ((active7 & 0x1000000ULL) != 0L)
            return jjStartNfaWithStates_0(16, 472, 154);
         break;
      default :
         break;
   }
   return jjStartNfa_0(15, active0, active1, active2, active3, 0L, active5, 0L, active7, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa17_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old5, unsigned long long active5, unsigned long long old7, unsigned long long active7){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active5 &= old5) | (active7 &= old7)) == 0L)
      return jjStartNfa_0(15, old0, old1, old2, old3, 0L, old5, 0L, old7, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(16, active0, active1, active2, active3, 0L, active5, 0L, 0L, 0L, 0L);
      return 17;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa18_0(active0, 0xc000000000000ULL, active1, 0L, active2, 0x1000ULL, active3, 0x4000000f000000ULL, active5, 0L);
      case 65:
      case 97:
         return jjMoveStringLiteralDfa18_0(active0, 0x800000ULL, active1, 0L, active2, 0L, active3, 0L, active5, 0L);
      case 67:
      case 99:
         return jjMoveStringLiteralDfa18_0(active0, 0x1000000000ULL, active1, 0x4ULL, active2, 0xe000ULL, active3, 0L, active5, 0L);
      case 69:
      case 101:
         if ((active0 & 0x1000000ULL) != 0L)
            return jjStartNfaWithStates_0(17, 24, 154);
         else if ((active3 & 0x200ULL) != 0L)
            return jjStartNfaWithStates_0(17, 201, 154);
         else if ((active5 & 0x200000ULL) != 0L)
            return jjStartNfaWithStates_0(17, 341, 154);
         return jjMoveStringLiteralDfa18_0(active0, 0x2000000ULL, active1, 0L, active2, 0L, active3, 0x800ULL, active5, 0L);
      case 71:
      case 103:
         if ((active0 & 0x20000000000ULL) != 0L)
            return jjStartNfaWithStates_0(17, 41, 154);
         return jjMoveStringLiteralDfa18_0(active0, 0L, active1, 0x800000000000ULL, active2, 0L, active3, 0L, active5, 0L);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa18_0(active0, 0L, active1, 0L, active2, 0x200000000ULL, active3, 0x400ULL, active5, 0L);
      case 78:
      case 110:
         return jjMoveStringLiteralDfa18_0(active0, 0L, active1, 0L, active2, 0x800000000ULL, active3, 0L, active5, 0L);
      case 82:
      case 114:
         return jjMoveStringLiteralDfa18_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x1000000000000ULL, active5, 0L);
      default :
         break;
   }
   return jjStartNfa_0(16, active0, active1, active2, active3, 0L, active5, 0L, 0L, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa18_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3, unsigned long long old5, unsigned long long active5){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3) | (active5 &= old5)) == 0L)
      return jjStartNfa_0(16, old0, old1, old2, old3, 0L, old5, 0L, 0L, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(17, active0, active1, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
      return 18;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa19_0(active0, 0L, active1, 0L, active2, 0xe000ULL, active3, 0L);
      case 65:
      case 97:
         return jjMoveStringLiteralDfa19_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x1000000000000ULL);
      case 67:
      case 99:
         return jjMoveStringLiteralDfa19_0(active0, 0x4000000000000ULL, active1, 0L, active2, 0L, active3, 0x3000000ULL);
      case 68:
      case 100:
         return jjMoveStringLiteralDfa19_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x800ULL);
      case 71:
      case 103:
         return jjMoveStringLiteralDfa19_0(active0, 0L, active1, 0L, active2, 0x800000000ULL, active3, 0x40000000000000ULL);
      case 76:
      case 108:
         return jjMoveStringLiteralDfa19_0(active0, 0x800000ULL, active1, 0L, active2, 0L, active3, 0L);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa19_0(active0, 0x2000000ULL, active1, 0L, active2, 0L, active3, 0L);
      case 78:
      case 110:
         return jjMoveStringLiteralDfa19_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x4000000ULL);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa19_0(active0, 0x1000000000ULL, active1, 0x4ULL, active2, 0L, active3, 0L);
      case 80:
      case 112:
         return jjMoveStringLiteralDfa19_0(active0, 0x8000000000000ULL, active1, 0L, active2, 0x1000ULL, active3, 0L);
      case 83:
      case 115:
         return jjMoveStringLiteralDfa19_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x8000000ULL);
      case 84:
      case 116:
         return jjMoveStringLiteralDfa19_0(active0, 0L, active1, 0x800000000000ULL, active2, 0x200000000ULL, active3, 0x400ULL);
      default :
         break;
   }
   return jjStartNfa_0(17, active0, active1, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa19_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3)) == 0L)
      return jjStartNfa_0(17, old0, old1, old2, old3, 0L, 0L, 0L, 0L, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(18, active0, active1, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
      return 19;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa20_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x800ULL);
      case 65:
      case 97:
         if ((active0 & 0x2000000ULL) != 0L)
            return jjStartNfaWithStates_0(19, 25, 154);
         return jjMoveStringLiteralDfa20_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x5000000ULL);
      case 67:
      case 99:
         return jjMoveStringLiteralDfa20_0(active0, 0L, active1, 0L, active2, 0x2000ULL, active3, 0x8000000ULL);
      case 68:
      case 100:
         return jjMoveStringLiteralDfa20_0(active0, 0x1000000000ULL, active1, 0x4ULL, active2, 0L, active3, 0L);
      case 72:
      case 104:
         if ((active1 & 0x800000000000ULL) != 0L)
            return jjStartNfaWithStates_0(19, 111, 154);
         break;
      case 78:
      case 110:
         return jjMoveStringLiteralDfa20_0(active0, 0L, active1, 0L, active2, 0x4000ULL, active3, 0x1000000000000ULL);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa20_0(active0, 0x4000000800000ULL, active1, 0L, active2, 0x1000ULL, active3, 0x2000000ULL);
      case 82:
      case 114:
         return jjMoveStringLiteralDfa20_0(active0, 0x8000000000000ULL, active1, 0L, active2, 0L, active3, 0x40000000000000ULL);
      case 83:
      case 115:
         return jjMoveStringLiteralDfa20_0(active0, 0L, active1, 0L, active2, 0x8000ULL, active3, 0L);
      case 84:
      case 116:
         return jjMoveStringLiteralDfa20_0(active0, 0L, active1, 0L, active2, 0x800000000ULL, active3, 0x400ULL);
      case 89:
      case 121:
         if ((active2 & 0x200000000ULL) != 0L)
            return jjStartNfaWithStates_0(19, 161, 154);
         break;
      default :
         break;
   }
   return jjStartNfa_0(18, active0, active1, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa20_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3)) == 0L)
      return jjStartNfa_0(18, old0, old1, old2, old3, 0L, 0L, 0L, 0L, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(19, active0, active1, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
      return 20;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 65:
      case 97:
         return jjMoveStringLiteralDfa21_0(active0, 0L, active1, 0L, active2, 0x6000ULL, active3, 0L);
      case 66:
      case 98:
         return jjMoveStringLiteralDfa21_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x800ULL);
      case 67:
      case 99:
         return jjMoveStringLiteralDfa21_0(active0, 0L, active1, 0L, active2, 0x8000ULL, active3, 0L);
      case 68:
      case 100:
         return jjMoveStringLiteralDfa21_0(active0, 0x4000000000000ULL, active1, 0L, active2, 0L, active3, 0x2000000ULL);
      case 69:
      case 101:
         if ((active0 & 0x1000000000ULL) != 0L)
            return jjStartNfaWithStates_0(20, 36, 154);
         else if ((active1 & 0x4ULL) != 0L)
            return jjStartNfaWithStates_0(20, 66, 154);
         return jjMoveStringLiteralDfa21_0(active0, 0x8000000000000ULL, active1, 0L, active2, 0L, active3, 0x400ULL);
      case 71:
      case 103:
         if ((active0 & 0x800000ULL) != 0L)
            return jjStartNfaWithStates_0(20, 23, 154);
         break;
      case 72:
      case 104:
         if ((active2 & 0x800000000ULL) != 0L)
            return jjStartNfaWithStates_0(20, 163, 154);
         return jjMoveStringLiteralDfa21_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x8000000ULL);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa21_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x4000000ULL);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa21_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x40000000000000ULL);
      case 83:
      case 115:
         return jjMoveStringLiteralDfa21_0(active0, 0L, active1, 0L, active2, 0x1000ULL, active3, 0x1000000000000ULL);
      case 84:
      case 116:
         return jjMoveStringLiteralDfa21_0(active0, 0L, active1, 0L, active2, 0L, active3, 0x1000000ULL);
      default :
         break;
   }
   return jjStartNfa_0(19, active0, active1, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa21_0(unsigned long long old0, unsigned long long active0, unsigned long long old1, unsigned long long active1, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3){
   if (((active0 &= old0) | (active1 &= old1) | (active2 &= old2) | (active3 &= old3)) == 0L)
      return jjStartNfa_0(19, old0, old1, old2, old3, 0L, 0L, 0L, 0L, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(20, active0, 0L, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
      return 21;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 65:
      case 97:
         return jjMoveStringLiteralDfa22_0(active0, 0L, active2, 0L, active3, 0x1000800ULL);
      case 67:
      case 99:
         return jjMoveStringLiteralDfa22_0(active0, 0x8000000000000ULL, active2, 0L, active3, 0L);
      case 68:
      case 100:
         if ((active3 & 0x400ULL) != 0L)
            return jjStartNfaWithStates_0(21, 202, 154);
         break;
      case 69:
      case 101:
         if ((active0 & 0x4000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(21, 50, 154);
         else if ((active3 & 0x2000000ULL) != 0L)
            return jjStartNfaWithStates_0(21, 217, 154);
         else if ((active3 & 0x4000000ULL) != 0L)
            return jjStartNfaWithStates_0(21, 218, 154);
         return jjMoveStringLiteralDfa22_0(active0, 0L, active2, 0L, active3, 0x8000000ULL);
      case 70:
      case 102:
         return jjMoveStringLiteralDfa22_0(active0, 0L, active2, 0L, active3, 0x1000000000000ULL);
      case 72:
      case 104:
         return jjMoveStringLiteralDfa22_0(active0, 0L, active2, 0x8000ULL, active3, 0L);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa22_0(active0, 0L, active2, 0x1000ULL, active3, 0L);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa22_0(active0, 0L, active2, 0x4000ULL, active3, 0L);
      case 84:
      case 116:
         return jjMoveStringLiteralDfa22_0(active0, 0L, active2, 0x2000ULL, active3, 0L);
      case 85:
      case 117:
         return jjMoveStringLiteralDfa22_0(active0, 0L, active2, 0L, active3, 0x40000000000000ULL);
      default :
         break;
   }
   return jjStartNfa_0(20, active0, 0L, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa22_0(unsigned long long old0, unsigned long long active0, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3){
   if (((active0 &= old0) | (active2 &= old2) | (active3 &= old3)) == 0L)
      return jjStartNfa_0(20, old0, 0L, old2, old3, 0L, 0L, 0L, 0L, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(21, active0, 0L, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
      return 22;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 65:
      case 97:
         return jjMoveStringLiteralDfa23_0(active0, 0L, active2, 0x2000ULL, active3, 0L);
      case 67:
      case 99:
         return jjMoveStringLiteralDfa23_0(active0, 0L, active2, 0L, active3, 0x800ULL);
      case 69:
      case 101:
         if ((active2 & 0x4000ULL) != 0L)
            return jjStartNfaWithStates_0(22, 142, 154);
         return jjMoveStringLiteralDfa23_0(active0, 0L, active2, 0x8000ULL, active3, 0L);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa23_0(active0, 0x8000000000000ULL, active2, 0L, active3, 0L);
      case 76:
      case 108:
         return jjMoveStringLiteralDfa23_0(active0, 0L, active2, 0L, active3, 0x1000000ULL);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa23_0(active0, 0L, active2, 0L, active3, 0x8000000ULL);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa23_0(active0, 0L, active2, 0L, active3, 0x1000000000000ULL);
      case 80:
      case 112:
         return jjMoveStringLiteralDfa23_0(active0, 0L, active2, 0L, active3, 0x40000000000000ULL);
      case 84:
      case 116:
         return jjMoveStringLiteralDfa23_0(active0, 0L, active2, 0x1000ULL, active3, 0L);
      default :
         break;
   }
   return jjStartNfa_0(21, active0, 0L, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa23_0(unsigned long long old0, unsigned long long active0, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3){
   if (((active0 &= old0) | (active2 &= old2) | (active3 &= old3)) == 0L)
      return jjStartNfa_0(21, old0, 0L, old2, old3, 0L, 0L, 0L, 0L, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(22, active0, 0L, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
      return 23;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa24_0(active0, 0L, active2, 0L, active3, 0x40000000000000ULL);
      case 65:
      case 97:
         if ((active3 & 0x8000000ULL) != 0L)
            return jjStartNfaWithStates_0(23, 219, 154);
         break;
      case 73:
      case 105:
         return jjMoveStringLiteralDfa24_0(active0, 0L, active2, 0x1000ULL, active3, 0L);
      case 75:
      case 107:
         if ((active3 & 0x800ULL) != 0L)
            return jjStartNfaWithStates_0(23, 203, 154);
         break;
      case 76:
      case 108:
         return jjMoveStringLiteralDfa24_0(active0, 0L, active2, 0x2000ULL, active3, 0L);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa24_0(active0, 0L, active2, 0x8000ULL, active3, 0L);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa24_0(active0, 0L, active2, 0L, active3, 0x1000000ULL);
      case 82:
      case 114:
         return jjMoveStringLiteralDfa24_0(active0, 0L, active2, 0L, active3, 0x1000000000000ULL);
      case 83:
      case 115:
         return jjMoveStringLiteralDfa24_0(active0, 0x8000000000000ULL, active2, 0L, active3, 0L);
      default :
         break;
   }
   return jjStartNfa_0(22, active0, 0L, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa24_0(unsigned long long old0, unsigned long long active0, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3){
   if (((active0 &= old0) | (active2 &= old2) | (active3 &= old3)) == 0L)
      return jjStartNfa_0(22, old0, 0L, old2, old3, 0L, 0L, 0L, 0L, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(23, active0, 0L, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
      return 24;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 65:
      case 97:
         if ((active2 & 0x8000ULL) != 0L)
            return jjStartNfaWithStates_0(24, 143, 154);
         break;
      case 70:
      case 102:
         return jjMoveStringLiteralDfa25_0(active0, 0L, active2, 0L, active3, 0x40000000000000ULL);
      case 71:
      case 103:
         if ((active3 & 0x1000000ULL) != 0L)
            return jjStartNfaWithStates_0(24, 216, 154);
         break;
      case 73:
      case 105:
         return jjMoveStringLiteralDfa25_0(active0, 0x8000000000000ULL, active2, 0L, active3, 0L);
      case 77:
      case 109:
         return jjMoveStringLiteralDfa25_0(active0, 0L, active2, 0L, active3, 0x1000000000000ULL);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa25_0(active0, 0L, active2, 0x3000ULL, active3, 0L);
      default :
         break;
   }
   return jjStartNfa_0(23, active0, 0L, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa25_0(unsigned long long old0, unsigned long long active0, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3){
   if (((active0 &= old0) | (active2 &= old2) | (active3 &= old3)) == 0L)
      return jjStartNfa_0(23, old0, 0L, old2, old3, 0L, 0L, 0L, 0L, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(24, active0, 0L, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
      return 25;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa26_0(active0, 0L, active2, 0L, active3, 0x1000000000000ULL);
      case 71:
      case 103:
         if ((active2 & 0x2000ULL) != 0L)
            return jjStartNfaWithStates_0(25, 141, 154);
         break;
      case 78:
      case 110:
         if ((active2 & 0x1000ULL) != 0L)
            return jjStartNfaWithStates_0(25, 140, 154);
         break;
      case 79:
      case 111:
         return jjMoveStringLiteralDfa26_0(active0, 0x8000000000000ULL, active2, 0L, active3, 0x40000000000000ULL);
      default :
         break;
   }
   return jjStartNfa_0(24, active0, 0L, active2, active3, 0L, 0L, 0L, 0L, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa26_0(unsigned long long old0, unsigned long long active0, unsigned long long old2, unsigned long long active2, unsigned long long old3, unsigned long long active3){
   if (((active0 &= old0) | (active2 &= old2) | (active3 &= old3)) == 0L)
      return jjStartNfa_0(24, old0, 0L, old2, old3, 0L, 0L, 0L, 0L, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(25, active0, 0L, 0L, active3, 0L, 0L, 0L, 0L, 0L, 0L);
      return 26;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 71:
      case 103:
         return jjMoveStringLiteralDfa27_0(active0, 0L, active3, 0x1000000000000ULL);
      case 78:
      case 110:
         if ((active0 & 0x8000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(26, 51, 154);
         break;
      case 82:
      case 114:
         return jjMoveStringLiteralDfa27_0(active0, 0L, active3, 0x40000000000000ULL);
      default :
         break;
   }
   return jjStartNfa_0(25, active0, 0L, 0L, active3, 0L, 0L, 0L, 0L, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa27_0(unsigned long long old0, unsigned long long active0, unsigned long long old3, unsigned long long active3){
   if (((active0 &= old0) | (active3 &= old3)) == 0L)
      return jjStartNfa_0(25, old0, 0L, 0L, old3, 0L, 0L, 0L, 0L, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(26, 0L, 0L, 0L, active3, 0L, 0L, 0L, 0L, 0L, 0L);
      return 27;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 95:
         return jjMoveStringLiteralDfa28_0(active3, 0x40000000000000ULL);
      case 82:
      case 114:
         return jjMoveStringLiteralDfa28_0(active3, 0x1000000000000ULL);
      default :
         break;
   }
   return jjStartNfa_0(26, 0L, 0L, 0L, active3, 0L, 0L, 0L, 0L, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa28_0(unsigned long long old3, unsigned long long active3){
   if (((active3 &= old3)) == 0L)
      return jjStartNfa_0(26, 0L, 0L, 0L, old3, 0L, 0L, 0L, 0L, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(27, 0L, 0L, 0L, active3, 0L, 0L, 0L, 0L, 0L, 0L);
      return 28;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 79:
      case 111:
         return jjMoveStringLiteralDfa29_0(active3, 0x1000000000000ULL);
      case 84:
      case 116:
         return jjMoveStringLiteralDfa29_0(active3, 0x40000000000000ULL);
      default :
         break;
   }
   return jjStartNfa_0(27, 0L, 0L, 0L, active3, 0L, 0L, 0L, 0L, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa29_0(unsigned long long old3, unsigned long long active3){
   if (((active3 &= old3)) == 0L)
      return jjStartNfa_0(27, 0L, 0L, 0L, old3, 0L, 0L, 0L, 0L, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(28, 0L, 0L, 0L, active3, 0L, 0L, 0L, 0L, 0L, 0L);
      return 29;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 85:
      case 117:
         return jjMoveStringLiteralDfa30_0(active3, 0x1000000000000ULL);
      case 89:
      case 121:
         return jjMoveStringLiteralDfa30_0(active3, 0x40000000000000ULL);
      default :
         break;
   }
   return jjStartNfa_0(28, 0L, 0L, 0L, active3, 0L, 0L, 0L, 0L, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa30_0(unsigned long long old3, unsigned long long active3){
   if (((active3 &= old3)) == 0L)
      return jjStartNfa_0(28, 0L, 0L, 0L, old3, 0L, 0L, 0L, 0L, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(29, 0L, 0L, 0L, active3, 0L, 0L, 0L, 0L, 0L, 0L);
      return 30;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 80:
      case 112:
         if ((active3 & 0x1000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(30, 240, 154);
         return jjMoveStringLiteralDfa31_0(active3, 0x40000000000000ULL);
      default :
         break;
   }
   return jjStartNfa_0(29, 0L, 0L, 0L, active3, 0L, 0L, 0L, 0L, 0L, 0L);
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa31_0(unsigned long long old3, unsigned long long active3){
   if (((active3 &= old3)) == 0L)
      return jjStartNfa_0(29, 0L, 0L, 0L, old3, 0L, 0L, 0L, 0L, 0L, 0L);
   if (input_stream->endOfInput()) {
      jjStopStringLiteralDfa_0(30, 0L, 0L, 0L, active3, 0L, 0L, 0L, 0L, 0L, 0L);
      return 31;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 69:
      case 101:
         if ((active3 & 0x40000000000000ULL) != 0L)
            return jjStartNfaWithStates_0(31, 246, 154);
         break;
      default :
         break;
   }
   return jjStartNfa_0(30, 0L, 0L, 0L, active3, 0L, 0L, 0L, 0L, 0L, 0L);
}

int SqlParserTokenManager::jjStartNfaWithStates_0(int pos, int kind, int state){
   jjmatchedKind = kind;
   jjmatchedPos = pos;
   if (input_stream->endOfInput()) { return pos + 1; }
   curChar = input_stream->readChar();
   return jjMoveNfa_0(state, pos + 1);
}

int SqlParserTokenManager::jjMoveNfa_0(int startState, int curPos){
   int startsAt = 0;
   jjnewStateCnt = 152;
   int i = 1;
   jjstateSet[0] = startState;
   int kind = 0x7fffffff;
   for (;;)
   {
      if (++jjround == 0x7fffffff)
         ReInitRounds();
      if (curChar < 64)
      {
         unsigned long long l = 1ULL << curChar;
         (void)l;
         do
         {
            switch(jjstateSet[--i])
            {
               case 8:
                  if ((0x3ff000000000000ULL & l) != 0L)
                  {
                     if (kind > 633)
                        kind = 633;
                     { jjCheckNAddStates(0, 8); }
                  }
                  else if ((0x100000200ULL & l) != 0L)
                  {
                     if (kind > 610)
                        kind = 610;
                     { jjCheckNAddTwoStates(146, 147); }
                  }
                  else if ((0x2400ULL & l) != 0L)
                  {
                     if (kind > 610)
                        kind = 610;
                     { jjCheckNAddStates(9, 11); }
                  }
                  else if (curChar == 46)
                     { jjCheckNAddTwoStates(150, 151); }
                  else if (curChar == 39)
                     { jjCheckNAddStates(12, 14); }
                  else if (curChar == 45)
                     jjstateSet[jjnewStateCnt++] = 15;
                  else if (curChar == 34)
                     { jjCheckNAddStates(15, 17); }
                  break;
               case 154:
                  if ((0x3ff000000000000ULL & l) != 0L)
                  {
                     if (kind > 639)
                        kind = 639;
                     { jjCheckNAdd(54); }
                  }
                  if ((0x3ff000000000000ULL & l) != 0L)
                  {
                     if (kind > 589)
                        kind = 589;
                     { jjCheckNAdd(53); }
                  }
                  break;
               case 25:
                  if ((0x3ff000000000000ULL & l) != 0L)
                  {
                     if (kind > 639)
                        kind = 639;
                     { jjCheckNAdd(54); }
                  }
                  else if (curChar == 39)
                     { jjCheckNAddStates(18, 20); }
                  if ((0x3ff000000000000ULL & l) != 0L)
                  {
                     if (kind > 589)
                        kind = 589;
                     { jjCheckNAdd(53); }
                  }
                  break;
               case 152:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAddTwoStates(151, 63); }
                  if ((0x3ff000000000000ULL & l) != 0L)
                  {
                     if (kind > 634)
                        kind = 634;
                     { jjCheckNAdd(150); }
                  }
                  break;
               case 7:
                  if ((0x3ff000000000000ULL & l) != 0L)
                  {
                     if (kind > 639)
                        kind = 639;
                     { jjCheckNAdd(54); }
                  }
                  else if (curChar == 38)
                     jjstateSet[jjnewStateCnt++] = 99;
                  if ((0x3ff000000000000ULL & l) != 0L)
                  {
                     if (kind > 589)
                        kind = 589;
                     { jjCheckNAdd(53); }
                  }
                  else if (curChar == 38)
                     jjstateSet[jjnewStateCnt++] = 71;
                  if (curChar == 38)
                     { jjCheckNAdd(3); }
                  break;
               case 153:
                  if ((0xfffffffbffffffffULL & l) != 0L)
                     { jjCheckNAddStates(15, 17); }
                  else if (curChar == 34)
                  {
                     if (kind > 596)
                        kind = 596;
                  }
                  if (curChar == 34)
                     jjstateSet[jjnewStateCnt++] = 12;
                  break;
               case 2:
                  if ((0x3ff000000000000ULL & l) == 0L)
                     break;
                  if (kind > 587)
                     kind = 587;
                  jjstateSet[jjnewStateCnt++] = 2;
                  break;
               case 3:
                  if (curChar == 39)
                     { jjCheckNAddStates(21, 23); }
                  break;
               case 4:
                  if ((0xffffff7fffffffffULL & l) != 0L)
                     { jjCheckNAddStates(21, 23); }
                  break;
               case 5:
                  if (curChar == 39)
                     { jjCheckNAdd(3); }
                  break;
               case 6:
                  if (curChar == 39 && kind > 588)
                     kind = 588;
                  break;
               case 10:
               case 12:
                  if (curChar == 34)
                     { jjCheckNAddStates(15, 17); }
                  break;
               case 11:
                  if ((0xfffffffbffffffffULL & l) != 0L)
                     { jjCheckNAddStates(15, 17); }
                  break;
               case 13:
                  if (curChar == 34)
                     jjstateSet[jjnewStateCnt++] = 12;
                  break;
               case 14:
                  if (curChar == 34 && kind > 596)
                     kind = 596;
                  break;
               case 15:
                  if (curChar != 45)
                     break;
                  if (kind > 612)
                     kind = 612;
                  { jjCheckNAddTwoStates(16, 17); }
                  break;
               case 16:
                  if ((0xffffffffffffdbffULL & l) == 0L)
                     break;
                  if (kind > 612)
                     kind = 612;
                  { jjCheckNAddTwoStates(16, 17); }
                  break;
               case 17:
                  if ((0x2400ULL & l) == 0L)
                     break;
                  if (kind > 612)
                     kind = 612;
                  { jjCheckNAdd(17); }
                  break;
               case 18:
                  if (curChar == 45)
                     jjstateSet[jjnewStateCnt++] = 15;
                  break;
               case 19:
               case 21:
                  if (curChar == 39)
                     { jjCheckNAddStates(12, 14); }
                  break;
               case 20:
                  if ((0xffffff7fffffffffULL & l) != 0L)
                     { jjCheckNAddStates(12, 14); }
                  break;
               case 22:
                  if (curChar == 39)
                     jjstateSet[jjnewStateCnt++] = 21;
                  break;
               case 23:
                  if (curChar == 39 && kind > 626)
                     kind = 626;
                  break;
               case 26:
                  if ((0xffffff7fffffffffULL & l) != 0L)
                     { jjCheckNAddStates(18, 20); }
                  break;
               case 27:
                  if (curChar == 39)
                     { jjCheckNAddStates(18, 20); }
                  break;
               case 28:
                  if (curChar == 39)
                     jjstateSet[jjnewStateCnt++] = 27;
                  break;
               case 29:
                  if (curChar != 39)
                     break;
                  if (kind > 627)
                     kind = 627;
                  { jjCheckNAddTwoStates(30, 31); }
                  break;
               case 30:
                  if ((0x2400ULL & l) != 0L)
                     { jjCheckNAddStates(24, 26); }
                  break;
               case 31:
                  if ((0x100000200ULL & l) != 0L)
                     { jjCheckNAddStates(24, 26); }
                  break;
               case 32:
               case 34:
                  if (curChar == 39)
                     { jjCheckNAddStates(27, 29); }
                  break;
               case 33:
                  if ((0xffffff7fffffffffULL & l) != 0L)
                     { jjCheckNAddStates(27, 29); }
                  break;
               case 35:
                  if (curChar == 39)
                     jjstateSet[jjnewStateCnt++] = 34;
                  break;
               case 37:
                  if (curChar == 39)
                     { jjCheckNAddStates(30, 32); }
                  break;
               case 38:
                  if (curChar == 32)
                     { jjCheckNAddStates(30, 32); }
                  break;
               case 39:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAddTwoStates(40, 41); }
                  break;
               case 40:
                  if (curChar == 32)
                     { jjCheckNAddTwoStates(40, 41); }
                  break;
               case 41:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAddStates(33, 35); }
                  break;
               case 42:
                  if (curChar == 32)
                     { jjCheckNAddStates(33, 35); }
                  break;
               case 43:
                  if (curChar != 39)
                     break;
                  if (kind > 631)
                     kind = 631;
                  { jjCheckNAddTwoStates(44, 45); }
                  break;
               case 44:
                  if ((0x2400ULL & l) != 0L)
                     { jjCheckNAddStates(36, 38); }
                  break;
               case 45:
                  if ((0x100000200ULL & l) != 0L)
                     { jjCheckNAddStates(36, 38); }
                  break;
               case 46:
                  if (curChar == 39)
                     { jjCheckNAddStates(39, 41); }
                  break;
               case 47:
                  if (curChar == 32)
                     { jjCheckNAddStates(39, 41); }
                  break;
               case 48:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAddTwoStates(49, 50); }
                  break;
               case 49:
                  if (curChar == 32)
                     { jjCheckNAddTwoStates(49, 50); }
                  break;
               case 50:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAddStates(42, 44); }
                  break;
               case 51:
                  if (curChar == 32)
                     { jjCheckNAddStates(42, 44); }
                  break;
               case 53:
                  if ((0x3ff000000000000ULL & l) == 0L)
                     break;
                  if (kind > 589)
                     kind = 589;
                  { jjCheckNAdd(53); }
                  break;
               case 54:
                  if ((0x3ff000000000000ULL & l) == 0L)
                     break;
                  if (kind > 639)
                     kind = 639;
                  { jjCheckNAdd(54); }
                  break;
               case 55:
                  if ((0x3ff000000000000ULL & l) == 0L)
                     break;
                  if (kind > 633)
                     kind = 633;
                  { jjCheckNAddStates(0, 8); }
                  break;
               case 56:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAddTwoStates(56, 57); }
                  break;
               case 58:
                  if ((0x3ff000000000000ULL & l) == 0L)
                     break;
                  if (kind > 633)
                     kind = 633;
                  { jjCheckNAdd(58); }
                  break;
               case 59:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAddTwoStates(59, 60); }
                  break;
               case 60:
                  if (curChar != 46)
                     break;
                  if (kind > 634)
                     kind = 634;
                  { jjCheckNAdd(61); }
                  break;
               case 61:
                  if ((0x3ff000000000000ULL & l) == 0L)
                     break;
                  if (kind > 634)
                     kind = 634;
                  { jjCheckNAdd(61); }
                  break;
               case 62:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAddTwoStates(62, 63); }
                  break;
               case 64:
                  if ((0x280000000000ULL & l) != 0L)
                     { jjCheckNAdd(65); }
                  break;
               case 65:
                  if ((0x3ff000000000000ULL & l) == 0L)
                     break;
                  if (kind > 635)
                     kind = 635;
                  { jjCheckNAdd(65); }
                  break;
               case 66:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAddTwoStates(66, 67); }
                  break;
               case 67:
                  if (curChar == 46)
                     { jjCheckNAddTwoStates(68, 63); }
                  break;
               case 68:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAddTwoStates(68, 63); }
                  break;
               case 70:
                  if (curChar == 38)
                     jjstateSet[jjnewStateCnt++] = 71;
                  break;
               case 71:
                  if (curChar == 34)
                     { jjCheckNAddStates(45, 47); }
                  break;
               case 72:
                  if (curChar == 34)
                     { jjCheckNAddStates(48, 51); }
                  break;
               case 73:
                  if (curChar == 34)
                     jjstateSet[jjnewStateCnt++] = 72;
                  break;
               case 74:
                  if ((0xfffffffbffffffffULL & l) != 0L)
                     { jjCheckNAddStates(48, 51); }
                  break;
               case 75:
                  if (curChar != 34)
                     break;
                  if (kind > 599)
                     kind = 599;
                  jjstateSet[jjnewStateCnt++] = 85;
                  break;
               case 77:
                  if (curChar == 39)
                     jjstateSet[jjnewStateCnt++] = 78;
                  break;
               case 78:
                  if ((0xfc00f77afffff9ffULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 79;
                  break;
               case 79:
                  if (curChar == 39 && kind > 599)
                     kind = 599;
                  break;
               case 86:
                  if ((0xfc00f77afffff9ffULL & l) != 0L)
                     { jjAddStates(52, 54); }
                  break;
               case 87:
                  if ((0xfc00f77afffff9ffULL & l) != 0L)
                     { jjCheckNAddStates(48, 51); }
                  break;
               case 88:
                  if (curChar == 43)
                     jjstateSet[jjnewStateCnt++] = 89;
                  break;
               case 89:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 90;
                  break;
               case 90:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 91;
                  break;
               case 91:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 92;
                  break;
               case 92:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 93;
                  break;
               case 93:
               case 97:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAdd(94); }
                  break;
               case 94:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAddStates(48, 51); }
                  break;
               case 95:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 96;
                  break;
               case 96:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 97;
                  break;
               case 98:
                  if (curChar == 38)
                     jjstateSet[jjnewStateCnt++] = 99;
                  break;
               case 99:
               case 100:
                  if (curChar == 39)
                     { jjCheckNAddStates(55, 58); }
                  break;
               case 101:
                  if (curChar == 39)
                     jjstateSet[jjnewStateCnt++] = 100;
                  break;
               case 102:
                  if ((0xffffff7fffffffffULL & l) != 0L)
                     { jjCheckNAddStates(55, 58); }
                  break;
               case 103:
                  if (curChar == 39)
                     { jjCheckNAddStates(59, 61); }
                  break;
               case 104:
                  if ((0x2400ULL & l) != 0L)
                     { jjCheckNAddStates(62, 64); }
                  break;
               case 105:
                  if ((0x100000200ULL & l) != 0L)
                     { jjCheckNAddStates(62, 64); }
                  break;
               case 106:
               case 107:
                  if (curChar == 39)
                     { jjCheckNAddStates(65, 68); }
                  break;
               case 108:
                  if (curChar == 39)
                     jjstateSet[jjnewStateCnt++] = 107;
                  break;
               case 109:
                  if ((0xffffff7fffffffffULL & l) != 0L)
                     { jjCheckNAddStates(65, 68); }
                  break;
               case 110:
                  if ((0xfc00f77afffff9ffULL & l) != 0L)
                     { jjAddStates(69, 71); }
                  break;
               case 111:
                  if ((0xfc00f77afffff9ffULL & l) != 0L)
                     { jjCheckNAddStates(65, 68); }
                  break;
               case 112:
                  if (curChar == 43)
                     jjstateSet[jjnewStateCnt++] = 113;
                  break;
               case 113:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 114;
                  break;
               case 114:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 115;
                  break;
               case 115:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 116;
                  break;
               case 116:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 117;
                  break;
               case 117:
               case 121:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAdd(118); }
                  break;
               case 118:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAddStates(65, 68); }
                  break;
               case 119:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 120;
                  break;
               case 120:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 121;
                  break;
               case 123:
                  if (curChar == 39)
                     jjstateSet[jjnewStateCnt++] = 124;
                  break;
               case 124:
                  if ((0xfc00f77afffff9ffULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 125;
                  break;
               case 125:
                  if (curChar == 39 && kind > 628)
                     kind = 628;
                  break;
               case 132:
                  if ((0xfc00f77afffff9ffULL & l) != 0L)
                     { jjAddStates(72, 74); }
                  break;
               case 133:
                  if ((0xfc00f77afffff9ffULL & l) != 0L)
                     { jjCheckNAddStates(55, 58); }
                  break;
               case 134:
                  if (curChar == 43)
                     jjstateSet[jjnewStateCnt++] = 135;
                  break;
               case 135:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 136;
                  break;
               case 136:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 137;
                  break;
               case 137:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 138;
                  break;
               case 138:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 139;
                  break;
               case 139:
               case 143:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAdd(140); }
                  break;
               case 140:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAddStates(55, 58); }
                  break;
               case 141:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 142;
                  break;
               case 142:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 143;
                  break;
               case 144:
                  if ((0x2400ULL & l) == 0L)
                     break;
                  if (kind > 610)
                     kind = 610;
                  { jjCheckNAddStates(9, 11); }
                  break;
               case 145:
                  if ((0x2400ULL & l) == 0L)
                     break;
                  if (kind > 610)
                     kind = 610;
                  { jjCheckNAdd(145); }
                  break;
               case 146:
                  if ((0x2400ULL & l) == 0L)
                     break;
                  if (kind > 616)
                     kind = 616;
                  { jjCheckNAddTwoStates(146, 147); }
                  break;
               case 147:
                  if ((0x100000200ULL & l) == 0L)
                     break;
                  if (kind > 616)
                     kind = 616;
                  { jjCheckNAddTwoStates(146, 147); }
                  break;
               case 148:
                  if ((0x100000200ULL & l) == 0L)
                     break;
                  if (kind > 610)
                     kind = 610;
                  { jjCheckNAddTwoStates(146, 147); }
                  break;
               case 149:
                  if (curChar == 46)
                     { jjCheckNAddTwoStates(150, 151); }
                  break;
               case 150:
                  if ((0x3ff000000000000ULL & l) == 0L)
                     break;
                  if (kind > 634)
                     kind = 634;
                  { jjCheckNAdd(150); }
                  break;
               case 151:
                  if ((0x3ff000000000000ULL & l) != 0L)
                     { jjCheckNAddTwoStates(151, 63); }
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else if (curChar < 128)
      {
         unsigned long long l = 1ULL << (curChar & 077);
         (void)l;
         do
         {
            switch(jjstateSet[--i])
            {
               case 8:
                  if ((0x7fffffe07fffffeULL & l) != 0L)
                  {
                     if (kind > 589)
                        kind = 589;
                     { jjCheckNAddTwoStates(53, 54); }
                  }
                  else if (curChar == 95)
                  {
                     if (kind > 587)
                        kind = 587;
                     { jjCheckNAddTwoStates(0, 1); }
                  }
                  if ((0x11288000112880ULL & l) != 0L)
                  {
                     if (kind > 595)
                        kind = 595;
                  }
                  else if ((0x20000000200000ULL & l) != 0L)
                     { jjAddStates(75, 76); }
                  else if ((0x100000001000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 37;
                  else if ((0x400000004000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 25;
                  if ((0x20000000200000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 7;
                  break;
               case 1:
                  if ((0x7fffffe07fffffeULL & l) != 0L)
                  {
                     if (kind > 587)
                        kind = 587;
                     { jjCheckNAdd(2); }
                  }
                  else if (curChar == 95)
                  {
                     if (kind > 587)
                        kind = 587;
                     { jjCheckNAddTwoStates(0, 1); }
                  }
                  break;
               case 154:
                  if ((0x7fffffe87fffffeULL & l) != 0L)
                  {
                     if (kind > 639)
                        kind = 639;
                     { jjCheckNAdd(54); }
                  }
                  if ((0x7fffffe87fffffeULL & l) != 0L)
                  {
                     if (kind > 589)
                        kind = 589;
                     { jjCheckNAdd(53); }
                  }
                  break;
               case 25:
                  if ((0x7fffffe87fffffeULL & l) != 0L)
                  {
                     if (kind > 639)
                        kind = 639;
                     { jjCheckNAdd(54); }
                  }
                  if ((0x7fffffe87fffffeULL & l) != 0L)
                  {
                     if (kind > 589)
                        kind = 589;
                     { jjCheckNAdd(53); }
                  }
                  break;
               case 7:
                  if ((0x7fffffe87fffffeULL & l) != 0L)
                  {
                     if (kind > 639)
                        kind = 639;
                     { jjCheckNAdd(54); }
                  }
                  if ((0x7fffffe87fffffeULL & l) != 0L)
                  {
                     if (kind > 589)
                        kind = 589;
                     { jjCheckNAdd(53); }
                  }
                  break;
               case 153:
               case 11:
                  { jjCheckNAddStates(15, 17); }
                  break;
               case 0:
                  if (curChar != 95)
                     break;
                  if (kind > 587)
                     kind = 587;
                  { jjCheckNAddTwoStates(0, 1); }
                  break;
               case 2:
                  if ((0x7fffffe87fffffeULL & l) == 0L)
                     break;
                  if (kind > 587)
                     kind = 587;
                  { jjCheckNAdd(2); }
                  break;
               case 4:
                  { jjAddStates(21, 23); }
                  break;
               case 9:
                  if ((0x11288000112880ULL & l) != 0L && kind > 595)
                     kind = 595;
                  break;
               case 16:
                  if (kind > 612)
                     kind = 612;
                  { jjAddStates(77, 78); }
                  break;
               case 20:
                  { jjAddStates(12, 14); }
                  break;
               case 24:
                  if ((0x400000004000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 25;
                  break;
               case 26:
                  { jjCheckNAddStates(18, 20); }
                  break;
               case 33:
                  { jjCheckNAddStates(27, 29); }
                  break;
               case 36:
                  if ((0x100000001000000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 37;
                  break;
               case 39:
                  if ((0x7e0000007eULL & l) != 0L)
                     { jjAddStates(79, 80); }
                  break;
               case 41:
                  if ((0x7e0000007eULL & l) != 0L)
                     { jjCheckNAddStates(33, 35); }
                  break;
               case 48:
                  if ((0x7e0000007eULL & l) != 0L)
                     { jjAddStates(81, 82); }
                  break;
               case 50:
                  if ((0x7e0000007eULL & l) != 0L)
                     { jjCheckNAddStates(42, 44); }
                  break;
               case 52:
                  if ((0x7fffffe07fffffeULL & l) == 0L)
                     break;
                  if (kind > 589)
                     kind = 589;
                  { jjCheckNAddTwoStates(53, 54); }
                  break;
               case 53:
                  if ((0x7fffffe87fffffeULL & l) == 0L)
                     break;
                  if (kind > 589)
                     kind = 589;
                  { jjCheckNAdd(53); }
                  break;
               case 54:
                  if ((0x7fffffe87fffffeULL & l) == 0L)
                     break;
                  if (kind > 639)
                     kind = 639;
                  { jjCheckNAdd(54); }
                  break;
               case 57:
                  if ((0x11288000112880ULL & l) != 0L && kind > 594)
                     kind = 594;
                  break;
               case 63:
                  if ((0x2000000020ULL & l) != 0L)
                     { jjAddStates(83, 84); }
                  break;
               case 69:
                  if ((0x20000000200000ULL & l) != 0L)
                     { jjAddStates(75, 76); }
                  break;
               case 74:
                  { jjCheckNAddStates(48, 51); }
                  break;
               case 76:
                  if ((0x2000000020ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 77;
                  break;
               case 78:
                  if ((0xf8000001f8000001ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 79;
                  break;
               case 80:
                  if ((0x1000000010000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 76;
                  break;
               case 81:
                  if ((0x200000002ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 80;
                  break;
               case 82:
                  if ((0x800000008ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 81;
                  break;
               case 83:
                  if ((0x8000000080000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 82;
                  break;
               case 84:
                  if ((0x2000000020ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 83;
                  break;
               case 85:
                  if ((0x20000000200000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 84;
                  break;
               case 86:
                  if ((0xf8000001f8000001ULL & l) != 0L)
                     { jjAddStates(52, 54); }
                  break;
               case 87:
                  if ((0xf8000001f8000001ULL & l) != 0L)
                     { jjCheckNAddStates(48, 51); }
                  break;
               case 89:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 90;
                  break;
               case 90:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 91;
                  break;
               case 91:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 92;
                  break;
               case 92:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 93;
                  break;
               case 93:
               case 97:
                  if ((0x7e0000007eULL & l) != 0L)
                     { jjCheckNAdd(94); }
                  break;
               case 94:
                  if ((0x7e0000007eULL & l) != 0L)
                     { jjCheckNAddStates(48, 51); }
                  break;
               case 95:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 96;
                  break;
               case 96:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 97;
                  break;
               case 102:
                  { jjCheckNAddStates(55, 58); }
                  break;
               case 109:
                  { jjCheckNAddStates(65, 68); }
                  break;
               case 110:
                  if ((0xf8000001f8000001ULL & l) != 0L)
                     { jjAddStates(69, 71); }
                  break;
               case 111:
                  if ((0xf8000001f8000001ULL & l) != 0L)
                     { jjCheckNAddStates(65, 68); }
                  break;
               case 113:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 114;
                  break;
               case 114:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 115;
                  break;
               case 115:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 116;
                  break;
               case 116:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 117;
                  break;
               case 117:
               case 121:
                  if ((0x7e0000007eULL & l) != 0L)
                     { jjCheckNAdd(118); }
                  break;
               case 118:
                  if ((0x7e0000007eULL & l) != 0L)
                     { jjCheckNAddStates(65, 68); }
                  break;
               case 119:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 120;
                  break;
               case 120:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 121;
                  break;
               case 122:
                  if ((0x2000000020ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 123;
                  break;
               case 124:
                  if ((0xf8000001f8000001ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 125;
                  break;
               case 126:
                  if ((0x1000000010000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 122;
                  break;
               case 127:
                  if ((0x200000002ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 126;
                  break;
               case 128:
                  if ((0x800000008ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 127;
                  break;
               case 129:
                  if ((0x8000000080000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 128;
                  break;
               case 130:
                  if ((0x2000000020ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 129;
                  break;
               case 131:
                  if ((0x20000000200000ULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 130;
                  break;
               case 132:
                  if ((0xf8000001f8000001ULL & l) != 0L)
                     { jjAddStates(72, 74); }
                  break;
               case 133:
                  if ((0xf8000001f8000001ULL & l) != 0L)
                     { jjCheckNAddStates(55, 58); }
                  break;
               case 135:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 136;
                  break;
               case 136:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 137;
                  break;
               case 137:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 138;
                  break;
               case 138:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 139;
                  break;
               case 139:
               case 143:
                  if ((0x7e0000007eULL & l) != 0L)
                     { jjCheckNAdd(140); }
                  break;
               case 140:
                  if ((0x7e0000007eULL & l) != 0L)
                     { jjCheckNAddStates(55, 58); }
                  break;
               case 141:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 142;
                  break;
               case 142:
                  if ((0x7e0000007eULL & l) != 0L)
                     jjstateSet[jjnewStateCnt++] = 143;
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else
      {
         int hiByte = (curChar >> 8);
         int i1 = hiByte >> 6;
         unsigned long long l1 = 1ULL << (hiByte & 077);
         int i2 = (curChar & 0xff) >> 6;
         unsigned long long l2 = 1ULL << (curChar & 077);
         do
         {
            switch(jjstateSet[--i])
            {
               case 154:
               case 53:
                  if (!jjCanMove_0(hiByte, i1, i2, l1, l2))
                     break;
                  if (kind > 589)
                     kind = 589;
                  { jjCheckNAdd(53); }
                  break;
               case 25:
                  if (!jjCanMove_0(hiByte, i1, i2, l1, l2))
                     break;
                  if (kind > 589)
                     kind = 589;
                  { jjCheckNAdd(53); }
                  break;
               case 7:
                  if (!jjCanMove_0(hiByte, i1, i2, l1, l2))
                     break;
                  if (kind > 589)
                     kind = 589;
                  { jjCheckNAdd(53); }
                  break;
               case 153:
               case 11:
                  if (jjCanMove_1(hiByte, i1, i2, l1, l2))
                     { jjCheckNAddStates(15, 17); }
                  break;
               case 2:
                  if (!jjCanMove_0(hiByte, i1, i2, l1, l2))
                     break;
                  if (kind > 587)
                     kind = 587;
                  jjstateSet[jjnewStateCnt++] = 2;
                  break;
               case 4:
                  if (jjCanMove_1(hiByte, i1, i2, l1, l2))
                     { jjAddStates(21, 23); }
                  break;
               case 16:
                  if (!jjCanMove_1(hiByte, i1, i2, l1, l2))
                     break;
                  if (kind > 612)
                     kind = 612;
                  { jjAddStates(77, 78); }
                  break;
               case 20:
                  if (jjCanMove_1(hiByte, i1, i2, l1, l2))
                     { jjAddStates(12, 14); }
                  break;
               case 26:
                  if (jjCanMove_1(hiByte, i1, i2, l1, l2))
                     { jjCheckNAddStates(18, 20); }
                  break;
               case 33:
                  if (jjCanMove_1(hiByte, i1, i2, l1, l2))
                     { jjCheckNAddStates(27, 29); }
                  break;
               case 74:
               case 87:
                  if (jjCanMove_1(hiByte, i1, i2, l1, l2))
                     { jjCheckNAddStates(48, 51); }
                  break;
               case 78:
                  if (jjCanMove_1(hiByte, i1, i2, l1, l2))
                     jjstateSet[jjnewStateCnt++] = 79;
                  break;
               case 86:
                  if (jjCanMove_1(hiByte, i1, i2, l1, l2))
                     { jjAddStates(52, 54); }
                  break;
               case 102:
               case 133:
                  if (jjCanMove_1(hiByte, i1, i2, l1, l2))
                     { jjCheckNAddStates(55, 58); }
                  break;
               case 109:
               case 111:
                  if (jjCanMove_1(hiByte, i1, i2, l1, l2))
                     { jjCheckNAddStates(65, 68); }
                  break;
               case 110:
                  if (jjCanMove_1(hiByte, i1, i2, l1, l2))
                     { jjAddStates(69, 71); }
                  break;
               case 124:
                  if (jjCanMove_1(hiByte, i1, i2, l1, l2))
                     jjstateSet[jjnewStateCnt++] = 125;
                  break;
               case 132:
                  if (jjCanMove_1(hiByte, i1, i2, l1, l2))
                     { jjAddStates(72, 74); }
                  break;
               default : if (i1 == 0 || l1 == 0 || i2 == 0 ||  l2 == 0) break; else break;
            }
         } while(i != startsAt);
      }
      if (kind != 0x7fffffff)
      {
         jjmatchedKind = kind;
         jjmatchedPos = curPos;
         kind = 0x7fffffff;
      }
      ++curPos;
      if ((i = jjnewStateCnt), (jjnewStateCnt = startsAt), (i == (startsAt = 152 - startsAt)))
         return curPos;
      if (input_stream->endOfInput()) { return curPos; }
      curChar = input_stream->readChar();
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa0_3(){
   return 1;
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa0_1(){
   switch(curChar)
   {
      case 77:
      case 109:
         return jjMoveStringLiteralDfa1_1(0x2ULL, 0xc000000ULL, 0x200000000000ULL);
      default :
         return 1;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa1_1(unsigned long long active0, unsigned long long active5, unsigned long long active8){
   if (input_stream->endOfInput()) {
      return 1;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 65:
      case 97:
         return jjMoveStringLiteralDfa2_1(active0, 0L, active5, 0x4000000ULL, active8, 0x200000000000ULL);
      case 73:
      case 105:
         return jjMoveStringLiteralDfa2_1(active0, 0x2ULL, active5, 0x8000000ULL, active8, 0L);
      default :
         return 2;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa2_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 2;
   if (input_stream->endOfInput()) {
      return 2;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 78:
      case 110:
         return jjMoveStringLiteralDfa3_1(active0, 0x2ULL, active5, 0x8000000ULL, active8, 0L);
      case 88:
      case 120:
         return jjMoveStringLiteralDfa3_1(active0, 0L, active5, 0x4000000ULL, active8, 0x200000000000ULL);
      default :
         return 3;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa3_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 3;
   if (input_stream->endOfInput()) {
      return 3;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 32:
         return jjMoveStringLiteralDfa4_1(active0, 0x2ULL, active5, 0xc000000ULL, active8, 0x200000000000ULL);
      default :
         return 4;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa4_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 4;
   if (input_stream->endOfInput()) {
      return 4;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 78:
      case 110:
         return jjMoveStringLiteralDfa5_1(active0, 0x2ULL, active5, 0x4000000ULL, active8, 0L);
      case 82:
      case 114:
         return jjMoveStringLiteralDfa5_1(active0, 0L, active5, 0x8000000ULL, active8, 0x200000000000ULL);
      default :
         return 5;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa5_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 5;
   if (input_stream->endOfInput()) {
      return 5;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 69:
      case 101:
         return jjMoveStringLiteralDfa6_1(active0, 0L, active5, 0x8000000ULL, active8, 0x200000000000ULL);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa6_1(active0, 0x2ULL, active5, 0x4000000ULL, active8, 0L);
      default :
         return 6;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa6_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 6;
   if (input_stream->endOfInput()) {
      return 6;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 78:
      case 110:
         return jjMoveStringLiteralDfa7_1(active0, 0x2ULL, active5, 0x4000000ULL, active8, 0L);
      case 83:
      case 115:
         return jjMoveStringLiteralDfa7_1(active0, 0L, active5, 0x8000000ULL, active8, 0x200000000000ULL);
      default :
         return 7;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa7_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 7;
   if (input_stream->endOfInput()) {
      return 7;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 32:
         return jjMoveStringLiteralDfa8_1(active0, 0x2ULL, active5, 0x4000000ULL, active8, 0L);
      case 69:
      case 101:
         return jjMoveStringLiteralDfa8_1(active0, 0L, active5, 0x8000000ULL, active8, 0x200000000000ULL);
      default :
         return 8;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa8_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 8;
   if (input_stream->endOfInput()) {
      return 8;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 82:
      case 114:
         return jjMoveStringLiteralDfa9_1(active0, 0x2ULL, active5, 0xc000000ULL, active8, 0x200000000000ULL);
      default :
         return 9;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa9_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 9;
   if (input_stream->endOfInput()) {
      return 9;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 69:
      case 101:
         return jjMoveStringLiteralDfa10_1(active0, 0x2ULL, active5, 0x4000000ULL, active8, 0L);
      case 86:
      case 118:
         return jjMoveStringLiteralDfa10_1(active0, 0L, active5, 0x8000000ULL, active8, 0x200000000000ULL);
      default :
         return 10;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa10_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 10;
   if (input_stream->endOfInput()) {
      return 10;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 69:
      case 101:
         return jjMoveStringLiteralDfa11_1(active0, 0L, active5, 0x8000000ULL, active8, 0x200000000000ULL);
      case 83:
      case 115:
         return jjMoveStringLiteralDfa11_1(active0, 0x2ULL, active5, 0x4000000ULL, active8, 0L);
      default :
         return 11;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa11_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 11;
   if (input_stream->endOfInput()) {
      return 11;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 68:
      case 100:
         return jjMoveStringLiteralDfa12_1(active0, 0L, active5, 0x8000000ULL, active8, 0x200000000000ULL);
      case 69:
      case 101:
         return jjMoveStringLiteralDfa12_1(active0, 0x2ULL, active5, 0x4000000ULL, active8, 0L);
      default :
         return 12;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa12_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 12;
   if (input_stream->endOfInput()) {
      return 12;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 32:
         return jjMoveStringLiteralDfa13_1(active0, 0L, active5, 0x8000000ULL, active8, 0x200000000000ULL);
      case 82:
      case 114:
         return jjMoveStringLiteralDfa13_1(active0, 0x2ULL, active5, 0x4000000ULL, active8, 0L);
      default :
         return 13;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa13_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 13;
   if (input_stream->endOfInput()) {
      return 13;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 86:
      case 118:
         return jjMoveStringLiteralDfa14_1(active0, 0x2ULL, active5, 0x4000000ULL, active8, 0L);
      case 87:
      case 119:
         return jjMoveStringLiteralDfa14_1(active0, 0L, active5, 0x8000000ULL, active8, 0x200000000000ULL);
      default :
         return 14;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa14_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 14;
   if (input_stream->endOfInput()) {
      return 14;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 69:
      case 101:
         return jjMoveStringLiteralDfa15_1(active0, 0x2ULL, active5, 0x4000000ULL, active8, 0L);
      case 79:
      case 111:
         return jjMoveStringLiteralDfa15_1(active0, 0L, active5, 0x8000000ULL, active8, 0x200000000000ULL);
      default :
         return 15;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa15_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 15;
   if (input_stream->endOfInput()) {
      return 15;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 68:
      case 100:
         return jjMoveStringLiteralDfa16_1(active0, 0x2ULL, active5, 0x4000000ULL, active8, 0L);
      case 82:
      case 114:
         return jjMoveStringLiteralDfa16_1(active0, 0L, active5, 0x8000000ULL, active8, 0x200000000000ULL);
      default :
         return 16;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa16_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 16;
   if (input_stream->endOfInput()) {
      return 16;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 32:
         return jjMoveStringLiteralDfa17_1(active0, 0x2ULL, active5, 0x4000000ULL, active8, 0L);
      case 68:
      case 100:
         if ((active5 & 0x8000000ULL) != 0L)
            return jjStopAtPos(16, 347);
         else if ((active8 & 0x200000000000ULL) != 0L)
            return jjStopAtPos(16, 557);
         break;
      default :
         return 17;
   }
   return 17;
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa17_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5, unsigned long long old8, unsigned long long active8){
   if (((active0 &= old0) | (active5 &= old5) | (active8 &= old8)) == 0L)
      return 17;
   if (input_stream->endOfInput()) {
      return 17;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 87:
      case 119:
         return jjMoveStringLiteralDfa18_1(active0, 0x2ULL, active5, 0x4000000ULL);
      default :
         return 18;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa18_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5){
   if (((active0 &= old0) | (active5 &= old5)) == 0L)
      return 18;
   if (input_stream->endOfInput()) {
      return 18;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 79:
      case 111:
         return jjMoveStringLiteralDfa19_1(active0, 0x2ULL, active5, 0x4000000ULL);
      default :
         return 19;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa19_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5){
   if (((active0 &= old0) | (active5 &= old5)) == 0L)
      return 19;
   if (input_stream->endOfInput()) {
      return 19;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 82:
      case 114:
         return jjMoveStringLiteralDfa20_1(active0, 0x2ULL, active5, 0x4000000ULL);
      default :
         return 20;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa20_1(unsigned long long old0, unsigned long long active0, unsigned long long old5, unsigned long long active5){
   if (((active0 &= old0) | (active5 &= old5)) == 0L)
      return 20;
   if (input_stream->endOfInput()) {
      return 20;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 68:
      case 100:
         if ((active0 & 0x2ULL) != 0L)
            return jjStopAtPos(20, 1);
         else if ((active5 & 0x4000000ULL) != 0L)
            return jjStopAtPos(20, 346);
         break;
      default :
         return 21;
   }
   return 21;
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa0_2(){
   switch(curChar)
   {
      case 42:
         return jjMoveStringLiteralDfa1_2(0x40000000000ULL);
      default :
         return 1;
   }
}

 int  SqlParserTokenManager::jjMoveStringLiteralDfa1_2(unsigned long long active9){
   if (input_stream->endOfInput()) {
      return 1;
   }
   curChar = input_stream->readChar();
   switch(curChar)
   {
      case 47:
         if ((active9 & 0x40000000000ULL) != 0L)
            return jjStopAtPos(1, 618);
         break;
      default :
         return 2;
   }
   return 2;
}

bool SqlParserTokenManager::jjCanMove_0(int hiByte, int i1, int i2, unsigned long long l1, unsigned long long l2){
   switch(hiByte)
   {
      case 0:
         return ((jjbitVec0[i2] & l2) != 0L);
      default :
         return false;
   }
}

bool SqlParserTokenManager::jjCanMove_1(int hiByte, int i1, int i2, unsigned long long l1, unsigned long long l2){
   switch(hiByte)
   {
      case 0:
         return ((jjbitVec3[i2] & l2) != 0L);
      default :
         if ((jjbitVec1[i1] & l1) != 0L)
            return true;
         return false;
   }
}

/** Token literal values. */

Token * SqlParserTokenManager::jjFillToken(){
   Token *t;
   JJString curTokenImage;
   int beginLine   = -1;
   int endLine     = -1;
   int beginColumn = -1;
   int endColumn   = -1;
   if (jjmatchedPos < 0)
   {
       curTokenImage = image.c_str();
   if (input_stream->getTrackLineColumn()) {
      beginLine = endLine = input_stream->getEndLine();
      beginColumn = endColumn = input_stream->getEndColumn();
   }
   }
   else
   {
      JJString im = jjstrLiteralImages[jjmatchedKind];
      curTokenImage = (im.length() == 0) ? input_stream->GetImage() : im;
   if (input_stream->getTrackLineColumn()) {
      beginLine = input_stream->getBeginLine();
      beginColumn = input_stream->getBeginColumn();
      endLine = input_stream->getEndLine();
      endColumn = input_stream->getEndColumn();
   }
   }
   t = Token::newToken(jjmatchedKind, curTokenImage);
   t->specialToken = nullptr;
   t->next = nullptr;

   if (input_stream->getTrackLineColumn()) {
   t->beginLine = beginLine;
   t->endLine = endLine;
   t->beginColumn = beginColumn;
   t->endColumn = endColumn;
   }

   return t;
}
const int defaultLexState = 0;
/** Get the next Token. */

Token * SqlParserTokenManager::getNextToken(){
  Token *specialToken = nullptr;
  Token *matchedToken = nullptr;
  int curPos = 0;

  for (;;)
  {
   EOFLoop: 
   if (input_stream->endOfInput())
   {
      jjmatchedKind = 0;
      jjmatchedPos = -1;
      matchedToken = jjFillToken();
      matchedToken->specialToken = specialToken;
      return matchedToken;
   }
   curChar = input_stream->BeginToken();
   image = jjimage;
   image.clear();
   jjimageLen = 0;

   for (;;)
   {
     switch(curLexState)
     {
       case 0:
         jjmatchedKind = 0x7fffffff;
         jjmatchedPos = 0;
         curPos = jjMoveStringLiteralDfa0_0();
         if (jjmatchedPos == 0 && jjmatchedKind > 643)
         {
            jjmatchedKind = 643;
         }
         break;
       case 1:
         jjmatchedKind = 0x7fffffff;
         jjmatchedPos = 0;
         curPos = jjMoveStringLiteralDfa0_1();
         break;
       case 2:
         jjmatchedKind = 0x7fffffff;
         jjmatchedPos = 0;
         curPos = jjMoveStringLiteralDfa0_2();
         if (jjmatchedPos == 0 && jjmatchedKind > 619)
         {
            jjmatchedKind = 619;
         }
         break;
       case 3:
         jjmatchedKind = 620;
         jjmatchedPos = -1;
         curPos = 0;
         curPos = jjMoveStringLiteralDfa0_3();
         break;
     }
     if (jjmatchedKind != 0x7fffffff)
     {
        if (jjmatchedPos + 1 < curPos)
           input_stream->backup(curPos - jjmatchedPos - 1);
        if ((jjtoToken[jjmatchedKind >> 6] & (1ULL << (jjmatchedKind & 077))) != 0L)
        {
           matchedToken = jjFillToken();
           matchedToken->specialToken = specialToken;
           TokenLexicalActions(matchedToken);
       if (jjnewLexState[jjmatchedKind] != -1)
         curLexState = jjnewLexState[jjmatchedKind];
           return matchedToken;
        }
        else if ((jjtoSkip[jjmatchedKind >> 6] & (1ULL << (jjmatchedKind & 077))) != 0L)
        {
           if ((jjtoSpecial[jjmatchedKind >> 6] & (1ULL << (jjmatchedKind & 077))) != 0L)
           {
              matchedToken = jjFillToken();
              if (specialToken == nullptr)
                 specialToken = matchedToken;
              else
              {
                 matchedToken->specialToken = specialToken;
                 specialToken = (specialToken->next = matchedToken);
              }
              SkipLexicalActions(matchedToken);
           }
           else
              SkipLexicalActions(nullptr);
         if (jjnewLexState[jjmatchedKind] != -1)
           curLexState = jjnewLexState[jjmatchedKind];
           goto EOFLoop;
        }
        jjimageLen += jjmatchedPos + 1;
      if (jjnewLexState[jjmatchedKind] != -1)
        curLexState = jjnewLexState[jjmatchedKind];
        curPos = 0;
        jjmatchedKind = 0x7fffffff;
     if (!input_stream->endOfInput()) {
           curChar = input_stream->readChar();
     continue;
   }
     }
     int error_line = input_stream->getEndLine();
     int error_column = input_stream->getEndColumn();
     JJString error_after;
     bool EOFSeen = false;
     if (input_stream->endOfInput()) {
        EOFSeen = true;
        error_after = curPos <= 1 ? EMPTY : input_stream->GetImage();
        if (curChar == '\n' || curChar == '\r') {
           error_line++;
           error_column = 0;
        }
        else
           error_column++;
     }
     if (!EOFSeen) {
        error_after = curPos <= 1 ? EMPTY : input_stream->GetImage();
     }
     errorHandler->lexicalError(EOFSeen, curLexState, error_line, error_column, error_after, curChar, this);
   }
  }
}


void  SqlParserTokenManager::SkipLexicalActions(Token *matchedToken){
   switch(jjmatchedKind)
   {
      case 620 : {
         image.append(input_stream->GetSuffix(jjimageLen + (lengthOfMatch = jjmatchedPos + 1)));
                     StoreImage(matchedToken);
         break;
       }
      default :
         break;
   }
}

void  SqlParserTokenManager::TokenLexicalActions(Token *matchedToken){
   switch(jjmatchedKind)
   {
      case 587 : {
        image.append(input_stream->GetSuffix(jjimageLen + (lengthOfMatch = jjmatchedPos + 1)));
                                                                             setKindToIdentifier(matchedToken);
         break;
       }
      case 588 : {
        image.append(input_stream->GetSuffix(jjimageLen + (lengthOfMatch = jjmatchedPos + 1)));
                                                        setUnicodeLiteralType(matchedToken);
         break;
       }
      default :
         break;
   }
}
  /** Reinitialise parser. */
  void SqlParserTokenManager::ReInit(JAVACC_CHARSTREAM *stream, int lexState) {
    clear();
    jjmatchedPos = jjnewStateCnt = 0;
    curLexState = lexState;
    input_stream = stream;
    ReInitRounds();
    debugStream = stdout; // init
    SwitchTo(lexState);
    errorHandler = new TokenManagerErrorHandler();
  }

  void SqlParserTokenManager::ReInitRounds() {
    int i;
    jjround = 0x80000001;
    for (i = 152; i-- > 0;)
      jjrounds[i] = 0x80000000;
  }

  /** Switch to specified lex state. */
  void SqlParserTokenManager::SwitchTo(int lexState) {
    if (lexState >= 4 || lexState < 0) {
      JJString message;
#ifdef WIDE_CHAR
      message += L"Error: Ignoring invalid lexical state : ";
      message += lexState; message += L". State unchanged.";
#else
      message += "Error: Ignoring invalid lexical state : ";
      message += lexState; message += ". State unchanged.";
#endif
      throw new TokenMgrError(message, INVALID_LEXICAL_STATE);
    } else
      curLexState = lexState;
  }

  /** Constructor. */
  SqlParserTokenManager::SqlParserTokenManager (JAVACC_CHARSTREAM *stream, int lexState)
  {
    input_stream = nullptr;
    ReInit(stream, lexState);
  }

  // Destructor
  SqlParserTokenManager::~SqlParserTokenManager () {
    clear();
  }

  // clear
  void SqlParserTokenManager::clear() {
    //Since input_stream was generated outside of TokenManager
    //TokenManager should not take care of deleting it
    //if (input_stream) delete input_stream;
    if (errorHandler) delete errorHandler, errorHandler = nullptr;    
  }


}
}
