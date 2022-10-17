import _extends from "@babel/runtime/helpers/extends";

/**
 * THIS FILE IS AUTO-GENERATED
 * DON'T MAKE CHANGES HERE
 */
import { config } from './configReadonly.js';
import { createNode, createObjectNode, createOperatorNode, createParenthesisNode, createRelationalNode, createArrayNode, createBlockNode, createConditionalNode, createConstantNode, createRangeNode, createReviver, createChainClass, createFunctionAssignmentNode, createChain, createAccessorNode, createAssignmentNode, createIndexNode, createSymbolNode, createFunctionNode, createParse, createResolve, createSimplifyConstant, createCompile, createHelpClass, createLeafCount, createSimplifyCore, createEvaluate, createHelp, createParserClass, createParser, createSimplify, createSymbolicEqual, createDerivative, createRationalize, createFilterTransform, createForEachTransform, createMapTransform, createApplyTransform, createDiffTransform, createSubsetTransform, createConcatTransform, createMaxTransform, createMinTransform, createRangeTransform, createSumTransform, createCumSumTransform, createRowTransform, createColumnTransform, createIndexTransform, createMeanTransform, createVarianceTransform, createStdTransform } from '../factoriesAny.js';
import { BigNumber, Complex, e, _false, fineStructure, Fraction, i, _Infinity, LN10, LOG10E, Matrix, _NaN, _null, phi, Range, ResultSet, SQRT1_2, // eslint-disable-line camelcase
sackurTetrode, tau, _true, version, DenseMatrix, efimovFactor, LN2, pi, replacer, SQRT2, typed, unaryPlus, weakMixingAngle, abs, acos, acot, acsc, addScalar, arg, asech, asinh, atan, atanh, bignumber, bitNot, boolean, clone, combinations, complex, conj, cosh, coth, csc, cube, equalScalar, erf, exp, expm1, filter, forEach, format, getMatrixDataType, hex, im, isInteger, isNegative, isPositive, isZero, LOG2E, lgamma, log10, log2, map, multiplyScalar, not, number, oct, pickRandom, print, random, re, sec, sign, sin, SparseMatrix, splitUnit, square, string, tan, typeOf, acosh, acsch, apply, asec, bin, combinationsWithRep, cos, csch, isNaN, isPrime, randomInt, sech, sinh, sparse, sqrt, tanh, unaryMinus, acoth, cot, fraction, isNumeric, matrix, matrixFromFunction, mod, nthRoot, numeric, or, prod, reshape, size, smaller, squeeze, subset, subtract, to, transpose, xgcd, zeros, and, bitAnd, bitXor, cbrt, compare, compareText, concat, count, ctranspose, diag, divideScalar, dotDivide, equal, fft, flatten, gcd, hasNumericValue, hypot, ifft, kron, largerEq, leftShift, lsolve, matrixFromColumns, min, mode, nthRoots, ones, partitionSelect, resize, rightArithShift, round, smallerEq, unequal, usolve, xor, add, atan2, bitOr, catalan, compareNatural, cumsum, deepEqual, diff, dot, equalText, floor, identity, invmod, larger, log, lsolveAll, matrixFromRows, multiply, qr, range, rightLogShift, setSize, slu, sum, trace, usolveAll, asin, ceil, composition, cross, det, distance, dotMultiply, FibonacciHeap, fix, ImmutableDenseMatrix, Index, intersect, lcm, log1p, max, quantileSeq, row, setCartesian, setDistinct, setIsSubset, setPowerset, sort, column, index, inv, pinv, pow, setDifference, setMultiplicity, Spa, sqrtm, Unit, vacuumImpedance, wienDisplacement, atomicMass, bohrMagneton, boltzmann, conductanceQuantum, createUnit, deuteronMass, dotPow, electricConstant, elementaryCharge, expm, faraday, firstRadiation, gamma, gravitationConstant, hartreeEnergy, klitzing, loschmidt, magneticConstant, molarMass, molarPlanckConstant, neutronMass, nuclearMagneton, planckCharge, planckLength, planckTemperature, protonMass, reducedPlanckConstant, rydberg, setIntersect, speedOfLight, stefanBoltzmann, thomsonCrossSection, avogadro, bohrRadius, coulomb, divide, electronMass, factorial, gravity, inverseConductanceQuantum, lup, magneticFluxQuantum, molarMassC12, multinomial, permutations, planckMass, quantumOfCirculation, secondRadiation, stirlingS2, unit, bellNumbers, eigs, fermiCoupling, mean, molarVolume, planckConstant, setSymDifference, variance, classicalElectronRadius, lusolve, median, setUnion, std, gasConstant, mad, norm, rotationMatrix, kldivergence, rotate, planckTime } from './pureFunctionsAny.generated.js';
var math = {}; // NOT pure!

var mathWithTransform = {}; // NOT pure!

var classes = {}; // NOT pure!

export var Node = createNode({
  mathWithTransform
});
export var ObjectNode = createObjectNode({
  Node
});
export var OperatorNode = createOperatorNode({
  Node
});
export var ParenthesisNode = createParenthesisNode({
  Node
});
export var RelationalNode = createRelationalNode({
  Node
});
export var ArrayNode = createArrayNode({
  Node
});
export var BlockNode = createBlockNode({
  Node,
  ResultSet
});
export var ConditionalNode = createConditionalNode({
  Node
});
export var ConstantNode = createConstantNode({
  Node
});
export var RangeNode = createRangeNode({
  Node
});
export var reviver = createReviver({
  classes
});
export var Chain = createChainClass({
  math,
  typed
});
export var FunctionAssignmentNode = createFunctionAssignmentNode({
  Node,
  typed
});
export var chain = createChain({
  Chain,
  typed
});
export var AccessorNode = createAccessorNode({
  Node,
  subset
});
export var AssignmentNode = createAssignmentNode({
  matrix,
  Node,
  subset
});
export var IndexNode = createIndexNode({
  Node,
  size
});
export var SymbolNode = createSymbolNode({
  Unit,
  Node,
  math
});
export var FunctionNode = createFunctionNode({
  Node,
  SymbolNode,
  math
});
export var parse = createParse({
  AccessorNode,
  ArrayNode,
  AssignmentNode,
  BlockNode,
  ConditionalNode,
  ConstantNode,
  FunctionAssignmentNode,
  FunctionNode,
  IndexNode,
  ObjectNode,
  OperatorNode,
  ParenthesisNode,
  RangeNode,
  RelationalNode,
  SymbolNode,
  config,
  numeric,
  typed
});
export var resolve = createResolve({
  ConstantNode,
  FunctionNode,
  OperatorNode,
  ParenthesisNode,
  parse,
  typed
});
export var simplifyConstant = createSimplifyConstant({
  bignumber,
  fraction,
  AccessorNode,
  ArrayNode,
  ConstantNode,
  FunctionNode,
  IndexNode,
  ObjectNode,
  OperatorNode,
  SymbolNode,
  config,
  mathWithTransform,
  matrix,
  parse,
  typed
});
export var compile = createCompile({
  parse,
  typed
});
export var Help = createHelpClass({
  parse
});
export var leafCount = createLeafCount({
  parse,
  typed
});
export var simplifyCore = createSimplifyCore({
  AccessorNode,
  ArrayNode,
  ConstantNode,
  FunctionNode,
  IndexNode,
  ObjectNode,
  OperatorNode,
  ParenthesisNode,
  SymbolNode,
  add,
  divide,
  equal,
  isZero,
  multiply,
  parse,
  pow,
  subtract,
  typed
});
export var evaluate = createEvaluate({
  parse,
  typed
});
export var help = createHelp({
  Help,
  mathWithTransform,
  typed
});
export var Parser = createParserClass({
  evaluate
});
export var parser = createParser({
  Parser,
  typed
});
export var simplify = createSimplify({
  bignumber,
  fraction,
  AccessorNode,
  ArrayNode,
  ConstantNode,
  FunctionNode,
  IndexNode,
  ObjectNode,
  OperatorNode,
  ParenthesisNode,
  SymbolNode,
  add,
  config,
  divide,
  equal,
  isZero,
  mathWithTransform,
  matrix,
  multiply,
  parse,
  pow,
  resolve,
  simplifyConstant,
  simplifyCore,
  subtract,
  typed
});
export var symbolicEqual = createSymbolicEqual({
  OperatorNode,
  parse,
  simplify,
  typed
});
export var derivative = createDerivative({
  ConstantNode,
  FunctionNode,
  OperatorNode,
  ParenthesisNode,
  SymbolNode,
  config,
  equal,
  isZero,
  numeric,
  parse,
  simplify,
  typed
});
export var rationalize = createRationalize({
  bignumber,
  fraction,
  AccessorNode,
  ArrayNode,
  ConstantNode,
  FunctionNode,
  IndexNode,
  ObjectNode,
  OperatorNode,
  ParenthesisNode,
  SymbolNode,
  add,
  config,
  divide,
  equal,
  isZero,
  mathWithTransform,
  matrix,
  multiply,
  parse,
  pow,
  simplify,
  simplifyConstant,
  simplifyCore,
  subtract,
  typed
});

_extends(math, {
  e,
  false: _false,
  fineStructure,
  i,
  Infinity: _Infinity,
  LN10,
  LOG10E,
  NaN: _NaN,
  null: _null,
  phi,
  SQRT1_2,
  sackurTetrode,
  tau,
  true: _true,
  'E': e,
  version,
  efimovFactor,
  LN2,
  pi,
  replacer,
  reviver,
  SQRT2,
  typed,
  unaryPlus,
  'PI': pi,
  weakMixingAngle,
  abs,
  acos,
  acot,
  acsc,
  addScalar,
  arg,
  asech,
  asinh,
  atan,
  atanh,
  bignumber,
  bitNot,
  boolean,
  clone,
  combinations,
  complex,
  conj,
  cosh,
  coth,
  csc,
  cube,
  equalScalar,
  erf,
  exp,
  expm1,
  filter,
  forEach,
  format,
  getMatrixDataType,
  hex,
  im,
  isInteger,
  isNegative,
  isPositive,
  isZero,
  LOG2E,
  lgamma,
  log10,
  log2,
  map,
  multiplyScalar,
  not,
  number,
  oct,
  pickRandom,
  print,
  random,
  re,
  sec,
  sign,
  sin,
  splitUnit,
  square,
  string,
  tan,
  typeOf,
  acosh,
  acsch,
  apply,
  asec,
  bin,
  chain,
  combinationsWithRep,
  cos,
  csch,
  isNaN,
  isPrime,
  randomInt,
  sech,
  sinh,
  sparse,
  sqrt,
  tanh,
  unaryMinus,
  acoth,
  cot,
  fraction,
  isNumeric,
  matrix,
  matrixFromFunction,
  mod,
  nthRoot,
  numeric,
  or,
  prod,
  reshape,
  size,
  smaller,
  squeeze,
  subset,
  subtract,
  to,
  transpose,
  xgcd,
  zeros,
  and,
  bitAnd,
  bitXor,
  cbrt,
  compare,
  compareText,
  concat,
  count,
  ctranspose,
  diag,
  divideScalar,
  dotDivide,
  equal,
  fft,
  flatten,
  gcd,
  hasNumericValue,
  hypot,
  ifft,
  kron,
  largerEq,
  leftShift,
  lsolve,
  matrixFromColumns,
  min,
  mode,
  nthRoots,
  ones,
  partitionSelect,
  resize,
  rightArithShift,
  round,
  smallerEq,
  unequal,
  usolve,
  xor,
  add,
  atan2,
  bitOr,
  catalan,
  compareNatural,
  cumsum,
  deepEqual,
  diff,
  dot,
  equalText,
  floor,
  identity,
  invmod,
  larger,
  log,
  lsolveAll,
  matrixFromRows,
  multiply,
  qr,
  range,
  rightLogShift,
  setSize,
  slu,
  sum,
  trace,
  usolveAll,
  asin,
  ceil,
  composition,
  cross,
  det,
  distance,
  dotMultiply,
  fix,
  intersect,
  lcm,
  log1p,
  max,
  quantileSeq,
  row,
  setCartesian,
  setDistinct,
  setIsSubset,
  setPowerset,
  sort,
  column,
  index,
  inv,
  pinv,
  pow,
  setDifference,
  setMultiplicity,
  sqrtm,
  vacuumImpedance,
  wienDisplacement,
  atomicMass,
  bohrMagneton,
  boltzmann,
  conductanceQuantum,
  createUnit,
  deuteronMass,
  dotPow,
  electricConstant,
  elementaryCharge,
  expm,
  faraday,
  firstRadiation,
  gamma,
  gravitationConstant,
  hartreeEnergy,
  klitzing,
  loschmidt,
  magneticConstant,
  molarMass,
  molarPlanckConstant,
  neutronMass,
  nuclearMagneton,
  planckCharge,
  planckLength,
  planckTemperature,
  protonMass,
  reducedPlanckConstant,
  rydberg,
  setIntersect,
  speedOfLight,
  stefanBoltzmann,
  thomsonCrossSection,
  avogadro,
  bohrRadius,
  coulomb,
  divide,
  electronMass,
  factorial,
  gravity,
  inverseConductanceQuantum,
  lup,
  magneticFluxQuantum,
  molarMassC12,
  multinomial,
  parse,
  permutations,
  planckMass,
  quantumOfCirculation,
  resolve,
  secondRadiation,
  simplifyConstant,
  stirlingS2,
  unit,
  bellNumbers,
  compile,
  eigs,
  fermiCoupling,
  leafCount,
  mean,
  molarVolume,
  planckConstant,
  setSymDifference,
  simplifyCore,
  variance,
  classicalElectronRadius,
  evaluate,
  help,
  lusolve,
  median,
  setUnion,
  std,
  gasConstant,
  mad,
  parser,
  simplify,
  symbolicEqual,
  derivative,
  norm,
  rationalize,
  rotationMatrix,
  kldivergence,
  rotate,
  planckTime,
  config
});

_extends(mathWithTransform, math, {
  filter: createFilterTransform({
    typed
  }),
  forEach: createForEachTransform({
    typed
  }),
  map: createMapTransform({
    typed
  }),
  apply: createApplyTransform({
    isInteger,
    typed
  }),
  diff: createDiffTransform({
    bignumber,
    matrix,
    number,
    subtract,
    typed
  }),
  subset: createSubsetTransform({
    matrix,
    typed
  }),
  concat: createConcatTransform({
    isInteger,
    matrix,
    typed
  }),
  max: createMaxTransform({
    config,
    larger,
    numeric,
    typed
  }),
  min: createMinTransform({
    config,
    numeric,
    smaller,
    typed
  }),
  range: createRangeTransform({
    bignumber,
    matrix,
    config,
    larger,
    largerEq,
    smaller,
    smallerEq,
    typed
  }),
  sum: createSumTransform({
    add,
    config,
    numeric,
    typed
  }),
  cumsum: createCumSumTransform({
    add,
    typed,
    unaryPlus
  }),
  row: createRowTransform({
    Index,
    matrix,
    range,
    typed
  }),
  column: createColumnTransform({
    Index,
    matrix,
    range,
    typed
  }),
  index: createIndexTransform({
    Index
  }),
  mean: createMeanTransform({
    add,
    divide,
    typed
  }),
  variance: createVarianceTransform({
    add,
    apply,
    divide,
    isNaN,
    multiply,
    subtract,
    typed
  }),
  std: createStdTransform({
    map,
    sqrt,
    typed,
    variance
  })
});

_extends(classes, {
  BigNumber,
  Complex,
  Fraction,
  Matrix,
  Node,
  ObjectNode,
  OperatorNode,
  ParenthesisNode,
  Range,
  RelationalNode,
  ResultSet,
  ArrayNode,
  BlockNode,
  ConditionalNode,
  ConstantNode,
  DenseMatrix,
  RangeNode,
  Chain,
  FunctionAssignmentNode,
  SparseMatrix,
  AccessorNode,
  AssignmentNode,
  IndexNode,
  FibonacciHeap,
  ImmutableDenseMatrix,
  Index,
  Spa,
  Unit,
  SymbolNode,
  FunctionNode,
  Help,
  Parser
});

Chain.createProxy(math);
export { embeddedDocs as docs } from '../expression/embeddedDocs/embeddedDocs.js';