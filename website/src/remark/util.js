

function isRemarkParser(parser) {
  return Boolean(parser && parser.prototype && parser.prototype.blockTokenizers)
}

function isRemarkCompiler(compiler) {
  return Boolean(compiler && compiler.prototype && compiler.prototype.visitors)
}

exports.isRemarkParser = isRemarkParser;
exports.isRemarkCompiler = isRemarkCompiler;
