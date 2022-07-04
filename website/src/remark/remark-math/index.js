// const inlinePlugin = require('./inline')
const blockPlugin = require('./block')



function math(options) {
  var settings = options || {}
  blockPlugin.call(this, settings)
  // inlinePlugin.call(this, settings)
}

module.exports = math;