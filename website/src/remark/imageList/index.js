const imageList = require('./list');

function plugin(options) {
  imageList.call(this, options || {})
};

module.exports = plugin;