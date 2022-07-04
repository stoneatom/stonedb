const visit = require('unist-util-visit');
const {select} = require('unist-util-select');

const plugin = (options) => {
  const transformer = async (ast) => {
    
    visit(ast, 'image', (node, index, parent) => {
      console.log('image', args)
    });
    visit(ast, 'paragraph', (node, index, parent) => {
      if(parent.type === 'root'){
        parent.class = 'images'
      }
    });
    visit(ast, 'listItem', (node, index, parent) => {

    });
    visit(ast, 'list', (node, index, parent) => {
      node.className = 'images'
    });
  };
  return transformer;
};

module.exports = plugin;