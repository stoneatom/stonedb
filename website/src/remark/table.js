const visit = require('unist-util-visit');

const checkCellNull = ({children}) => {
  const nullCells = children.filter((data) => data.children.length === 0);
  return nullCells.length === children.length;
}

const plugin = (options) => {
  const transformer = async (ast) => {
    visit(ast, 'table', (node) => {
      console.log('table', node)
      const index = node.children.findIndex((data) => {
        const res = checkCellNull(data);
        console.log(res, data)
        return res;
      });
    });
  };
  return transformer;
};

module.exports = plugin;