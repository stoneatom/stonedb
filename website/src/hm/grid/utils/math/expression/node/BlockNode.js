import _defineProperty from "@babel/runtime/helpers/defineProperty";
import { isNode } from '../../utils/is.js';
import { forEach, map } from '../../utils/array.js';
import { factory } from '../../utils/factory.js';
var name = 'BlockNode';
var dependencies = ['ResultSet', 'Node'];
export var createBlockNode = /* #__PURE__ */factory(name, dependencies, _ref => {
  var {
    ResultSet,
    Node
  } = _ref;

  class BlockNode extends Node {
    /**
     * @constructor BlockNode
     * @extends {Node}
     * Holds a set with blocks
     * @param {Array.<{node: Node} | {node: Node, visible: boolean}>} blocks
     *            An array with blocks, where a block is constructed as an
     *            Object with properties block, which is a Node, and visible,
     *            which is a boolean. The property visible is optional and
     *            is true by default
     */
    constructor(blocks) {
      super(); // validate input, copy blocks

      if (!Array.isArray(blocks)) throw new Error('Array expected');
      this.blocks = blocks.map(function (block) {
        var node = block && block.node;
        var visible = block && block.visible !== undefined ? block.visible : true;
        if (!isNode(node)) throw new TypeError('Property "node" must be a Node');

        if (typeof visible !== 'boolean') {
          throw new TypeError('Property "visible" must be a boolean');
        }

        return {
          node,
          visible
        };
      });
    }

    get type() {
      return name;
    }

    get isBlockNode() {
      return true;
    }
    /**
     * Compile a node into a JavaScript function.
     * This basically pre-calculates as much as possible and only leaves open
     * calculations which depend on a dynamic scope with variables.
     * @param {Object} math     Math.js namespace with functions and constants.
     * @param {Object} argNames An object with argument names as key and `true`
     *                          as value. Used in the SymbolNode to optimize
     *                          for arguments from user assigned functions
     *                          (see FunctionAssignmentNode) or special symbols
     *                          like `end` (see IndexNode).
     * @return {function} Returns a function which can be called like:
     *                        evalNode(scope: Object, args: Object, context: *)
     */


    _compile(math, argNames) {
      var evalBlocks = map(this.blocks, function (block) {
        return {
          evaluate: block.node._compile(math, argNames),
          visible: block.visible
        };
      });
      return function evalBlockNodes(scope, args, context) {
        var results = [];
        forEach(evalBlocks, function evalBlockNode(block) {
          var result = block.evaluate(scope, args, context);

          if (block.visible) {
            results.push(result);
          }
        });
        return new ResultSet(results);
      };
    }
    /**
     * Execute a callback for each of the child blocks of this node
     * @param {function(child: Node, path: string, parent: Node)} callback
     */


    forEach(callback) {
      for (var i = 0; i < this.blocks.length; i++) {
        callback(this.blocks[i].node, 'blocks[' + i + '].node', this);
      }
    }
    /**
     * Create a new BlockNode whose children are the results of calling
     * the provided callback function for each child of the original node.
     * @param {function(child: Node, path: string, parent: Node): Node} callback
     * @returns {BlockNode} Returns a transformed copy of the node
     */


    map(callback) {
      var blocks = [];

      for (var i = 0; i < this.blocks.length; i++) {
        var block = this.blocks[i];

        var node = this._ifNode(callback(block.node, 'blocks[' + i + '].node', this));

        blocks[i] = {
          node,
          visible: block.visible
        };
      }

      return new BlockNode(blocks);
    }
    /**
     * Create a clone of this node, a shallow copy
     * @return {BlockNode}
     */


    clone() {
      var blocks = this.blocks.map(function (block) {
        return {
          node: block.node,
          visible: block.visible
        };
      });
      return new BlockNode(blocks);
    }
    /**
     * Get string representation
     * @param {Object} options
     * @return {string} str
     * @override
     */


    _toString(options) {
      return this.blocks.map(function (param) {
        return param.node.toString(options) + (param.visible ? '' : ';');
      }).join('\n');
    }
    /**
     * Get a JSON representation of the node
     * @returns {Object}
     */


    toJSON() {
      return {
        mathjs: name,
        blocks: this.blocks
      };
    }
    /**
     * Instantiate an BlockNode from its JSON representation
     * @param {Object} json
     *     An object structured like
     *     `{"mathjs": "BlockNode", blocks: [{node: ..., visible: false}, ...]}`,
     *     where mathjs is optional
     * @returns {BlockNode}
     */


    static fromJSON(json) {
      return new BlockNode(json.blocks);
    }
    /**
     * Get HTML representation
     * @param {Object} options
     * @return {string} str
     * @override
     */


    toHTML(options) {
      return this.blocks.map(function (param) {
        return param.node.toHTML(options) + (param.visible ? '' : '<span class="math-separator">;</span>');
      }).join('<span class="math-separator"><br /></span>');
    }
    /**
     * Get LaTeX representation
     * @param {Object} options
     * @return {string} str
     */


    _toTex(options) {
      return this.blocks.map(function (param) {
        return param.node.toTex(options) + (param.visible ? '' : ';');
      }).join('\\;\\;\n');
    }

  }

  _defineProperty(BlockNode, "name", name);

  return BlockNode;
}, {
  isClass: true,
  isNode: true
});