import {curry, reduce, assoc, keys, is} from 'ramda';

interface keysMap {
  [key: string]: (key: string) => string;
}

export const modifyKeyName = curry((keysMap: keysMap | Function, obj) => {
  return reduce((acc, key) => {
    const fn = is(Function, keysMap) ? keysMap : keysMap[key];
    const resultKey = fn(key);
    return assoc(resultKey, obj[key], acc)
  }, {}, keys(obj))
})