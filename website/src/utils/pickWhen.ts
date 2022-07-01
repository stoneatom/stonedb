import {curry, reduce} from 'ramda';


export const pickWhen = curry((key, reduceFn, arr) => {
  const fn = reduce((acc, cur) => {
    if(cur.type === key){
      acc = reduceFn(acc, cur);
    } else if(cur.children && cur.children.length) {
      acc = acc.concat(fn([], cur.children))
    }
    return acc;
  });
  return  fn([], arr);
})