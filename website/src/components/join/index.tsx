import React, { useState, useEffect } from 'react';
import {unified} from 'unified';
import remarkParse from 'remark-parse';
import { v4 as uuidv4 } from 'uuid';
import {pickWhen} from '@site/src/utils';
import MDXA from '@site/src/theme/MDXComponents/A';
import {LinkList} from './styles';

const Join: React.FC<any> = ({children}) => {
  const [list, setList] = useState(null)
  function init() {
    const node = unified().use(remarkParse).parse(children);

    const list = pickWhen('link', (acc, cur) => {
      acc.push(cur);
      return acc;
    }, [node]);
    setList(list);
  }

  useEffect(() => {
    init();
  }, []);

  return (
    <LinkList>
      {
        list && list.length && list.map(({children, ...props}) => (
          <MDXA key={uuidv4()} {...props}>{children[0].value}</MDXA>
        ))
      }
    </LinkList>
  );
};
export default Join;