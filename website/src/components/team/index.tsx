import React, { useState, useEffect } from 'react';
import {unified} from 'unified';
import remarkParse from 'remark-parse';
import {reduce} from 'ramda';
import { v4 as uuidv4 } from 'uuid';
import {pickWhen} from '@site/src/utils';
import MDXImage from '@site/src/theme/MDXComponents/Image'
import {SeeMore} from '../seeMore';
import {Row, Col} from './styles';

const Team: React.FC<any> = ({children}) => {
  const [list, setList] = useState(null);
  
  function init() {
    const node = unified().use(remarkParse).parse(children);
    

    const list = pickWhen('image',  (acc, cur) => {
      acc.push(cur)
      return acc;
    }, [node]);

    setList(list);
  }

  useEffect(() => {
    init();
  }, []);

  return (
    <Row>
      <SeeMore size={5}>
        {
          list && list.length ? list.map(({url, alt, title}) => (
            <Col key={uuidv4()}>
              <MDXImage src={url} alt={alt} title={title} />
            </Col>
          )) : null
        }
      </SeeMore>
    </Row>
  );
};
export default Team;