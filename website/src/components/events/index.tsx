import React, { useEffect, useState } from 'react';
import {unified} from 'unified';
import remarkParse from 'remark-parse';
import { v4 as uuidv4 } from 'uuid';
import {pickWhen} from '@site/src/utils';
import {Image} from '../image';
import {Row, Col, Title, Desc, LinkStyle} from './styles';

const Events: React.FC<any> = ({children}) => {
  const [list, setList] = useState(null);

  function init() {
    const node = unified().use(remarkParse).parse(children);
    const list = pickWhen('listItem', (acc, cur) => {
      return acc.concat(cur.children);
    }, [node]);
    setList(list)
  }

  useEffect(() => {
    init();
  }, []);
  
  const renderItem = (children) => {
    const id = uuidv4();
    const image = children[0]?.type === 'image' ? children[0] : null;
    const texts = children[1]?.type === 'text' ? children[1].value.split('\n') : [];
    const link = children[2]?.type === 'link' ? children[2].url : '';
    return (
      <Col key={id}>
        {
          image && image.type ? (
            <Image src={''}>
              <Title>{texts[0]}</Title>
              <Desc>{texts[1]}</Desc>
              <LinkStyle to={link}><u>More</u></LinkStyle>
            </Image>
          ) : null
        }
      </Col>
    )
  }

  return (
    <Row>
      {
        list && list.length && list.map(({children}) => renderItem(children))
      }
    </Row>
  );
};
export default Events;