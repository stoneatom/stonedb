import React, { useState, useEffect } from 'react';
import {unified} from 'unified';
import remarkParse from 'remark-parse';
import {flatten} from 'ramda';
import {pickWhen} from '@site/src/utils';
import { Grid, Row, Cell } from "@site/src/hm";
import kunpeng from '@site/static/resource/kunpeng.png';
import tx from '@site/static/resource/tx.png';
import Opengauss from '@site/static/resource/Opengauss.png';
import BgChinaly from '@site/static/resource/BgChinaly.png';
import { Title, Context } from "../styles";
import {ItemWrap, PanelWrap} from './styles';

const imgs: any = {
  kunpeng,
  tx,
  Opengauss
}

const ShowCase: React.FC<any> = ({children}) => {
  const [title, setTitle] = useState(null);
  const [list, setList] = useState<any[]>(null);
  function reduceParagraph(list: any[]) {
    if(!list.length) {
      return [];
    }
    return pickWhen('paragraph', (acc, cur) => {
      const {value} = cur.children?.[0];
      acc.push(value);
      return acc;
    }, list);
  }

  function reduceList(arr: any[]) {
    return arr.map((item) => {
      const title = item.children[0] && item.children[0].type === 'paragraph' ? item.children[0].children[0]?.value : '';
      const nodeList = item.children[1]?.children;
      const res =  nodeList?.map((data) => {
        const [logo, name] = reduceParagraph(data.children);
        return {
          logo,
          name
        }
      });
      return {
        title,
        list: res
      }
    });
  }

  function reduceNode(node: any){
    return node.children.map((data: any) => reduceList(data.children));
  }

  function init() {
    const node: any = unified().use(remarkParse).parse(children);
    const data = flatten(reduceNode(node))[0];
    setTitle(data?.title);
    setList(data?.list);
  }

  useEffect(() => {
    init();
  }, []);

  return (
    <PanelWrap color='#232326' bg={BgChinaly}>
      <Context>
        <Title color="#FFFFFF">{title}</Title>
        <Grid>
          <Row>
            {
              list?.map(({logo, name}) => (
                <Cell key={logo}>
                  <ItemWrap>
                    <dt>{name}</dt>
                    <dd><img src={imgs[logo]} /></dd>
                  </ItemWrap>
                </Cell>
              ))
            }
          </Row>
        </Grid>
      </Context>
    </PanelWrap>
  )
}

export default ShowCase;