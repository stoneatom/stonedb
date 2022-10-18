import React, { useState, useEffect } from 'react';
import {unified} from 'unified';
import remarkParse from 'remark-parse';
import {flatten} from 'ramda';
import {pickWhen} from '@site/src/utils';
import aliyun from '@site/static/resource/aliyun.png';
import xbb from '@site/static/resource/xbb.png';
import BgCustomer from '@site/static/resource/BgCustomer.png';
import { Context } from "../styles";
import { Title, ItemWrap, ListWrap, PanelWrap} from './styles';

const imgs: any = {
  aliyun,
  xbb
}

const Customers: React.FC<any> = ({children}) => {
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
    <PanelWrap color='#E5E8F0' size="small" bg={BgCustomer}>
      <Context>
        <Title>{title}</Title>
        <ListWrap>
        {
          list?.map(({logo}) => (
            <ItemWrap key={logo}>
              <img src={imgs[logo]} />
            </ItemWrap>
          ))
        }
        </ListWrap>
      </Context>
    </PanelWrap>
  )
}

export default Customers;