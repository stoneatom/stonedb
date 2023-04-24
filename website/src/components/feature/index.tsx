import React, { useState, useEffect } from "react";
import {unified} from 'unified';
import remarkParse from 'remark-parse';
import {flatten} from 'ramda';
import {pickWhen} from '@site/src/utils';
import { Grid, Row, Cell } from "@site/src/hm";
import BgFeature from '@site/static/resource/BgFeature.png';
import {Item} from './Item';
import { Title, Context } from "../styles";
import {PanelWrap} from './styles';

const Feature: React.FC<any> = ({children}) => {
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
        const icon = data.children?.[0].children?.[0]?.value;
        const [title, desc] = reduceParagraph(data.children[1].children);
        const restData = data.children.slice(2);
        const list =  reduceParagraph(restData.filter((item: any) => item.ordered));
        const values = restData.filter((item: any) => !item.ordered).map((node: any) => reduceList(node.children));
        return {
          icon,
          title, desc,
          list,
          values: flatten(values)[0]
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
    <PanelWrap color='#232326' bg={BgFeature}>
    <Context>
      <Title color="#FFFFFF">{title}</Title>
      <Grid>
        <Row>
          {
            list && list.map((item: any) => (
              <Cell span={6} key={item.icon}>
                <Item {...item} />
              </Cell>
            ))
          }
        </Row>
      </Grid>
    </Context>
    </PanelWrap>
  );
};
export default Feature;
