import React, {ReactNode, useState} from 'react';
import {DownOutlined, UpOutlined} from '@ant-design/icons';
import Translate from '@docusaurus/Translate';
import {ISeeMore} from './interface';
import {Button} from './styles';

export const SeeMore: React.FC<ISeeMore> = ({children = [], size}) => {
  const [more, setMore] = useState(false);
  const childs = children as ReactNode[];
  const show = childs?.slice(0, size);
  const hide = childs?.slice(size);
  const toggleMore = () => {
    setMore(!more);
  }
  const Less = <Translate id="less">Less</Translate>;
  const More = <Translate id="more">More</Translate>;
  return (
    <>
      {show}
      {
        more ? hide : null
      }
      <Button onClick={toggleMore}>{more ? Less : More}{more ? <UpOutlined /> : <DownOutlined />}</Button>
    </>
  )
}
