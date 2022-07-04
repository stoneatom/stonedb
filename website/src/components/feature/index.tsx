import React from 'react';
import {Space} from 'antd';
import Translate from '@docusaurus/Translate';
import { IconFont } from '../icon';
import {Title, Context} from '../styles';
import {Item, List} from './styles';

const Feature = () => {
  return (
    <Context>
      <Title>
        <Translate id="home.feat.title">版本特性</Translate>
      </Title>
      <List wrap={true}>
        <Item>
          <IconFont type='lottie-compatible' className='icon' trigger="hover" />
          <b>
            <Translate id="home.compatible.title">100%兼容MySQL 5.7</Translate>
          </b>
          <p>
            <Translate id="home.compatible.desc">基于MySQL5.7版本开发，完全兼容所有MySQL语法与接口，支持从MySQL直接更新升级</Translate>
          </p>
        </Item>
        <Item>
          <IconFont type='lottie-hybrid' className='icon' />
          <b>
            <Translate id="home.hybrid.title">混合事务和分析处理</Translate>
          </b>
          <p>
            <Translate id="home.hybrid.desc">在MySQL事务处理（TP）功能的基础上，增加分析处理（AP）功能</Translate>
          </p>
        </Item>
        <Item>
          <IconFont type='lottie-realTime' className='icon' />
          <b>
            <Translate id="home.realTime.title">实时分析，低时延</Translate>
          </b>
          <p>
            <Translate id="home.realTime.desc">无需数据提取，直接实时分析，分析操作几乎无延时</Translate>
          </p>
        </Item>
        <Item>
          <IconFont type='lottie-query' className='icon' />
          <b>
            <Translate id="home.query.title">10倍查询速度</Translate>
          </b>
          <p>
            <Translate id="home.query.desc">复杂query查询速度可提升10倍</Translate>
          </p>
        </Item>
        <Item>
          <IconFont type='lottie-initial' className='icon' />
          <b>
            <Translate id="home.initial.title">10倍初始导入速度</Translate>
          </b>
          <p>
            <Translate id="home.initial.desc">数据库初始导入速度可提升10倍</Translate>
          </p>
        </Item>
        <Item>
          <IconFont type='lottie-storage' className='icon' />
          <b>
            <Translate id="home.storage.title">1/10 存储占用</Translate>
          </b>
          <p>
            <Translate id="home.storage.desc">高效压缩算法，节省大量存储空间</Translate>
          </p>
        </Item>
      </List>
    </Context>
  )
}
export default Feature;