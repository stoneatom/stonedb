import React from 'react';
import { Space, Divider } from "antd";
import { SendOutlined } from '@ant-design/icons';
import Translate from '@docusaurus/Translate';
import {IBase} from '../default.interface';
import { SubscribeBtn, BR } from './styles';

export const Tip: React.FC<IBase> = ({className}) => {
  const onSubscribe = () => {
    window.open('mailto:dev-subscribe@stonedb.io?subject=Subscribe to the mailing list', 'blank');
  }
  return (
    <Space direction="vertical" className={className}>
      <dl>
        <dt>
          <Translate id="subscribe.tip.title">如何加入</Translate>
        </dt>
        <dd></dd>
        <dd>
          <Translate id="subscribe.tip.p.1">给 contact@stonedb.io 发送任意主题的邮件。</Translate>
        </dd>
        <dd>
          <Translate id="subscribe.tip.p.2">你会收到一封回信，请照着邮件内容操作。</Translate>
        </dd>
      </dl>
      <BR dashed={true} className="br" />
      <SubscribeBtn size="middle" type="primary" onClick={onSubscribe}>
        <SendOutlined />
        <Translate id="subscribe.tip.btn">马上订阅</Translate>
      </SubscribeBtn>
    </Space>
  );
  
}