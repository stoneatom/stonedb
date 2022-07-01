import React from 'react';
import { Space, Divider } from "antd";
import Translate from '@docusaurus/Translate';
import {IBase} from '../default.interface';

export const Message: React.FC<IBase> = ({className}) => (
  <Space direction="vertical" className={className}>
    <dl>
      <dt>
        <Translate id="subscribe.message.title">加入邮件列表</Translate>
      </dt>
      <dd></dd>
      <dd>
        <Translate id="subscribe.message.p.1">邮件列表是我们公开讨论并记录的地方。如果你希望：</Translate>
      </dd>
      <dd>
        <Translate id="subscribe.message.p.2">参与讨论开发计划或特定的问题</Translate>
      </dd>
      <dd>
        <Translate id="subscribe.message.p.3">帮助通过邮件提问的人</Translate>
      </dd>
      <dd>
        <Translate id="subscribe.message.p.4">帮助通过邮件提问的人</Translate>
      </dd>
    </dl>
    <Divider dashed={true} className="br" />
    <p>
      <Translate id="subscribe.message.note">如果你有一个特定的问题要问，建议使用 GitHub Issue 报 Bug 或提需求，这是一种更有效率的方式。</Translate>
    </p>
  </Space>
);
