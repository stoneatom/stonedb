import React, { useEffect } from 'react';
import { message, Input } from 'antd';
import { SendOutlined } from '@ant-design/icons';
import {EMAIL} from '@site/src/utils';
import { ISubscribeForm } from './interface';
import { FormStyle, SubscribeBtn } from './styles';

export const Form: React.FC<ISubscribeForm> = ({ onSubmit, className }) => {
  const doSunscribe = async(values: any) => {
    
  };

  return (
    <FormStyle layout="vertical" className={className} onFinish={doSunscribe}>
      <FormStyle.Item label="如何称呼您(选填)" name="name" rules={[{ required: true, message: '请输入您的姓名' }]}>
        <Input placeholder="请输入姓名" />
      </FormStyle.Item>
      <FormStyle.Item label="您的邮箱" name="mail" rules={[{ required: true, message: '请输入正确的邮箱', pattern: EMAIL }]}>
        <Input placeholder="your@email.com" />
      </FormStyle.Item>
      <FormStyle.Item>
        <SubscribeBtn size="middle" htmlType="submit" type="primary">
          <SendOutlined />
          订阅
        </SubscribeBtn>
      </FormStyle.Item>
    </FormStyle>
  );
};
