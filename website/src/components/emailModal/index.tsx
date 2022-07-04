import React, { useState } from 'react';
import { Alert } from 'antd';
import { CloseCircleOutlined } from '@ant-design/icons';
import {SubscribeMail} from '../subscribeMail'
import {EmailModalStyle, EmailModalContext} from './styles';

export const EmailModal = ({children}) => {
  const [isModalVisible, setIsModalVisible] = useState(false);
  const showModal = () => {
    setIsModalVisible(true);
  };

  const handleCancel = () => {
    setIsModalVisible(false);
  };

  const childs = React.Children.map(children, (child) => React.cloneElement(child, {
    onClick: showModal
  }))

  return (
    <>
      {childs}
      <EmailModalStyle
        title={null}
        visible={isModalVisible}
        width={776}
        closeIcon={<CloseCircleOutlined />}
        footer={null}
        onCancel={handleCancel}
      >
        <EmailModalContext>
          <SubscribeMail.Tip/>
          <Alert className='message' message={<SubscribeMail.Message />} type="info" />
        </EmailModalContext>
      </EmailModalStyle>
    </>
  );
}