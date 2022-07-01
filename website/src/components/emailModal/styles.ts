import styled from 'styled-components';
import {Modal} from 'antd';

export const EmailModalStyle = styled(Modal)`
  height: 385px;
  .ant-modal-close{
    margin-top: -40px;
    margin-right: -40px;
    color: #fff;
    .ant-modal-close-x{
      font-size: 22px;
    }
  }
  @media (max-width: 996px){
    .ant-modal-close{
      margin-top: -50px;
      margin-right: -20px;
      color: #fff;
      .ant-modal-close-x{
        font-size: 22px;
      }
    }
  }
`;

export const EmailModalContext = styled.div`
  display: flex;
  flex-direction: row;
  @media (max-width: 996px){
    flex-direction: column;
    .message{
      margin-top: 20px;
    }
  }
`