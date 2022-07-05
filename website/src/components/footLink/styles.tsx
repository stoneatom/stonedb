import styled from 'styled-components';
import {Modal} from 'antd';
import ULink from '@docusaurus/Link';

export const Link = styled(ULink)`
  font-size: 30px;
  color: var(--safe-text-color-secondary-dark);
  &:hover{
    color: var(--safe-text-color-dark);
  }
`;

export const EmailModal = styled(Modal)`
  height: 385px;
  .ant-modal-close{
    margin-top: -40px;
    margin-right: -40px;
    color: #fff;
    .ant-modal-close-x{
      font-size: 22px;
    }
  }
`;

export const EmailModalContext = styled.div`
  display: flex;
  flex-direction: row;
  background-color: red;;
  @media (max-width: 996px){
    flex-direction: column;
  }
`