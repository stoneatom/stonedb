import { Space } from 'antd';
import styled from 'styled-components';

export const List = styled(Space)`
  .ant-space-item{
    padding: 45px;
  }
  @media (max-width: 996px){
    padding: 0;
    .ant-space-item{
      width: 100%;
      padding: 0 45px;
    }
  }
`

export const Item = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: flex-start;
  align-items: center;
  width: 300px;
  text-align: center;
  .icon{
    font-size: 150px;
    width: 150px;
    height: 200px;
  }
  @media (max-width: 996px){
    width: 100%;
    padding: 14px;
  }
`
