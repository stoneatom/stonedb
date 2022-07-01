import { Button } from 'antd';
import styled from 'styled-components';


export const Item = styled.div`
  flex: 1;
  padding: 14px;
  h2 {
    font-size: 55px;
    font-weight: 400;
    line-height: 67px;
    color: var(--safe-text-color);
    @media (max-width: 996px){
      line-height: 24px;
      font-size: 20px;
    }
  }
  p{
    height: 25px;
    font-size: 18px;
    font-weight: 400;
    color: var(--safe-text-color-secondary);
    line-height: 22px;
    margin-bottom: 90px;
    @media (max-width: 996px){
      font-size: 12px;
      font-weight: 400;
      line-height: 20px;
    }
  }
`

export const DownLoadBtn = styled(Button)`
  width: 174px;
  @media (max-width: 996px){
    display: none;
  }
`
