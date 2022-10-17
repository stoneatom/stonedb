import { Button } from 'antd';
import styled from 'styled-components';
import { Panel } from '../styles';

export const PanelWrap = styled(Panel)`
  background-size: auto 100%;
  background-position-x: right;
  padding: 0 0 109px 0;
`

export const Item = styled.div`
  flex: 1;
  padding: 14px;
  height: 500px;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: flex-start;
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
  b{display: inline-block; margin-right: 8px;}
  @media (max-width: 996px){
    display: none;
  }
`
