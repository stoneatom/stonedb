import styled from 'styled-components';
import BorderChinaly from '@site/static/resource/BorderChinaly.png';
import { Panel } from '../styles';

export const PanelWrap = styled(Panel)`
  background-size: contain;
  background-position-x: center;
`

export const ItemWrap = styled.dl`
  text-align: center;
  background-image: url(${BorderChinaly});
  background-size: 100%;
  background-repeat: no-repeat;
  padding: 40px;
  dt{
    font-size: 22px;
    font-weight: 500;
    color: #FFFFFF;
    line-height: 30px;
  }
  dd{
    
  }
`