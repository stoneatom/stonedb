import styled from 'styled-components';
import { Panel } from '../styles';

export const PanelWrap = styled(Panel)`
  background-size: contain;
  background-position-x: center;
`

export const Title = styled.div`
  text-align: center;
  font-size: 24px;
  font-weight: 500;
  color: #373C43;
  line-height: 33px;
`

export const ItemWrap = styled.div`
  text-align: center;
  margin: 36px 25px;
  img{
    height: 46px;
    max-width: auto;
  }
`

export const ListWrap = styled.div`
  display: flex;
  justify-content: center;
`