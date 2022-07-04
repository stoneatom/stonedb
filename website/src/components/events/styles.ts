 import styled from "styled-components";
 import Link from '@docusaurus/Link';
 import {Image} from '../image';

export const Row = styled.div`
  display: flex;
  flex-direction: row;
  flex-wrap: wrap;
  justify-content: flex-start;
  margin: 0 -8px;
  overflow: hidden;
  position: relative;
  @media (max-width: 996px){
    margin: 0 14px;
  }
`
export const Col = styled.div`
  width: 50%;
  padding: 8px;
  display: flex;
  justify-content: center;
  align-items: center;
  position: relative;
  overflow: hidden;
  > div {
    width: 100%;
    height: 100%;
    border-radius: 10px;
  }
  @media (max-width: 996px){
    width: 100%;
  }
`
export const Title = styled.h4`
  height: 27px;
  font-size: 22px;
  font-weight: 400;
  color: #FFFFFF;
  line-height: 27px;
`

export const Desc = styled.p`
  height: 30px;
  font-size: 16px;
  font-weight: 300;
  color: #FFFFFF;
  line-height: 30px;
`

export const LinkStyle = styled(Link)`
  height: 19px;
  font-size: 16px;
  font-weight: 400;
  color: #FFFFFF;
  line-height: 19px;
`
