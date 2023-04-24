import styled, { StyledComponent } from 'styled-components';
import Icon from "@site/src/icon";
import {Context} from '../../styles';

export const StepWrap = styled.div`
  width: 100%;
  height: 376px;
  position: relative;
  &::before {
    content: '';
    display: block;
    position: absolute;
    z-index: 0;
    left: 0;
    bottom: 7px;
    width: 100%;
    height: 2px;
    background: #E5E8F0;
  }
`;

const StepItemActive = `
  background: #FC5A03;
  box-shadow: 0px 5px 20px 0px rgba(252,90,3,0.4);
  &::before{
    background: #FC5A03;
  }
  dt, dd {
    color: #fff;
  }
`

const StepItemDisable = `
  background: #F2F3F7;
  &::before{
    background: #E5E8F0;
  }
  dt, dd {
    color: #989CA8;
  }
`

const StepItemDefault = `
  background: #F2F3F7;
  &::before{
    background: #FC5A03;
  }
  dt{
    color: #373C43;
  }
  dd{
    p {
      color: #646A7D;
    }
    span{
      color: #FC5A03;
    }
  }
`

export const StepItemWrap: StyledComponent<"div", any, {active?: boolean;disable?: boolean;}> = styled.div`
  width: 320px;
  height: 100%;
  display: inline-block;
  z-index: 2;
  position: relative;
  dl{
    width: 300px;
    height: 320px;
    border-radius: 10px;
    position: relative;
    overflow: visible;
    display: flex;
    padding: 40px 20px 20px 20px;
    flex-direction: column;
    margin: 0;
    z-index: 2;
    &::before{
      content: ' ';
      display: block;
      width: 16px;
      height: 16px;
      position: absolute;
      bottom: -56px;
      left: 50%;
      margin-left: -15px;
      z-index: 3;
      transform: rotate(134deg);
      outline: 7px solid #ffffff;
    }

    ${({active, disable}: any) => active ? StepItemActive : disable ? StepItemDisable : StepItemDefault}
  }

  &::after{
    content: '';
    display: block;
    width: calc(100% - 5px);
    right: 50%;
    bottom: 7px;
    background: #FC5A03;
    height: 2px;
    position: absolute;
  }

  &:first-child::after{
    width: 100vh;
  }
  
  ${({disable}: any) => disable ? `
    &::after{
      background: #E5E8F0;
    }
  ` : null}
  dt{
    font-size: 22px;
    font-weight: 500;
    line-height: 30px;
    margin-bottom: 20px;
  }
  dd{
    flex: 1;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    overflow-y: auto;
    p {
      font-size: 18px;
      font-weight: 400;
      line-height: 30px;
      flex: 1;
    }
    span{
      font-size: 16px;
      font-weight: 400;
      line-height: 24px;
    }
    ol{
      margin: 0;
      padding-left: 16px;
      flex: 1;
    }
  }
  /* ${({active, disable}: any) => active ? StepItemActive : disable ? StepItemDisable : StepItemDefault}   */
`

export const StepContext = styled(Context)`
  height: 100%;
  overflow: visible;
  padding: 0;
  z-index: 0;
  .wrap{
    min-width: 100%;
    height: 100%;
    ${({scroll, counts}: any) => `
      width: ${counts*320}px;
      transform: translateX(-${scroll <=0 ? 0 : scroll}px);
    `}
  }
`

export const ButtonWrap = styled.button`
  width: 50px;
  height: 50px;
  line-height: 50px;
  border-radius: 50px;
  border: 0;
  background: #FFFFFF;
  color: #FC5A03;
  box-shadow: 0px 5px 20px 0px #E5E8F0;
  position: absolute;
  top: 50%;
  z-index: 10;
  ${({type}: any) => type === 'left' ? `
    left: 50px;
    transform: rotate(90deg);
  ` : `
    right: 50px;
    transform: rotate(270deg);
  `}
`


export const ButtonIcon = styled(Icon)`
  font-size: 14px;
  fill: #FC5A03;
  width: 14px;
  height: 14px;
  line-height: 14px;
  text-align: center;
  cursor: pointer;
`

export const Logo = styled(Icon)`
  fill: #ffffff;
  font-size: 20px;
  width: 20px;
  height: 20px;
  line-height: 20px;
  text-align: center;
  position: absolute;
  right: 10px;
  top: 10px;
`