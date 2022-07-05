 import styled from "styled-components";
 import {Space} from 'antd';
 import {SubscribeMail} from '../subscribeMail'
 import {DocContext} from '../styles';

 export const SubscribeStyle = styled.div`
  width: 100%;
  height: 400px;
  background-image: url(https://static.stoneatom.com/assets/rc-upload-1652691483537-2_subscribeBg.png);
  background-repeat: no-repeat;
  background-position: right center;
  background-size: 100% 100%;
  margin: 60px auto;
  @media (max-width: 996px){
    width: 100%;
    margin-left: 0;
    background-image: none;
    height: auto;
  }
 `

export const Message = styled(SubscribeMail.Message)`
  color: #fff;
  .br {
    border-color: #fff;
    min-width: 30px;
    max-width: 30px;
  }
  @media (max-width: 996px){
    background-color: #00A6FB;
    padding: var(--ifm-spacing-horizontal);
  }
`

export const FormStyle = styled(SubscribeMail.Form)`
  width: 300px;
  height: 300px;
  background: #FFFFFF;
  box-shadow: 0px 30px 50px 0px rgba(55, 60, 67, 0.2);
  border-radius: 10px;
  padding: 20px;
  
  .ant-btn{
    width: 100%;
  }
`

export const TipStyle = styled(SubscribeMail.Tip)`
  width: 300px;
  height: 300px;
  background: #FFFFFF;
  box-shadow: 0px 30px 50px 0px rgba(55, 60, 67, 0.2);
  border-radius: 10px;
  padding: 20px;
  margin-top: 20px;
  .ant-btn{
    width: 100%;
  }
  @media (max-width: 996px){
    width: 90%;
    height: auto;
    flex: 1;
    margin-top: -20px;
    margin-bottom: 20px;
    margin-left: var(--ifm-spacing-horizontal);
    margin-right: var(--ifm-spacing-horizontal);
  }
`

export const Mail = styled.div`
  height: 100%;
  width: 100%;
  display: flex;
  flex-direction: row;
  padding: 30px 0 30px 30px;
  @media (max-width: 996px){
    flex-direction: column;
    padding: 0;
    width: 120%;
    height: 120%;
    margin: -10%;
    > div:first-child {
      width: 100%;
      padding: 30px;
    }
  }
`
