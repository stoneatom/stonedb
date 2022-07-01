 import styled from "styled-components";

export const First = styled.div`
  display: flex;
  flex-direction: row;
  flex-wrap: wrap;
  justify-content: flex-start;
  overflow: hidden;
  position: relative;
  margin: 10px -20px;
  @media (max-width: 996px){
    margin: 10px 0;
  }
  a{
    padding-top: 20px;
    font-size: 22px;
    font-weight: 400;
    color: #373C43;
  }
  > div {
    width: 50%;
    padding: 0 20px;
    @media (max-width: 996px){
      width: 100%;
      padding: 0;
    }
  }

`

export const Second = styled.div`
  display: flex;
  flex-direction: column;
  overflow: hidden;
  padding-left: 20px;
  > div{
    width: 100%;
    display: flex;
    flex-direction: column;
  }
  a{
    font-size: 18px;
    font-weight: 400;
    color: #646A7D;
  }
`

export const Third = styled.div`
  display: flex;
  flex-direction: row;
  flex-wrap: wrap;
  justify-content: flex-start;
  overflow: hidden;
  a{
    display: inline-block;
    margin: 0 20px;
    border-width: 0;
    font-size: 14px;
    font-weight: 300;
    color: #646A7D;
    @media (max-width: 996px){
      display: flex;
      width: 100%;
      margin: 0;
      margin-left: 20px;
      border-width: 1px;
    }
    span{
      opacity: 0;
      @media (max-width: 996px){
        opacity: 1;
      }
    }
    &:hover{
      border-width: 1px;
      span{
        opacity: 1;
      }
    }
  }
`

export const Fourth = styled.div`
  display: flex;
  flex-direction: row;
  flex-wrap: wrap;
  justify-content: flex-start;
  overflow: hidden;
  margin: 10px -20px;
  @media (max-width: 996px){
    margin: 10px 0;
  }
  > a {
    width: 20%;
    margin: 0 20px;
    padding-top: 20px;
    @media (max-width: 996px){
      width: 100%;
      margin: 0;
    }
  }
`;