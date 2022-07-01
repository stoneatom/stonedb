 import styled from "styled-components";

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
  width: 20%;
  padding: 8px;
  display: flex;
  justify-content: center;
  align-items: center;
  position: relative;
  overflow: hidden;
  > div {
    width: 100%;
    height: 100%;
    border-radius: 8px;
  }
  @media (max-width: 996px){
    width: 50%;
  }
`
