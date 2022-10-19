import React from 'react';
import styled from 'styled-components';

export const Panel: React.FC<any> = styled.div`
  width: 100%;
  ${
    (props => props.size === 'small' ? `padding: 40px 0;` : `padding: 80px 0;`)
  }
  background-color: ${props => props.color || '#fff'};
  overflow: hidden;
  background-image: url(${props => props.bg});
  background-repeat: no-repeat;
  @media (max-width: 996px){
    padding: 0;
  }
`

export const Title = styled.div`
  font-size: 36px;
  font-weight: 500;
  line-height: 50px;
  margin-bottom: 38px;
  color: ${({color}) => color};
  &::after{
    content: '.';
  }
  @media (max-width: 996px){
    font-size: 16px;
    padding: 14px;
    margin-bottom: 0;
  }
`;

export const Context = styled.div`
  width: var(--safe-screen-xl);
  margin: 0 auto;
  overflow: hidden;
  @media (max-width: 996px){
    width: 100%;
  }
  @media (max-width: 1200px){
    padding: 14px;
  }
`

export const DocContext = styled.div`
  width: var(--ifm-container-width);
  margin: 0 auto;
  overflow: hidden;
  @media (max-width: 996px){
    width: 100%;
  }
`

export const SubTitle = styled.div`
  height: 34px;
  font-size: 28px;
  font-weight: 400;
  line-height: 34px;
  margin-bottom: 20px;
  margin-top: 80px;
  display: flex;
  flex-direction: row;
  justify-content: space-between;
`

export const EllipsisText = styled.p`
  overflow: hidden;
  text-overflow: ellipsis;
  position: relative;
  display: -webkit-box;
  -webkit-line-clamp: ${(props) => props.line!};
  -webkit-box-orient: vertical;
`