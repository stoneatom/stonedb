import styled from 'styled-components';
import {IImage} from './interface';



export const ImageStyle = styled.div.attrs(({width, height, src}: IImage) => {
  const size = width || height || 'auto';
  return {
    height: height ? `${height}px` : size,
    width: width ? `${width}px` : size,
    src: src || 'none',
  }
})`
  background-image: url(${props => props.src});
  background-repeat: no-repeat;
  background-size: cover;
  width: ${props => props.width};
  height: ${props => props.height};
  overflow: hidden;
  padding: 16px;
  position: relative;
  padding-top: 100%;
  cursor: pointer;
  &:hover{
    .mask {
      background-color: transparent;
    }
  }
`

export const Mask = styled.div`
  background-color: rgba(0,0,0,0.6);
  position: absolute;
  left: 0;
  right: 0;
  top: 0;
  bottom: 0;
  z-index: 0;
`

export const Title = styled.div`
  position: absolute;
  bottom: 16px;
  z-index: 1;
  color: #fff;
`;
