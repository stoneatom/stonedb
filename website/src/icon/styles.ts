import styled from "styled-components";
import {IIcon} from './interface';

export const Span = styled.span.attrs(({size, style, ...props}: IIcon) => {
  return {
    ...props,
    style,
    width: size || style?.fontSize || style?.width,
    height: size || style?.fontSize || style?.height,
  }
})`
  ${({style}) => ({...style})}
  fill: ${({color, style}) => color || style?.color || 'inherit'};
  font-size: ${({size, style}: IIcon) => size || style?.fontSize || 'inherit'};
  display: inline-block;
  width: ${({width}) => width};
  height: ${({height}) => height};
  line-height: ${({height}) => height};
  overflow: hidden;
  img {
    width: ${({width}) => width};
    height: ${({height}) => height};
    vertical-align: initial;
  }
`;
