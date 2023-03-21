
import React from 'react';
import {GridProps} from './interface';
import {GridWrap} from './style';

export const Grid: React.FC<GridProps>= ({
  align,
  children,
  className = '',
  fixedColumnWidth = false,
  ...props
}) => {
  return (
    <GridWrap align={align} fixedColumnWidth className={className} {...props}>
      {children}
    </GridWrap>
  );
};

