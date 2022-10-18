import React from 'react';
import {CellProps} from './interface';
import {CellWrap} from './style';

export const Cell: React.FC<CellProps> = ({
  children = '',
  className = '',
  span,
  order,
  ...props
}) => {
  return (
    <CellWrap span={span} className={className} {...props}>
      {children}
    </CellWrap>
  );
};
