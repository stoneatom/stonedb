import React from 'react';
import {RowProps} from './interface';
import {RowWrap} from './style';

export const Row: React.FC<RowProps> = ({
    children = '',
    className,
    ...props
  }) => {
  
    return (
      <RowWrap className={className} {...props}>
        {children}
      </RowWrap>
    );
  };
