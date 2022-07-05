import React from 'react';
import { Tooltip } from 'antd';
import {IOmitText} from './interface';

export const OmitText: React.FC<IOmitText> = ({ children, size }) => {
  const value = children as string || '';
  return (
    <Tooltip title={value.length > size ? value : null}>
      <span>
        {value.substring(0, size)}
        {size < value.length ? '...' : ''}
      </span>
    </Tooltip>
  );
};
