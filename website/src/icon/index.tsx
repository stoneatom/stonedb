import React from 'react';
import {Span} from './styles';
import svgs from './svg';
import {IIcon} from './interface';
export {IIcon} from './interface';

const Icon: React.FC<IIcon> = ({name, ...props}) => {
  const Component: any = svgs[name];
  return (
    <Span {...props}>
      {Component}
    </Span>
  )
}

export default Icon;
