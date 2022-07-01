import React from 'react';
import {ForkOutlined} from '@ant-design/icons';
import {IFork} from './interface';
import {Social} from '../social';

export const Fork: React.FC<IFork> = ({value}) => {
  return (
    <Social title="Fork" value={value} to="https://github.com/stoneatom/stonedb">
      <ForkOutlined />
    </Social>
  );
}
