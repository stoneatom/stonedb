import React from 'react';
import {GithubOutlined} from '@ant-design/icons';
import {Social} from '../social';
import {IStar} from './interface'

export const Star: React.FC<IStar> = ({value}) => {

  return (
    <Social title="Star" value={value} to="https://github.com/stoneatom/stonedb">
      <GithubOutlined />
    </Social>
  );
}
