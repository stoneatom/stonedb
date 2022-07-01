import React from 'react';
import { Button, Divider } from "antd";
import {ISocial} from './interface';

function formatNumber(num: number, unit = 'K') {
  let numStr = '';
  if(num < 1000) {
    numStr = String(num);
  } else {
    numStr = `${(num / 1000).toFixed(2)}${unit}`
  }
  return numStr
}

export const Social: React.FC<ISocial> = ({title, value, children, to}) => {
  const goto = () => {
    if(!to){
      return false;
    }
    window.open(to, 'blank');
  }

  return (
    <>
      {
        value ? (
          <Button onClick={goto}>
            {children}
            <span>{title}</span>
            <Divider type="vertical" />
            <b>{formatNumber(parseInt(value))}</b>
          </Button>
        ) : null
      }
    </>
  )
}
