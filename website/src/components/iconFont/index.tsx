import React, { useEffect, useState } from 'react';
import lottie from 'lottie-web'
import { createFromIconfontCN } from '@ant-design/icons';
import md5 from 'js-md5';
import {Span} from './styles';

const Icon = createFromIconfontCN({
    scriptUrl: [
        '//static.stoneatom.com/assets/rc-upload-1656646066193-4_iconfont.js'
    ],
  });

  


  export const IconFont = ({type, className}: any) => {
    const [lotties, setLotties] = useState({});
    const id = md5(type);
    const onMouseEnter = () => {
      lotties[id].play();
    }
  
    const onMouseLeave = () => {
      lotties[id].stop();
      
    }
    useEffect(() => {
      if(type.indexOf('lottie-') >= 0) {
        const instance = lottie.loadAnimation({
          container: document.getElementById(id),
          renderer: 'svg',
          loop: true,
          autoplay: false,
          path: `https://stoneatom-static.oss-cn-hangzhou.aliyuncs.com/assets/${type}.json`
        });
        setLotties(Object.assign(lotties, {[id]: instance}))
        return () => {
          lotties[id].destroy();
        }
      }   
    }, []);

    return (
        <>
        {
            type.indexOf('lottie-') >= 0  ? (
              <Span id={id} className={`${className} icon`} onMouseEnter={onMouseEnter} onMouseLeave={onMouseLeave}></Span>
            ) : (
              <Icon type={type} className={`${className} icon`} />
            )
        }
        </>
    )
  }