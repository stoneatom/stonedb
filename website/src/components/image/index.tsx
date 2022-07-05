import React from 'react';
import { useHistory } from '@docusaurus/router';
import {IImage} from './interface';
import {ImageStyle, Title, Mask} from './styles';

export const Image: React.FC<IImage> = ({src, children, className, alt, to}) => {
  const history = useHistory();
  const linkTo = () => {
    if(to.indexOf('//') === 0) {
      window.open(to, 'blank')
    } else {
      history.push(to);
    }
  }
  return (
    <ImageStyle src={src} className={className} loading="lazy" alt={alt} onClick={to ? linkTo : null}>
      <Title>{children}</Title>
      <Mask className='mask' />
    </ImageStyle>
  )
}