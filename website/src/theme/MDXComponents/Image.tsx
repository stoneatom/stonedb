import React from 'react';
import {pipe, split, map, fromPairs, trim, isEmpty, is} from 'ramda';
import {Image, OmitText} from '@site/src/components';
import {modifyKeyName} from '@site/src/utils';

export default function MDXImage({className, alt, src, title, ...props}: any): JSX.Element {
  const isCustomImg = title && title.indexOf(':') >= 0;
  const data = isCustomImg ? pipe(
    split(','),
    map(split(':')),
    fromPairs,
    modifyKeyName(trim)
  )(title) : title ? {title} : {};
  return (
    <>
      {
        isCustomImg ? (
          <Image src={src} className={className} alt={alt} to={data.to}>
            <OmitText size={20}>{data.title}</OmitText>
          </Image>
        ) : (
          <img
            loading="lazy"
            alt={alt}
            src={src}
            title={title}
            className={props.className}
          />
        )
      }
    </>
  )
}