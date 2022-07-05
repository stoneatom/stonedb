/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from 'react';
import {pipe, split, map, fromPairs, trim, isEmpty} from 'ramda';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {modifyKeyName} from '@site/src/utils';
import {LinkSocial, Link, LinkIcon, LinkBtn} from '@site/src/components';

export default function MDXA({url = '', children, title, href, ...props}: any): JSX.Element {
  const {
    i18n: {currentLocale}
  } = useDocusaurusContext();
  const data = title && title.indexOf(':') >= 0 ? pipe(
    split(','),
    map(split(':')),
    fromPairs,
    modifyKeyName(trim)
  )(title) : title ? {title} : {};
  const isLinkSocial = !!data.icon;
  const isLink = isEmpty(data);
  const isLinkIcon = data.type === 'export';
  const isLinkBtnMore = data.type === 'btnMore';
  data.local = data.local || currentLocale;
  return ( 
    <>
      {
        isLink && currentLocale === data.local  ? (
          <Link to={url || href}>{children}</Link>
        ) : null
      }
      {
        isLinkSocial && currentLocale === data.local ? (
          <LinkSocial to={url || href} icon={data.icon}>{children}</LinkSocial>
        ) : null
      }
      {
        isLinkIcon && currentLocale === data.local ? (
          <LinkIcon to={url || href}>{children}</LinkIcon>
        ) : null
      }
      {
        isLinkBtnMore && currentLocale === data.local ? (
          <LinkBtn to={url || href}>{children}</LinkBtn>
        ) : null
      }
    </>
  );
}
