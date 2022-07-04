/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useState } from 'react';
import { Tooltip } from 'antd';
import type {Props} from '@theme/Footer/LinkItem';
import {IconFont, EmailModal} from '@site/src/components';
import {Link, LinkItemStyle} from './styles';

export default function FooterLinkItem({item}: Props): JSX.Element {
  const {to, href, label, icon, onclick, ...props} = item;
  return (
    <LinkItemStyle>
      {
        label && label !== ' ' ? (
          <>
            {
              onclick && onclick === 'subscribe' ? (
                <Tooltip title={label}>
                  <EmailModal>
                    <Link to="">
                      <IconFont type={icon as string} />
                    </Link>
                  </EmailModal>
                </Tooltip>
              ) : (
                <Tooltip title={label}>
                  <Link to={href}>
                    <IconFont type={icon as string} />
                  </Link>
                </Tooltip>
              )
            }
          </>
          
        ) : (
          <Link to={href}>
            <IconFont type={icon as string} />
          </Link>
        )
      }
    </LinkItemStyle>
  );
}
