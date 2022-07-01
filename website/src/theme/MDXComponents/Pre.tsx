/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, {isValidElement, useEffect, useState} from 'react';
import CodeBlock from '@theme/CodeBlock';
import Team from '@site/src/components/team';
import Join from '@site/src/components/join';
import Events from '@site/src/components/events';
import Subscribe from '@site/src/components/subscribe';
import DocLinks from '@site/src/components/docLinks';

const CustomComponentMap = {
  'language-custom-eventList': Events,
  'language-custom-subscribe': Subscribe,
  'language-custom-joinList': Join,
  'language-custom-teamList': Team,
  'language-custom-docLinksList': DocLinks,
}

export default function MDXPre(props: any): JSX.Element {
  const {children, className, originalType, metastring} = props.children.props;
  const isOrigin = className ? className.indexOf('custom') < 0 : true;
  const CustomComponent = CustomComponentMap[className];
  return (
    <>
      {
        isOrigin ? (
          <CodeBlock
            {...(isValidElement(props.children) &&
            originalType === 'code'
              ? props.children?.props
              : {...props})}
          />
        ) : (
          <CustomComponent type={metastring}>
            {
              children
            }
          </CustomComponent>
        )
      }
    </>
  );
}
