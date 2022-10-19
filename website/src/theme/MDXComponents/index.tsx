/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import MDXComponentsOriginal from '@theme-original/MDXComponents';
import type {MDXComponentsObject} from '@theme/MDXComponents';
import MDXImage from './Image';
import MDXA from './A';
import MDXPre from './Pre';

const MDXComponents: MDXComponentsObject = {
  ...MDXComponentsOriginal,
  img: MDXImage,
  a: MDXA,
  pre: MDXPre
};

export default MDXComponents;
