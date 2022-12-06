import React from 'react';
import ComponentTypes from '@theme-original/NavbarItem/ComponentTypes';
import {Github} from '@site/src/components';


 export default {
  ...ComponentTypes,
  'custom-github': () => (
    <Github className="hideInMobile">
      <Github.Star />
    </Github>
  ),
};

