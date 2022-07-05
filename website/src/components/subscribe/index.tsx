import React from 'react';
import BrowserOnly from '@docusaurus/BrowserOnly';
import {SubscribeStyle, Message, TipStyle, Doc, Mail} from './styles';

const Subscribe: React.FC = () => {

  return (
    <BrowserOnly>
      {
        () => (
          <SubscribeStyle>
            <Mail>
              <Message />
              <TipStyle />
            </Mail>
          </SubscribeStyle>
        )
      }
    </BrowserOnly>
  );
};
export default Subscribe;