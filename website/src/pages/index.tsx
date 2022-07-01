import React, {useEffect, useState} from 'react';
import Layout from '@theme/Layout'
import BrowserOnly from '@docusaurus/BrowserOnly';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Download from "@site/src/components/download";
import Feature from '@site/src/components/feature';
import Concat from '@site/src/components/contact';
import Panel from './styles/panel';
import { Tooltip } from 'antd';

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  
  return (
    <Layout title={`${siteConfig.title} - A Real-time HTAP Database`}>
      <BrowserOnly>
        {
          () => (
            <>
              <Panel>
                <Download />
              </Panel>
              <Panel color='#F2F3F7;'>
                <Feature />
              </Panel>
              <Panel >
                <Concat  />
              </Panel>
            </>
          )
        }
      </BrowserOnly>
    </Layout>
  );
}
