import React from 'react';
import Layout from '@theme/Layout';
import type {Props} from '@theme/BlogLayout';
import {Nav, Container} from './styles';

export default function BlogLayout(props: Props): JSX.Element {
  const {sidebar, toc, children, ...layoutProps} = props;
  return (
    <Layout {...layoutProps} title="Community">
      <div className="container margin-vert--lg">
      <div className="row communityRow">
        <main
          itemScope
          className='col--9 col--offset-1'
          itemType="http://schema.org/Blog">
          {children}
        </main>
        {toc && <div className="col col--2">{toc}</div>}
      </div>
      </div>
    </Layout>
  );
}
