/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from 'react';
import clsx from 'clsx';
import {
  PageMetadata,
  HtmlClassNameProvider,
  ThemeClassNames,
} from '@docusaurus/theme-common';
import Layout from '@theme/Layout';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import MDXContent from '@theme/MDXContent';
import TOC from '@theme/TOC';
import type {Props} from '@theme/MDXPage';
import styles from './styles.module.css';
import {useLocation} from '@docusaurus/router';

export default function MDXPage(props: Props): JSX.Element {
  const {siteConfig} = useDocusaurusContext();
  const {content: MDXPageContent} = props;
  const {
    metadata: {title, description, frontMatter},
  } = MDXPageContent;
  const {wrapperClassName, hide_table_of_contents: hideTableOfContents} =
    frontMatter;

    const location = useLocation();

  const isHome = location.pathname === '/' || location.pathname === '/zh/';


  return (
    <HtmlClassNameProvider
      className={clsx(
        wrapperClassName ?? ThemeClassNames.wrapper.mdxPages,
        ThemeClassNames.page.mdxPage,
      )}>
      <PageMetadata title={title} description={description} />
      {
        isHome ? (
          <Layout title={`${siteConfig.title} - A Real-time HTAP Database`}>
            <MDXContent>
              <MDXPageContent />
            </MDXContent>
          </Layout>
        ) : (
          <Layout>
            <main className="container container--fluid margin-vert--lg">
              <div className={clsx('row', styles.mdxPageWrapper)}>
                <div className={clsx('col', !hideTableOfContents && 'col--8')}>
                  <article>
                    <MDXContent>
                      <MDXPageContent />
                    </MDXContent>
                  </article>
                </div>
                {!hideTableOfContents && MDXPageContent.toc.length > 0 && (
                  <div className="col col--2">
                    <TOC
                      toc={MDXPageContent.toc}
                      minHeadingLevel={frontMatter.toc_min_heading_level}
                      maxHeadingLevel={frontMatter.toc_max_heading_level}
                    />
                  </div>
                )}
              </div>
            </main>
          </Layout>
        )
      }
      
    </HtmlClassNameProvider>
  );
}
