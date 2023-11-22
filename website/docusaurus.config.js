// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion
const modifyVars = require('./scripts/config.theme.js');
// const tableRemark = require('./src/remark/table');
// const ulRemark = require('./src/remark/ul');
// const math = require('./src/remark/remark-math');
// const imageList = require('./src/remark/imageList');
const path = require('path');
const isDev = process.env.NODE_ENV === 'development';

const isDeployPreview =
  !!process.env.NETLIFY && process.env.CONTEXT === 'deploy-preview';

const config = {
  title: 'StoneDB',
  tagline: 'StoneDB',
  url: 'https://stonedb.io',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'StoneAtom', // Usually your GitHub org/user name.
  projectName: 'stonedb-docs', // Usually your repo name.

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en', 'zh'],
    localeConfigs: {
      en: { 
        label: 'English',
        htmlLang: 'en',
        direction: 'ltr',
      },
      zh: { 
        label: '简体中文',
        htmlLang: 'zh',
        direction: 'ltr',
      },
    },
  },
  staticDirectories: [
    'static',
  ],
  presets: [
    [
      '@docusaurus/preset-classic',
      ({
        docs: {
          path: '../Docs',
          id: 'default',
          routeBasePath: 'docs',
          sidebarPath: require.resolve('./sidebars.js'),
          showLastUpdateAuthor: true,
          showLastUpdateTime: true,
          editLocalizedFiles: true,
          editUrl: ({docPath, locale}) => {
            if(locale !== 'en') {
              return `https://github.com/stoneatom/stonedb/edit/stonedb-5.7-dev/website/i18n/${locale}/docusaurus-plugin-content-docs/current/${docPath}`
            }
            return `https://github.com/stoneatom/stonedb/edit/stonedb-5.7-dev/Docs/${docPath}`
          },
        },
        blog: {
          // routeBasePath: '/',
          path: 'blog',
          // editUrl: ({locale, blogDirPath, blogPath}) => {
          //   if(locale !== 'en') {
          //     return `https://github.com/stoneatom/stonedb/edit/stonedb-5.7-dev/website/i18n/${locale}/docusaurus-plugin-content-blog/current/${docPath}`
          //   }
          //   return `https://github.com/stoneatom/stonedb/edit/stonedb-5.7-dev/website/blog/${docPath}`
          // },
          postsPerPage: 5,
          feedOptions: {
            type: 'all',
            copyright: `Copyright © The StoneAtom-tech ALL Rights Reserved`,
          },
        },
        theme: {
          customCss: require.resolve('./src/css/custom.less'),
        },
      }),
    ],
  ],
  themeConfig:
    ({
      colorMode: {
        defaultMode: 'light',
        disableSwitch: true,
      },
      navbar: {
        title: 'Home',
        logo: {
          alt: 'StoneDB',
          src: 'img/logo_stonedb.svg',
          href: '/',
          target: '_self',
        },
        hideOnScroll: true,
        items: [
          {
            to: '/docs/download', 
            label: 'Download',
            key: 'Download'
          },
          {
            label: 'Docs',
            to: '/docs/about-stonedb/intro',
            key: 'Docs',
          },
          {
            label: 'Discussion',
            to: 'https://github.com/stoneatom/stonedb/discussions',
            key: 'Discussion',
          },
          {
            label: 'Community',
            to: '/community',
            key: 'Community',
          },
          {
            label: 'Blog',
            to: '/blog',
            key: 'Blog',
          },
          {
            label: 'Forum',
            to: 'https://forum.stonedb.io',
            key: 'Forum',
          },
          
          {
            type: 'custom-github',
            position: 'right'
          },{
            type: 'localeDropdown',
            position: 'right',
          }
        ],
      },
      footer: {
        copyright: `Copyright © The StoneAtom-tech ALL Rights Reserved`,
        links: [
          {
            label: '@StoneDB2022',
            href: 'https://twitter.com/StoneDataBase',
            icon: 'icon-a-bianzu2'
          },
          {
            label: 'dev@stonedb.io',
            href: '#',
            icon: 'icon-a-bianzu17beifen2',
            onclick: 'subscribe'
          },
          {
            label: ' ',
            href: 'https://stonedb.slack.com/join/shared_invite/zt-1ba2lpvbo-Vqq62DJcxViyxCZmp7Rimw#/shared-invite/email',
            icon: 'icon-a-bianzu18beifen2'
          },
          {
            label: 'StoneAtom/stonedb',
            href: 'https://github.com/StoneAtom/stonedb',
            icon: 'icon-a-bianzu4',
          }
        ]
      },
      prism: {
        
      },
    }),
  // scripts: [
  //   'https://unpkg.com/libpag@latest/lib/libpag.min.js',
  // ],
  plugins: [
    // '@docusaurus/plugin-ideal-image',
    [
      "docusaurus-plugin-less", 
      {
        lessOptions: {
          modifyVars,
          javascriptEnabled: true,
        }
      }
    ],
    
    [
      '@docusaurus/plugin-pwa',
      {
        debug: isDeployPreview,
        offlineModeActivationStrategies: [
          'appInstalled',
          'standalone',
          'queryString',
        ],
        // swRegister: false,
        pwaHead: [
          {
            tagName: 'link',
            rel: 'icon',
            href: 'img/stoneDB.png',
          },
          {
            tagName: 'link',
            rel: 'manifest',
            href: 'manifest.json',
          },
          {
            tagName: 'meta',
            name: 'apple-mobile-web-app-capable',
            content: 'yes',
          },
          {
            tagName: 'meta',
            name: 'apple-mobile-web-app-status-bar-style',
            content: '#000',
          },
          {
            tagName: 'link',
            rel: 'apple-touch-icon',
            href: 'img/stoneDB.png',
          },
          {
            tagName: 'link',
            rel: 'mask-icon',
            href: 'img/stoneDB.png',
            color: 'rgb(62, 204, 94)',
          },
          {
            tagName: 'meta',
            name: 'msapplication-TileImage',
            content: 'img/stoneDB.png',
          },
          {
            tagName: 'meta',
            name: 'msapplication-TileColor',
            content: '#000',
          },
        ],
      },
    ],
  ],
  themes: [
    [
      "@easyops-cn/docusaurus-search-local",
      {
        hashed: true,
        language: ["en", "zh"],
        highlightSearchTermsOnTargetPage: true,
        explicitSearchResultPath: true,
      },
    ],
  ],
  scripts:["https://hm.baidu.com/hm.js?7bd24ad933c7b555361696ce01e3e8ff"]
};

module.exports = config;


