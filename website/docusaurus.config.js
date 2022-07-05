// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion
const modifyVars = require('./scripts/config.theme.js');
// const tableRemark = require('./src/remark/table');
// const ulRemark = require('./src/remark/ul');
// const math = require('./src/remark/remark-math');
// const imageList = require('./src/remark/imageList');

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
          path: 'docs',
          id: 'default',
          routeBasePath: 'docs',
          sidebarPath: require.resolve('./sidebars.js'),
          showLastUpdateAuthor: true,
          showLastUpdateTime: true,
          editLocalizedFiles: true,
          editUrl: ({versionDocsDirPath, docPath, locale}) => {
            if(locale !== 'en') {
              return `https://github.com/stoneatom/stonedb-docs/edit/main/i18n/${locale}/docusaurus-plugin-content-docs/current/${docPath}`
            }
            return `https://github.com/stoneatom/stonedb-docs/edit/main/${versionDocsDirPath}/${docPath}`
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
            to: '/community/main',
            label: 'Community',
            key: 'Community',
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
      '@docusaurus/plugin-content-blog',
      {
        path: 'community',
        id: 'community',
        routeBasePath: 'community',
        blogSidebarCount: 0,
      }
    ],
    [
      'pwa',
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
};

module.exports = config;
