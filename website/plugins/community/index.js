const pluginContentDoc = require('@docusaurus/plugin-content-docs');

async function CommunityPlugin(context, options) {
  const communityPlugin = await pluginContentDoc.default(context, {
    ...options,
  });
  return {
    ...communityPlugin,
    name: 'community',
    docItemComponent: '@theme/CommunityItem',
    getThemePath() {
      return './theme';
    },
  }
}

CommunityPlugin.validateOptions = pluginContentDoc.validateOptions;

module.exports = CommunityPlugin;

