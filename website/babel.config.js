module.exports = {
  presets: [require.resolve('@docusaurus/core/lib/babel/preset')],
  plugins: [
    "babel-plugin-styled-components",
    ["import", { "libraryName": "antd", "libraryDirectory": "es", "style": true }],
  ]
};
