
const fs = require('fs');
const path = require('path');
const lessToJs = require('less-vars-to-js');
const webpack = require('webpack');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const themes = require('./config.theme.js');
const srcPath = path.join(__dirname, '..', 'src', 'css');
const stylePath = path.join(__dirname, '..', 'node_modules', 'antd', 'lib', 'style');
const colorLess = fs.readFileSync(path.join(stylePath, 'color', 'colors.less'), 'utf8');
const defaultLess = fs.readFileSync(path.join(stylePath, 'themes', 'default.less'), 'utf8');

const allPaletteLess = lessToJs(`${colorLess}${defaultLess}`, {
  stripPrefix: true,
  resolveVariables: false,
});

const defaultPaletteLess = lessToJs(`${defaultLess}`, {
  stripPrefix: true,
  resolveVariables: false,
});

const lessFinaly = Object.assign(allPaletteLess, themes);

let less = '';
for (let key in lessFinaly) {
  const value = lessFinaly[key];
  // if (value.indexOf('@') === 0) {
  //   value = `var(--safe-${value.replace('@', '')})`;
  // } else if (value.indexOf('colorPalette') >= 0) {
  //   value = value.replace('@{', 'var(--safe-').replace('}', ')');
  // }
  // if(key === 'alert-info-bg-color') {
  //   console.log(key, value);
  // }
  less += `@${key}: ${value};\n`
}
fs.writeFileSync('scripts/var.less', `@import '~antd/lib/style/color/colorPalette.less';\n${less}`);

const themeFinaly = Object.assign(defaultPaletteLess, themes);

let theme = '';
for (let key in themeFinaly) {
  theme += `--safe-${key}: @${key};\n`
}

fs.writeFileSync('scripts/theme.less', `@import './var.less';\n :root{\n${theme}\n}`);


module.exports = webpack({
  "mode": 'development',
  "output": {
    "path": srcPath,
    "filename": "[name].js",
    'library': 'safeTheme'
  },
  "entry": {
    "theme": ['./scripts/styles.jsx']
  },
  "module": {
    "rules": [
    {
      test: /\.less$/,
      exclude: /node_modules/,
      use: [
        MiniCssExtractPlugin.loader,
        {
        loader: 'css-loader',
      }, {
        loader: 'less-loader',
        options: {
          lessOptions: {
            javascriptEnabled: true,
          }
        }
      }]
    }
    ]
  },
  plugins: [
    new MiniCssExtractPlugin({
      filename: '[name].css',
    }),

  ],
}, async (err, stats) => {
  if (err || stats.hasErrors()) {
    console.log('构建过程出错', stats.compilation.errors)
  }
  fs.rm('src/css/theme.js', {force: true}, (err) => {});
});

