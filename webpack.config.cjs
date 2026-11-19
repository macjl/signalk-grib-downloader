const path = require('node:path')
const { container: { ModuleFederationPlugin } } = require('webpack')

module.exports = {
  mode: 'production',
  entry: {},
  output: {
    path: path.join(__dirname, 'public'),
    publicPath: 'auto',
    chunkFilename: '[name].js',
  },
  module: {
    rules: [{
      test: /\.jsx$/,
      use: { loader: 'babel-loader', options: { presets: ['@babel/preset-react'] } },
    }],
  },
  plugins: [new ModuleFederationPlugin({
    name: 'signalk_grib_downloader',
    library: { type: 'var', name: 'signalk_grib_downloader' },
    filename: 'remoteEntry.js',
    exposes: {
      './PluginConfigurationPanel': { import: './src/config-panel.jsx', name: 'grib-downloader-config' },
    },
    shared: { react: { import: false, singleton: true, requiredVersion: false } },
  })],
}
