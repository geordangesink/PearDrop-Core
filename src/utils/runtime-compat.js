let fs
let path

if (typeof globalThis.Bare !== 'undefined') {
  fs = require('bare-fs')
  path = require('bare-path')
} else {
  const nodeRequire = eval('require')
  fs = nodeRequire('fs')
  path = nodeRequire('path')
}

module.exports = {
  fs,
  path
}
