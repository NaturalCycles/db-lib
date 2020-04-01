test('should not leak memory', () => {
  require('.')
  require('./validation')
  require('./testing')
  require('./adapter/cachedb')
  require('./adapter/noop')
})
