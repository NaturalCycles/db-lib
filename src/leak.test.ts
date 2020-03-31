test('should not leak memory', () => {
  require('.')
  require('./validation')
})
