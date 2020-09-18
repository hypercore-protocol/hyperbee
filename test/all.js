const tape = require('tape')
const { create, collect } = require('./helpers')

tape('out of bounds iterator', async function (t) {
  const db = create()

  const b = db.batch()

  await b.put('a', null)
  await b.put('b', null)
  await b.put('c', null)

  await b.flush()

  const s = db.createReadStream({ gt: Buffer.from('c') })
  let count = 0

  s.on('data', function (data) {
    count++
  })

  return new Promise(resolve => {
    s.on('end', function () {
      t.same(count, 0, 'no out of bounds reads')
      resolve()
    })
  })
})

tape('createHistoryStream reverse', async function (t) {
  const db = create()

  const b = db.batch()

  await b.put('a', null)
  await b.put('b', null)
  await b.put('c', null)

  await b.flush()

  const s = db.createHistoryStream({ reverse: true })

  let res = ''
  s.on('data', function (data) {
    const { key } = data
    res += key.toString()
  })

  return new Promise(resolve => {
    s.on('end', function () {
      t.same(res, 'cba', 'reversed correctly')
      resolve()
    })
  })
})

tape('out of bounds iterator, string encoding', async function (t) {
  const db = create({ keyEncoding: 'utf8' })

  const b = db.batch()

  await b.put('a', null)
  await b.put('b', null)
  await b.put('c', null)

  await b.flush()

  const s = db.createReadStream({ gte: 'f' })
  let count = 0

  s.on('data', function (data) {
    count++
  })

  return new Promise(resolve => {
    s.on('end', function () {
      t.same(count, 0, 'no out of bounds reads')
      resolve()
    })
  })
})

tape('out of bounds iterator, larger db', async function (t) {
  const db = create({ keyEncoding: 'utf8' })

  for (let i = 0; i < 8; i++) {
    await db.put('' + i, 'hello world')
  }

  const s = db.createReadStream({ gte: 'a' })
  let count = 0

  s.on('data', function (data) {
    count++
  })

  return new Promise(resolve => {
    s.on('end', function () {
      t.same(count, 0, 'no out of bounds reads')
      resolve()
    })
  })
})

tape('test all short iterators', async function (t) {
  const db = create({ keyEncoding: 'utf8' })

  const MAX = 25

  for (let size = 1; size <= MAX; size++) {
    const reference = []
    for (let i = 0; i < size; i++) {
      const key = '' + i
      await db.put(key, 'hello world')
      reference.push(key)
    }
    reference.sort()

    for (let i = 0; i < size; i++) {
      for (let j = 0; j <= i; j++) {
        for (let k = 0; k < 8; k++) {
          const greater = (k & 1) ? 'gte' : 'gt'
          const lesser = (k >> 1 & 1) ? 'lte' : 'lt'
          const reverse = !!(k >> 2 & 1)
          const opts = {
            [greater]: '' + j,
            [lesser]: '' + i,
            reverse
          }
          const entries = await collect(db.createReadStream(opts))
          if (!validate(size, reference, opts, entries)) {
            return t.end()
          }
        }
      }
    }
  }

  t.pass('all iterations passed')
  t.end()

  function validate (size, reference, opts, entries) {
    const start = opts.gt ? reference.indexOf(opts.gt) + 1 : reference.indexOf(opts.gte)
    const end = opts.lt ? reference.indexOf(opts.lt) : reference.indexOf(opts.lte) + 1
    const range = reference.slice(start, end)
    if (opts.reverse) range.reverse()
    for (let i = 0; i < range.length; i++) {
      if (!entries[i] || range[i] !== entries[i].key) {
        console.log('========')
        console.log('SIZE:', size)
        console.log('FAILED WITH OPTS:', opts)
        console.log('  expected:', range, 'start:', start, 'end:', end)
        console.log('  actual:', entries.map(e => e.key))
        t.fail('ranges did not match')
        return false
      }
    }
    return true
  }
})

tape('can create block pointers with the link flag', async function (t) {
  const db = create({
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })

  await db.put('hello', 'world')
  await db.put('pointer', 'hello', { link: true })

  const node = await db.get('pointer')
  t.same(node.key, 'pointer')
  t.same(node.value, 'world')

  t.end()
})

tape('getting a pointer with the noResolve flag returns the pointer block', async function (t) {
  const db = create({
    keyEncoding: 'utf-8',
    valueEncoding: 'utf-8'
  })

  await db.put('hello', 'world')
  await db.put('pointer', 'hello', { link: true })

  const node = await db.get('pointer', { noResolve: true })
  t.same(node.key, 'pointer')
  t.same(node.value, 'hello')

  t.end()
})
