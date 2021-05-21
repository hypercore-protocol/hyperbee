const tape = require('tape')
const { create, collect } = require('./helpers')

const Hyperbee = require('..')

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

tape('createReadStream with live:true', async function (t) {
  const db = create({ keyEncoding: 'utf8' })

  const b = db.batch()

  await b.put('a', null)
  await b.put('b', null)
  await b.put('c', null)

  await b.flush()

  let data = ''
  const s = db.createReadStream({ gte: 'a', live: true })
  s.on('data', function ({ key }) {
    data += key
  })

  setTimeout(async () => {
    await db.put('d', null)
  }, 200)

  return new Promise(resolve => {
    s.on('end', function () {
      t.same(data, 'abcd', 'stream should stay open')
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

tape('test all short iterators, sub database', async function (t) {
  const parent = create({ keyEncoding: 'utf8' })
  const db = parent.sub('sub1')

  const MAX = 25

  for (let size = 1; size <= MAX; size++) {
    const reference = []
    for (let i = 0; i < size; i++) {
      const key = '' + i
      await db.put(key, 'hello world')
      await parent.put(key, 'parent hello world')
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

tape('simple sub put/get', async t => {
  const db = create()
  const sub = db.sub('hello')
  await sub.put('world', 'hello world')
  const node = await sub.get('world')
  t.same(node && node.key, 'world')
  t.same(node && node.value, 'hello world')
  t.end()
})

tape('multiple levels of sub', async t => {
  const db = create({ sep: '!' })
  const sub = db.sub('hello').sub('world')
  await sub.put('a', 'b')

  const encoded = sub.keyEncoding.encode('a')

  {
    const node = await sub.get('a')
    t.same(node && node.key, 'a')
    t.same(node && node.value, 'b')
  }

  {
    const node = await db.get(encoded)
    t.same(node && node.key, encoded.toString('utf-8'))
    t.same(node && node.value, 'b')
  }

  {
    const key = 'hello' + db.sep + 'world' + db.sep + 'a'
    t.same(key, encoded.toString('utf-8'))
    const node = await db.get(key)
    t.same(node && node.key, key)
    t.same(node && node.value, 'b')
  }

  t.end()
})

tape('multiple levels of sub, entries outside sub', async t => {
  const db = create({ sep: '!' })
  const helloSub = db.sub('hello')
  const worldSub = helloSub.sub('world')
  await helloSub.put('a', 'b')
  await worldSub.put('b', 'c')

  const expected = [['b', 'c']]
  for await (const { key, value } of worldSub.createReadStream()) {
    const next = expected.shift()
    if (!next) {
      t.fail('iterated unexpected value')
      break
    }
    t.same(key, next[0])
    t.same(value, next[1])
  }
  t.same(expected.length, 0)

  t.end()
})

tape('sub respects keyEncoding', async t => {
  t.plan(2)

  const db = create({ sep: '!' })
  const helloSub = db.sub('hello', {
    keyEncoding: {
      encode (key) {
        return Buffer.from(key.key)
      },
      decode (buf) {
        return { key: buf.toString() }
      }
    }
  })

  await helloSub.put({ key: 'hello' }, 'val')

  for await (const data of helloSub.createReadStream()) {
    t.same(data.key, { key: 'hello' })
  }

  const node = await helloSub.get({ key: 'hello' })

  t.ok(node)
  t.end()
})

tape('sub with a key that starts with 0xff', async t => {
  t.plan(2)

  const db = create({ sep: '!', keyEncoding: 'binary' })
  const helloSub = db.sub('hello')
  const key = Buffer.from([0xff, 0x01, 0x02])

  await helloSub.put(key, 'val')

  for await (const data of helloSub.createReadStream()) {
    t.same(data.key, key)
  }

  const node = await helloSub.get(key)

  t.ok(node)
  t.end()
})

tape('setting read-only flag to false disables header write', async t => {
  const db = create({ readonly: true })
  await db.ready()
  t.same(db.feed.length, 0)
  t.true(db.readonly)
  t.end()
})

tape('cannot append to read-only db', async t => {
  const db = create({ readonly: true })
  await db.ready()
  try {
    await db.append('hello', 'world')
  } catch (err) {
    t.true(err)
  }
  t.end()
})

tape('feed is unwrapped in getter', async t => {
  const feed = require('hypercore')(require('random-access-memory'))
  const db = new Hyperbee(feed)
  await db.ready()
  t.same(feed, db.feed)
  t.end()
})
