// manages a list of async functions and makes sure that
// no more than <concurrency> are running at once

const limiter = concurrency => {
  const pending = new Set()
  let next
  let nextResolve
  const ret = async promise => {
    const p = (async () => { await promise })()
    p.then(() => pending.delete(p))
    pending.add(p)
    while (pending.size >= concurrency) {
      const r = await Promise.race(Array.from(pending))
      if (next) nextResolve(r)
    }
  }
  ret.wait = () => Promise.all(pending)
  ret.next = () => {
    if (!next) {
      next = new Promise(resolve => {
        nextResolve = resolve
      })
      next.then(() => {
        next = null
        nextResolve = null
      })
    }
    return next
  }
  return ret
}

module.exports = limiter
