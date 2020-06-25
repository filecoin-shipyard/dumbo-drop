const limiter = concurrency => {
  const pending = new Set()
  const ret = async promise => {
    const p = (async () => { await promise })()
    p.then(() => pending.delete(p))
    pending.add(p)
    while (pending.size >= concurrency) {
      await Promise.race(Array.from(pending))
    }
  }
  ret.wait = () => Promise.all(pending)
  return ret
}

module.exports = limiter
