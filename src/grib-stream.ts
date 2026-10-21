// Check GRIB2 message framing without buffering grids or decoding fields.
// In particular, a clean HTTP EOF must not turn a truncated GRIB into a dataset.
export class GribStreamValidator {
  private header = Buffer.alloc(0)
  private remaining = 0
  private tail = Buffer.alloc(0)
  private messages = 0

  push(chunk: Buffer): void {
    let offset = 0
    while (offset < chunk.length) {
      if (this.remaining === 0) {
        const n = Math.min(16 - this.header.length, chunk.length - offset)
        this.header = Buffer.concat([this.header, chunk.subarray(offset, offset + n)])
        offset += n
        if (this.header.length < 16) continue
        if (this.header.toString('ascii', 0, 4) !== 'GRIB' || this.header[7] !== 2) throw new Error('Upstream content is not GRIB2')
        const length = this.header.readBigUInt64BE(8)
        if (length < 20n || length > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error('Invalid GRIB2 message length')
        this.remaining = Number(length) - 16
        this.header = Buffer.alloc(0)
        this.tail = Buffer.alloc(0)
      }
      const n = Math.min(this.remaining, chunk.length - offset)
      const segment = chunk.subarray(offset, offset + n)
      this.tail = segment.length >= 4 ? Buffer.from(segment.subarray(-4)) : Buffer.concat([this.tail, segment]).subarray(-4)
      offset += n
      this.remaining -= n
      if (this.remaining === 0) {
        if (this.tail.toString('ascii') !== '7777') throw new Error('Invalid GRIB2 message terminator')
        this.messages++
      }
    }
  }

  finish(): void {
    if (!this.messages || this.remaining || this.header.length) throw new Error('Empty or truncated upstream GRIB2 stream')
  }
}
