(function(util) {
  function crc32(parts, length) {
    let crc = 0xffffffff;
    let remaining = length;
    for (const part of parts) {
      for (let i = 0; i < part.length && remaining > 0; i++, remaining--) {
        crc ^= part[i];
        for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0);
      }
      if (remaining === 0) break;
    }
    return (crc ^ 0xffffffff) >>> 0;
  }

  function readU32(parts, offset) {
    return (((byteAt(parts, offset) * 0x1000000) + (byteAt(parts, offset + 1) << 16) + (byteAt(parts, offset + 2) << 8) + byteAt(parts, offset + 3)) >>> 0);
  }

  function byteAt(parts, offset) {
    for (const part of parts) {
      if (offset < part.length) return part[offset];
      offset -= part.length;
    }
    throw new Error("truncated EventStream frame");
  }

  function copyRange(parts, offset, length) {
    const out = new Uint8Array(length);
    let target = 0;
    for (const part of parts) {
      if (offset >= part.length) {
        offset -= part.length;
        continue;
      }
      const count = Math.min(part.length - offset, length - target);
      out.set(part.subarray(offset, offset + count), target);
      target += count;
      offset = 0;
      if (target === length) break;
    }
    if (target !== length) throw new Error("truncated EventStream frame");
    return out;
  }

  function decodeUTF8(bytes) {
    let text = "";
    for (let i = 0; i < bytes.length;) {
      const first = bytes[i++];
      let point;
      let count;
      if (first < 0x80) { point = first; count = 0; }
      else if (first >= 0xc2 && first <= 0xdf) { point = first & 0x1f; count = 1; }
      else if (first >= 0xe0 && first <= 0xef) { point = first & 0x0f; count = 2; }
      else if (first >= 0xf0 && first <= 0xf4) { point = first & 0x07; count = 3; }
      else throw new Error("invalid UTF-8");
      if (i + count > bytes.length) throw new Error("invalid UTF-8");
      for (let j = 0; j < count; j++) {
        const next = bytes[i++];
        if ((next & 0xc0) !== 0x80 || (j === 0 && ((first === 0xe0 && next < 0xa0) || (first === 0xed && next >= 0xa0) || (first === 0xf0 && next < 0x90) || (first === 0xf4 && next >= 0x90)))) throw new Error("invalid UTF-8");
        point = (point << 6) | (next & 0x3f);
      }
      if (count === 1 && point < 0x80 || count === 2 && point < 0x800 || count === 3 && point < 0x10000 || point > 0x10ffff) throw new Error("invalid UTF-8");
      if (point <= 0xffff) text += String.fromCharCode(point);
      else {
        point -= 0x10000;
        text += String.fromCharCode(0xd800 + (point >> 10), 0xdc00 + (point & 0x3ff));
      }
    }
    return text;
  }

  function decodeHeaders(bytes) {
    const headers = Object.create(null);
    let pos = 0;
    while (pos < bytes.length) {
      const nameLength = bytes[pos++];
      if (nameLength === 0 || pos + nameLength + 1 > bytes.length) throw new Error("invalid EventStream header name length");
      const name = decodeUTF8(bytes.subarray(pos, pos + nameLength));
      pos += nameLength;
      const type = bytes[pos++];
      let value;
      switch (type) {
        case 0: value = false; break;
        case 1: value = true; break;
        case 2:
          if (pos + 1 > bytes.length) throw new Error("truncated EventStream int8");
          value = new DataView(bytes.buffer, bytes.byteOffset + pos, 1).getInt8(0); pos += 1; break;
        case 3:
          if (pos + 2 > bytes.length) throw new Error("truncated EventStream int16");
          value = new DataView(bytes.buffer, bytes.byteOffset + pos, 2).getInt16(0, false); pos += 2; break;
        case 4:
          if (pos + 4 > bytes.length) throw new Error("truncated EventStream int32");
          value = new DataView(bytes.buffer, bytes.byteOffset + pos, 4).getInt32(0, false); pos += 4; break;
        case 5:
          if (pos + 8 > bytes.length) throw new Error("truncated EventStream int64");
          value = util.i64be(bytes.subarray(pos, pos + 8), 0); pos += 8; break;
        case 6:
        case 7: {
          if (pos + 2 > bytes.length) throw new Error("truncated EventStream value length");
          const length = (bytes[pos] << 8) | bytes[pos + 1]; pos += 2;
          if (length === 0 || length > 32767 || pos + length > bytes.length) throw new Error("invalid EventStream value length");
          const data = bytes.subarray(pos, pos + length); pos += length;
          value = type === 6 ? data.slice().buffer : decodeUTF8(data);
          break;
        }
        case 8:
          if (pos + 8 > bytes.length) throw new Error("truncated EventStream timestamp");
          value = util.u64be(bytes.subarray(pos, pos + 8), 0); pos += 8; break;
        case 9:
          if (pos + 16 > bytes.length) throw new Error("truncated EventStream uuid");
          value = bytes.slice(pos, pos + 16).buffer; pos += 16; break;
        default: throw new Error("unknown EventStream header value type " + type);
      }
      if (Object.prototype.hasOwnProperty.call(headers, name)) throw new Error("duplicate EventStream header " + name);
      headers[name] = value;
    }
    return headers;
  }

  Object.defineProperty(util, "eventstream", {
    value: Object.freeze({
      decode(input) {
        let started = false;
        let stopped = false;
        const source = input[Symbol.iterator]();
        const decoder = (function* () {
          let segments = [];
          let available = 0;
          let completed = false;
          let hasPrimaryError = false;
          try {
          while (true) {
            const step = source.next();
            if (step.done) break;
            const item = step.value;
            const chunk = item instanceof ArrayBuffer ? new Uint8Array(item)
              : ArrayBuffer.isView(item) ? new Uint8Array(item.buffer, item.byteOffset, item.byteLength)
              : null;
            if (!chunk) throw new Error("EventStream chunk must be an ArrayBuffer or typed array view");
            if (chunk.length) {
              segments.push(chunk);
              available += chunk.length;
            }
            while (available >= 12) {
              const totalLength = readU32(segments, 0);
              const headersLength = readU32(segments, 4);
              if (crc32(segments, 8) !== readU32(segments, 8)) throw new Error("EventStream prelude CRC mismatch");
              if (totalLength < 16 || headersLength > totalLength - 16) throw new Error("invalid EventStream frame length");
              if (available < totalLength) break;
              if (crc32(segments, totalLength - 4) !== readU32(segments, totalLength - 4)) throw new Error("EventStream message CRC mismatch");
              const headers = decodeHeaders(copyRange(segments, 12, headersLength));
              const payloadLength = totalLength - 16 - headersLength;
              const payload = copyRange(segments, 12 + headersLength, payloadLength).buffer;
              let consumed = totalLength;
              if (consumed === available) {
                segments = [];
              } else {
                const rest = [];
                let skip = consumed;
                for (const part of segments) {
                  if (skip >= part.length) {
                    skip -= part.length;
                    continue;
                  }
                  rest.push(part.subarray(skip));
                  skip = 0;
                }
                segments = rest;
              }
              available -= consumed;
              yield { headers, payload };
            }
          }
          if (available !== 0) throw new Error("truncated EventStream frame");
          completed = true;
        } catch (err) {
          hasPrimaryError = true;
          throw err;
        } finally {
          if (!completed && !stopped && source && typeof source.return === "function") {
            stopped = true;
            if (hasPrimaryError) {
              try { source.return(); } catch (_) {}
            } else {
              source.return();
            }
          }
        }
        })();
        const iterator = {
          next(value) {
            if (stopped) return { done: true };
            started = true;
            return decoder.next(value);
          },
          return(value) {
            if (stopped) return { done: true };
            if (!started) {
              stopped = true;
              if (source && typeof source.return === "function") return source.return();
              return { done: true, value };
            }
            return decoder.return(value);
          },
          throw(error) {
            if (stopped) throw error;
            if (!started) {
              if (source && typeof source.return === "function") {
                stopped = true;
                try { source.return(); } catch (_) {}
              }
              throw error;
            }
            started = true;
            return decoder.throw(error);
          },
          [Symbol.iterator]() { return this; }
        };
        return iterator;
      }
    }),
    writable: false,
    configurable: false,
    enumerable: true
});
})(this.util);
