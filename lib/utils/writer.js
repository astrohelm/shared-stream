'use strict';

const { READ_INDEX, WRITE_INDEX, READ_CYCLE, WRITE_CYCLE } = require('./config');
const assert = require('node:assert');

const BYTE_SIZE = 4;
const EXTRA_BYTES = 2;
const SHARED_STATE_SIZE = 32;

module.exports = class Writable extends require('node:events') {
  _sharedBuffer = null;
  _sharedState = null;
  _extraBuffer = [];
  _cycle = 0;
  _write = 0;

  _ready = false;
  _ended = false;
  _closed = false;
  _ending = false;
  _errored = false;
  _destroyed = false;
  _flashing = false;
  _finished = false;
  _needDrain = false;

  constructor({ sharedState, sharedBuffer, maxMessageSize }) {
    assert(sharedState && sharedState.byteLength >= SHARED_STATE_SIZE);
    assert(maxMessageSize);
    assert(sharedBuffer);
    super();

    this._sharedState = new Int32Array(sharedState);
    this._sharedBuffer = Buffer.from(sharedBuffer);
    this.writeSync = Write.bind(this, true);
    this.write = Write.bind(this, false);
  }

  store(data, isFinal) {
    //! Data + EXTRA_BYTES <= this._sharedBuffer.byteLength
    const written = this._buffer.write(data, this._write + BYTE_SIZE);
    this._buffer.writeInt32LE(isFinal, this._write + written + BYTE_SIZE);
    this._buffer.writeInt32LE(written, this._write);
    this._write += BYTE_SIZE + written + BYTE_SIZE;
    Atomics.store(this._sharedState, WRITE_INDEX, this._write);
    Atomics.notify(this._sharedState, WRITE_INDEX);
  }

  end() {}
  destroy() {
    this._destroyed = true;
  }

  get writable() {
    return !this._destroyed && !this._ending;
  }

  get writableEnded() {
    return this._ending;
  }

  get writableFinished() {
    return this._finished;
  }

  get writableErrored() {
    return this._errored;
  }

  get ready() {
    return this._ready;
  }

  get closed() {
    return this._closed;
  }

  get writableNeedDrain() {
    return this._needDrain;
  }

  get writableObjectMode() {
    return false;
  }
};

function Write(sync, toWrite) {
  const read = Atomics.load(this._sharedBuffer, READ_INDEX);
  const cycle = Atomics.load(this._sharedBuffer, READ_CYCLE);
  const isBehind = read > this._write || cycle < this._cycle;
  let leftover = (isBehind ? read : this._sharedBuffer.length) - this._write;
  if (leftover < 0) throw new Error('overwritten'); //? Should never happen
  if (cycle > this._cycle) throw new Error('Read further than expected');
  leftover -= EXTRA_BYTES;

  if (leftover <= 0 && isBehind) {
    var { async, value } = Atomics.waitAsync(this._sharedState, READ_INDEX, read);
    if (!async) return Write.call(this, sync, toWrite);
    if (sync) return wait(this._sharedState, read), Write.call(this, sync, toWrite);
    value.then(() => {
      if (!this.flush()) return;
      this.write = Write.bind(this, sync);
      this._needDrain = false;
      this.emit('drain');
    });

    this.write = FakeWrite;
    this._needDrain = true;
    return true;
  }

  if (leftover <= 0 && !isBehind) {
    this._write = 0;
    Atomics.store(this._sharedState, WRITE_INDEX, this._write);
    Atomics.store(this._sharedState, WRITE_CYCLE, ++this.cycle);
    Atomics.notify(this._sharedState, WRITE_INDEX);
    return Write.call(this, sync, toWrite, true);
  }

  if (leftover < Buffer.byteLength(toWrite) + EXTRA_BYTES) {
    this.store(toWrite.slice(0, leftover), 0);
    return Write.call(this, sync, toWrite.slice(leftover));
  }

  this.store(toWrite);
  return false;
}

function FakeWrite(data) {
  this._extraBuffer.push(data);
}

const MAX_SPINS = 10;
const WAIT_TIMEOUT = 1000;
const ERROR_TOO_LONG = 'Wait took too long (10s)';
function wait(state, origin) {
  let spins = 0;
  do {
    var status = Atomics.wait(state, READ_INDEX, origin, WAIT_TIMEOUT);
    var read = Atomics.load(state, READ_INDEX);
    if (++spins === MAX_SPINS) throw new Error(ERROR_TOO_LONG);
  } while (status !== 'ok' || origin !== read);
}
