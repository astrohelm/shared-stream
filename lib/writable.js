'use strict';

const assert = require('node:assert');
const { READ_INDEX, READ_CYCLE, READ_PROCESS } = require('./config');
const { WRITE_INDEX, WRITE_CYCLE, WRITE_PROCESS } = require('./config');
const { READY_SIGN, EMPTY_SIGN, FAILED_SIGN, FINISHED_SIGN, FINISHING_SIGN } = require('./config');
const { PREFIX_SIZE, POSTFIX_SIZE } = require('./config');

const EXTRA_SPACE = PREFIX_SIZE + POSTFIX_SIZE;
const SHARED_STATE_SIZE = 128;

const READ_SPINS = 10;
const FINISH_SPINS = 10;
const SPIN_TIMEOUT = 1000;
const START_TIMEOUT = 5000;

const TOO_LONG_READING = 'Reading process took too long (' + READ_SPINS + 's)';
const READER_EXIT_BEFORE_SYNC = 'Failed to start - reader failed before synchronization';
const READER_FAILED_TIMEOUT = 'Failed to start - reader timed out (' + START_TIMEOUT + 'ms)';
const READER_EXIT_AT_SYNC = 'Failed to start - reader exited while synchronization';
const FAILED_TO_FINISH_TIMEOUT = 'Failed to finish - timed out (' + FINISH_SPINS + ')s';
const FAILED_TO_FINISH_READER = 'Failed to finish - reader exited with error';
const READER_EXIT_WHILE_WATCH = 'Reader failed while watching';

module.exports = class Writable extends require('node:events') {
  _sharedBuffer = null;
  _sharedState = null;
  _extraBuffer = [];
  _process = EMPTY_SIGN;
  _cycle = 0;
  _write = 0;

  _ready = false;
  _ended = false;
  _closed = false;
  _ending = false;
  _errored = null;
  _destroyed = false;
  _flashing = false;
  _finished = false;
  _needDrain = false;
  _watching = false;

  constructor({ sharedState, sharedBuffer }) {
    assert(sharedState && sharedState.byteLength >= SHARED_STATE_SIZE);
    assert(sharedBuffer);
    super();

    this._sharedState = new Int32Array(sharedState);
    this._sharedBuffer = Buffer.from(sharedBuffer);
    this.writeSync = Write.bind(this, true);
    this.write = Write.bind(this, false);
    // this._synchronize();
  }

  synchronize() {
    this._process = READY_SIGN;
    Atomics.store(this._sharedState, WRITE_PROCESS, this._process);
    const status = Atomics.load(this._sharedState, READ_PROCESS);
    if (status === READY_SIGN) {
      this._ready = true;
      this.emit('ready');
      return;
    }

    if (status !== EMPTY_SIGN) return void this.destroy(new Error(READER_EXIT_BEFORE_SYNC));
    const result = Atomics.waitAsync(this._sharedState, READ_PROCESS, EMPTY_SIGN, START_TIMEOUT);
    if (!result.async) {
      if (result.value === 'not-equal') return void this._synchronize();
      return void this.destroy(new Error(READER_FAILED_TIMEOUT));
    }

    result.value.then(v => {
      if (v !== 'ok') return void this.destroy(new Error(READER_FAILED_TIMEOUT));
      const status = Atomics.load(this._sharedState, READ_PROCESS);
      if (status !== READY_SIGN) return void this.destroy(new Error(READER_EXIT_AT_SYNC));
      this._watching = true;
      this._ready = true;
      this.emit('ready');
      this._watch();
    });
  }

  _watch() {
    if (!this._watching) return;
    const status = Atomics.load(this._sharedState, READ_PROCESS);

    if (status === FINISHING_SIGN) {
      if (!this._extraBuffer.length) return void this.end();
      this.once('drain', () => void this.end());
    }

    if (status === FAILED_SIGN || status === FINISHED_SIGN) {
      return void this.destroy(new Error(READER_EXIT_WHILE_WATCH));
    }

    const { async, value } = Atomics.waitAsync(this._sharedState, READ_PROCESS, status);
    if (!async) return void this._watch();
    value.then(() => this._watch());
  }

  _store(data, isNotFin = 0) {
    //! Data + EXTRA_BYTES <= this._sharedBuffer.byteLength
    const written = this._sharedBuffer.write(data, this._write + PREFIX_SIZE);
    // process._rawDebug({ data, written, offset: this._write, fin: isNotFin });
    this._sharedBuffer.writeInt32LE(written, this._write);
    this._sharedBuffer.writeUInt8(isNotFin, this._write + written + EXTRA_SPACE);
    this._write += PREFIX_SIZE + written + POSTFIX_SIZE + 1;
    Atomics.store(this._sharedState, WRITE_INDEX, this._write);
    Atomics.notify(this._sharedState, WRITE_INDEX);
  }

  _drain() {
    do {
      var item = this._extraBuffer.shift();
      var status = Write.call(this, false, item);
      if (status) return false;
    } while (this._extraBuffer.length);

    this.write = Write.bind(this, false);
    this._needDrain = false;
    this.emit('drain');
    return true;
  }

  end() {
    if (!this.writable) return;
    this._process = FINISHING_SIGN;
    this._watching = false;
    this._ending = true;

    let status = Atomics.load(this._sharedState, READ_PROCESS);
    if (status === READY_SIGN || status === EMPTY_SIGN || status === FINISHING_SIGN) {
      Atomics.store(this._sharedState, WRITE_PROCESS, FINISHING_SIGN);
      Atomics.notify(this._sharedState, WRITE_PROCESS);
    }

    let spins = 0;
    const origin = status;
    while (status !== FAILED_SIGN || status !== FINISHED_SIGN) {
      Atomics.wait(this._sharedState, READ_PROCESS, status, SPIN_TIMEOUT);
      status = Atomics.load(this._sharedState, READ_PROCESS);
      if (++spins === FINISH_SPINS) break;
    }

    if (status === FAILED_SIGN || status === origin) {
      this._ending = false;
      if (status === origin) this.destroy(new Error(FAILED_TO_FINISH_TIMEOUT));
      else this.destroy(new Error(FAILED_TO_FINISH_READER));
      return;
    }

    Atomics.store(this._sharedState, WRITE_PROCESS, FINISHED_SIGN);
    Atomics.notify(this._sharedState, WRITE_PROCESS);
    this._process = FINISHED_SIGN;
    this._finished = true;
    this._ended = true;
    this.emit('finish');
  }

  destroy(err) {
    if (!this.writable) return;
    this._watching = false;
    const readStatus = Atomics.load(this._sharedState, READ_PROCESS);
    const writeStatus = Atomics.load(this._sharedState, WRITE_PROCESS);
    const writeActive = writeStatus === READY_SIGN || writeStatus === EMPTY_SIGN;
    const readActive = readStatus === READY_SIGN || readStatus === EMPTY_SIGN;

    if (writeActive && readActive) {
      const sign = err ? FAILED_SIGN : FINISHED_SIGN;
      Atomics.store(this._sharedState, WRITE_PROCESS, sign);
      Atomics.notify(this._sharedState, WRITE_PROCESS);
    }

    if (err) {
      this._errored = err;
      this.emit('error', err);
    }

    this._destroyed = true;
    this._closed = true;
    this.emit('close');
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
    return !!this._errored;
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

  // eslint-disable-next-line class-methods-use-this
  get writableObjectMode() {
    return false;
  }
};

function Write(sync, toWrite) {
  if (!this.writable) return false;
  const read = Atomics.load(this._sharedState, READ_INDEX);
  const cycle = Atomics.load(this._sharedState, READ_CYCLE);
  const isBehind = read > this._write || cycle < this._cycle;
  let leftover = (isBehind ? read : this._sharedBuffer.length) - this._write;
  if (leftover < 0) throw new Error('overwritten'); //? Should never happen
  if (cycle > this._cycle) throw new Error('Read further than expected');
  leftover -= EXTRA_SPACE + 1;

  if (leftover <= 0 && isBehind) {
    if (sync) {
      var spins = 0;
      do {
        var status = Atomics.wait(this._sharedState, READ_INDEX, read, SPIN_TIMEOUT);
        if (++spins === READ_SPINS) throw new Error(TOO_LONG_READING);
      } while (status === 'timed-out');
      return Write.call(this, false, toWrite);
    }

    var { async, value } = Atomics.waitAsync(this._sharedState, READ_INDEX, read);
    if (!async) return Write.call(this, sync, toWrite);
    value.then(() => void this._drain());
    this.write = FakeWrite;
    this._needDrain = true;
    return FakeWrite.call(this, toWrite);
  }

  if (leftover <= 0 && !isBehind) {
    this._write = 0;
    Atomics.store(this._sharedState, WRITE_INDEX, this._write);
    Atomics.store(this._sharedState, WRITE_CYCLE, this._cycle++);
    Atomics.notify(this._sharedState, WRITE_INDEX);
    return Write.call(this, sync, toWrite);
  }

  if (leftover < Buffer.byteLength(toWrite) + EXTRA_SPACE) {
    this._store(toWrite.slice(0, leftover), 1);
    return Write.call(this, sync, toWrite.slice(leftover));
  }

  this._store(toWrite);
  return false;
}

function FakeWrite(data) {
  this._extraBuffer.push(data);
  return true;
}
