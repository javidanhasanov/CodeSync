'use strict';

/**
 * ===============================================================================
 *  CodeSync Broker
 *  Real-time file synchronization broker with VCS (version control system)
 *
 *  @version  4.1.0
 *  @protocol 2
 *
 *  Setup:
 *      npm install ws
 *      npm install diff-match-patch      (optional - patch protocol)
 *      npm install archiver              (optional - ZIP download)
 *      node server.js
 *
 *  Environment:
 *      PORT                          HTTP/WS port                (default: 3000)
 *      HOST                          Bind address                (default: 0.0.0.0)
 *      CODESYNC_SNAPSHOT_DIR         Snapshot directory          (default: ./_snapshots)
 *      CODESYNC_VCS_DIR              VCS directory               (default: ./_vcs)
 *      CODESYNC_VCS                  0 disables VCS              (default: enabled)
 *      CODESYNC_AUTO_COMMIT_INTERVAL Auto-commit interval ms      (default: 300000)
 *      CODESYNC_MAX_AUTO_COMMITS     Max auto-commits per branch (default: 100)
 *      CODESYNC_SNAPSHOT_DEBOUNCE    Snapshot debounce ms        (default: 5000)
 *      CODESYNC_SNAPSHOTS            0 disables snapshots
 *      CODESYNC_RESTORE              0 disables snapshot restore
 *      CODESYNC_MAX_CONNECTIONS      Global WS limit             (default: 500)
 *      CODESYNC_MAX_PER_IP           Per-IP limit                (default: 50)
 *      CODESYNC_SERVER_SECRET        Shared secret for server role
 *      CODESYNC_TRUST_PROXY          1 - trust X-Forwarded-For
 *      CODESYNC_QUIET                1 - suppress info logs
 *      CODESYNC_DEBUG                1 - verbose debug logs
 *
 *  Protocol (client -> server):
 *      hello                   { username, role, protocolVersion, capabilities, secret? }
 *      set-project-name        { name }
 *      file-update             { path, content, hash? }
 *      file-patch              { path, patch, baseHash?, resultHash? }
 *      file-delete             { path }
 *      snapshot-request        {}
 *      snapshot-restore        { name }
 *      commit                  { message }
 *      commit-request-dirty    {}
 *      branch-create           { name, from, autocommit }
 *      branch-switch           { name }
 *      branch-delete           { name }
 *      branch-merge-preview    { from, to }
 *      branch-merge            { from, to, resolutions }
 *      commit-restore          { id }
 *      history-request         { limit?, includeAuto? }
 *      ping                    { t }
 *
 *  Protocol (server -> client):
 *      init-begin / init-files / init-end / init
 *      project-name, client-count
 *      file-update, file-delete
 *      snapshot-created, snapshot-restored, filesystem-replaced
 *      conflict-detected, role-assigned
 *      vcs-state, commit-created, branch-created, branch-switch-started,
 *      branch-switched, branch-deleted, merge-preview, merge-started,
 *      merge-completed, commit-restored, history, dirty-files
 *      error, pong
 * ===============================================================================
 */

const http = require('http');
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

let DMPClass = null;
try { DMPClass = require('diff-match-patch'); } catch { /* optional */ }
const dmp = DMPClass ? new DMPClass() : null;

let ArchiverClass = null;
try { ArchiverClass = require('archiver'); } catch { /* optional */ }

const SERVER_VERSION = '4.1.0';
const PROTOCOL_VERSION = 2;
const WS_OPEN = WebSocket.OPEN;
const SERVER_CAPABILITIES = Object.freeze(dmp ? ['patch-v1'] : []);

/* ═══════════════════════════════════════════════════════════════════════
   Error types
   ═══════════════════════════════════════════════════════════════════════ */

class AppError extends Error {
  constructor(code, message, meta = {}) {
    super(message);
    this.name = 'AppError';
    this.code = code;
    this.meta = meta;
  }
}

/* ═══════════════════════════════════════════════════════════════════════
   Config
   ═══════════════════════════════════════════════════════════════════════ */

function parseIntEnv(name, defaultValue, opts = {}) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return defaultValue;
  const n = Number.parseInt(raw, 10);
  if (Number.isNaN(n)) return defaultValue;
  if (opts.min !== undefined && n < opts.min) return defaultValue;
  if (opts.max !== undefined && n > opts.max) return defaultValue;
  return n;
}
function parseBoolEnv(name, defaultValue) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return defaultValue;
  return raw === '1' || raw.toLowerCase() === 'true';
}

const CONFIG = Object.freeze({
  PORT: parseIntEnv('PORT', 3000, { min: 1, max: 65535 }),
  HOST: process.env.HOST || '0.0.0.0',

  MAX_FILE_SIZE: 5 * 1024 * 1024,
  MAX_PATH_LENGTH: 512,
  MAX_USERNAME_LENGTH: 32,
  MAX_PROJECT_NAME_LENGTH: 64,
  MAX_MESSAGE_BYTES: 6 * 1024 * 1024,
  MAX_TOTAL_FILES: 20000,
  MAX_TOTAL_BYTES: 500 * 1024 * 1024,
  MAX_WS_BUFFERED_BYTES: 8 * 1024 * 1024,

  MAX_CONNECTIONS: parseIntEnv('CODESYNC_MAX_CONNECTIONS', 500, { min: 1, max: 100000 }),
  MAX_CONNECTIONS_PER_IP: parseIntEnv('CODESYNC_MAX_PER_IP', 50, { min: 1, max: 10000 }),

  INIT_CHUNK_SIZE: 100,
  INIT_CHUNK_THRESHOLD: 50,
  INIT_CHUNK_DELAY_MS: 5,
  INIT_FALLBACK_TIMEOUT_MS: 5000,

  HTTP_KEEP_ALIVE_TIMEOUT_MS: 65000,
  HTTP_HEADERS_TIMEOUT_MS: 66000,
  HTTP_REQUEST_TIMEOUT_MS: 30000,

  SNAPSHOT_DIR: process.env.CODESYNC_SNAPSHOT_DIR || path.join(__dirname, '_snapshots'),
  SNAPSHOT_DEBOUNCE_MS: parseIntEnv('CODESYNC_SNAPSHOT_DEBOUNCE', 5000, { min: 0, max: 600000 }),
  SNAPSHOT_MIN_INTERVAL_MS: parseIntEnv('CODESYNC_SNAPSHOT_MIN', 3000, { min: 0, max: 600000 }),
  SNAPSHOT_HISTORY: parseIntEnv('CODESYNC_SNAPSHOT_HISTORY', 500, { min: 1, max: 100000 }),
  SNAPSHOT_RETAIN: parseIntEnv('CODESYNC_SNAPSHOT_RETAIN', 500, { min: 1, max: 100000 }),
  ENABLE_SNAPSHOTS: parseBoolEnv('CODESYNC_SNAPSHOTS', true),
  RESTORE_FROM_SNAPSHOT: parseBoolEnv('CODESYNC_RESTORE', true),

  VCS_DIR: process.env.CODESYNC_VCS_DIR || path.join(__dirname, '_vcs'),
  ENABLE_VCS: parseBoolEnv('CODESYNC_VCS', true),
  AUTO_COMMIT_INTERVAL_MS: parseIntEnv('CODESYNC_AUTO_COMMIT_INTERVAL', 5 * 60 * 1000, { min: 30000, max: 3600000 }),
  MAX_AUTO_COMMITS: parseIntEnv('CODESYNC_MAX_AUTO_COMMITS', 100, { min: 10, max: 10000 }),

  SERVER_SECRET: process.env.CODESYNC_SERVER_SECRET || null,
  TRUST_PROXY: parseBoolEnv('CODESYNC_TRUST_PROXY', false),

  QUIET: parseBoolEnv('CODESYNC_QUIET', false),
  DEBUG: parseBoolEnv('CODESYNC_DEBUG', false),
  HEARTBEAT_INTERVAL_MS: 30000,
  RATE_LIMIT_WINDOW_MS: 1000,
  RATE_LIMIT_MAX_MESSAGES: 200,
  CONFLICT_WINDOW_MS: 30000,
});

/* ═══════════════════════════════════════════════════════════════════════
   Logging
   ═══════════════════════════════════════════════════════════════════════ */

const LogLevel = Object.freeze({ DEBUG: 10, INFO: 20, WARN: 30, ERROR: 40 });
const logLevelThreshold = CONFIG.DEBUG ? LogLevel.DEBUG : CONFIG.QUIET ? LogLevel.WARN : LogLevel.INFO;

function logLine(level, tag, args) {
  if (level < logLevelThreshold) return;
  const ts = new Date().toISOString();
  const prefix = `[${ts}] [${tag}]`;
  const method = level >= LogLevel.ERROR ? 'error' : level >= LogLevel.WARN ? 'warn' : 'log';
  console[method](prefix, ...args);
}
const log = {
  debug: (...args) => logLine(LogLevel.DEBUG, 'DBG', args),
  info:  (...args) => logLine(LogLevel.INFO,  'INF', args),
  warn:  (...args) => logLine(LogLevel.WARN,  'WRN', args),
  error: (...args) => logLine(LogLevel.ERROR, 'ERR', args),
};

/* ═══════════════════════════════════════════════════════════════════════
   Utilities
   ═══════════════════════════════════════════════════════════════════════ */

const WINDOWS_RESERVED = /^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\..*)?$/i;

function hashContent(str) {
  if (typeof str !== 'string') return '';
  let h1 = 0xdeadbeef;
  let h2 = 0x41c6ce57;
  for (let i = 0; i < str.length; i++) {
    const ch = str.charCodeAt(i);
    h1 = Math.imul(h1 ^ ch, 2654435761);
    h2 = Math.imul(h2 ^ ch, 1597334677);
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
  return (h2 >>> 0).toString(16).padStart(8, '0') + (h1 >>> 0).toString(16).padStart(8, '0');
}

function formatBytes(n) {
  if (n == null) return '?';
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MB`;
}

function newRequestId() {
  return crypto.randomBytes(6).toString('hex');
}

function getLocalIps() {
  const result = [];
  try {
    const ifaces = os.networkInterfaces();
    for (const name of Object.keys(ifaces)) {
      const addrs = ifaces[name];
      if (!addrs) continue;
      for (const addr of addrs) {
        if (addr.family === 'IPv4' && !addr.internal) result.push(addr.address);
      }
    }
  } catch { /* noop */ }
  return result;
}

function getClientIp(req) {
  if (!req) return 'unknown';
  if (CONFIG.TRUST_PROXY) {
    const fwd = req.headers && req.headers['x-forwarded-for'];
    if (typeof fwd === 'string') return fwd.split(',')[0].trim();
  }
  return (req.socket && req.socket.remoteAddress) || 'unknown';
}

function validatePath(rawPath) {
  if (typeof rawPath !== 'string') return { ok: false, error: 'path must be a string' };
  if (rawPath.length === 0) return { ok: false, error: 'path is empty' };
  if (rawPath.length > CONFIG.MAX_PATH_LENGTH) return { ok: false, error: 'path too long' };
  if (rawPath.includes('\0')) return { ok: false, error: 'path contains null byte' };
  if (rawPath.includes('\\')) return { ok: false, error: 'backslashes not allowed' };
  if (rawPath.startsWith('/')) return { ok: false, error: 'absolute paths not allowed' };
  if (/^[a-zA-Z]:/.test(rawPath)) return { ok: false, error: 'absolute paths not allowed' };
  if (rawPath.endsWith('/')) return { ok: false, error: 'path must not end with slash' };
  if (rawPath.includes('//')) return { ok: false, error: 'path contains empty segments' };

  for (const seg of rawPath.split('/')) {
    if (seg === '' || seg === '.' || seg === '..') {
      return { ok: false, error: 'path contains invalid segments' };
    }
    if (WINDOWS_RESERVED.test(seg)) return { ok: false, error: `reserved filename: ${seg}` };
  }
  return { ok: true };
}

function sanitizeSegment(segment) {
  return segment.replace(/[<>:"|?*\x00-\x1f]/g, '_').replace(/^\.+$/, '_');
}

function sanitizeForDisk(filePath) {
  return filePath.split('/').filter(Boolean).map(sanitizeSegment).join(path.sep);
}

function sanitizeZipPath(relPath) {
  // Prevent path traversal in zip entries and use forward slashes
  const segs = relPath.split('/').filter(Boolean).map(seg => {
    if (seg === '.' || seg === '..') return '_';
    return seg;
  });
  return segs.join('/');
}

function validateUsername(raw) {
  if (typeof raw !== 'string') return { ok: false, error: 'username must be a string' };
  const trimmed = raw.trim().slice(0, CONFIG.MAX_USERNAME_LENGTH);
  if (!trimmed) return { ok: false, error: 'username is empty' };
  return { ok: true, value: trimmed };
}

function validateProjectName(raw) {
  if (typeof raw !== 'string') return { ok: false, error: 'name must be a string' };
  const trimmed = raw.trim().slice(0, CONFIG.MAX_PROJECT_NAME_LENGTH);
  if (!trimmed) return { ok: false, error: 'name is empty' };
  if (!/^[\p{L}\p{N}\s._-]+$/u.test(trimmed)) {
    return { ok: false, error: 'name contains invalid characters' };
  }
  return { ok: true, value: trimmed };
}

function validateSnapshotName(raw) {
  if (typeof raw !== 'string') return { ok: false, error: 'name must be a string' };
  if (raw.length === 0 || raw.length > 256) return { ok: false, error: 'invalid name length' };
  if (raw.includes('/') || raw.includes('\\') || raw.includes('..')) {
    return { ok: false, error: 'invalid snapshot name' };
  }
  if (!/^[A-Za-z0-9\s._-]+$/.test(raw)) return { ok: false, error: 'invalid characters in name' };
  return { ok: true, value: raw };
}

function validateBranchName(raw) {
  if (typeof raw !== 'string') return { ok: false, error: 'name must be a string' };
  const trimmed = raw.trim();
  if (trimmed.length < 2 || trimmed.length > 64) {
    return { ok: false, error: 'branch name must be 2-64 characters' };
  }
  if (!/^[A-Za-z0-9._-]+$/.test(trimmed)) {
    return { ok: false, error: 'branch name contains invalid characters' };
  }
  return { ok: true, value: trimmed };
}

function applyPatchToContent(baseContent, patchText, expectedBaseHash, expectedResultHash) {
  if (!dmp) throw new AppError('patch-rejected', 'Patch protocol is not enabled.');
  if (expectedBaseHash && hashContent(baseContent) !== expectedBaseHash) {
    throw new AppError('patch-base-mismatch', 'Base hash does not match.');
  }
  let patches;
  try {
    patches = dmp.patch_fromText(patchText);
  } catch (e) {
    throw new AppError('patch-rejected', `Patch parse failed: ${e.message}`);
  }
  if (!patches || patches.length === 0) throw new AppError('patch-rejected', 'Patch is empty.');
  let result, applied;
  try {
    [result, applied] = dmp.patch_apply(patches, baseContent);
  } catch (e) {
    throw new AppError('patch-rejected', `Patch apply failed: ${e.message}`);
  }
  if (!applied.every(Boolean)) throw new AppError('patch-rejected', 'Patch incomplete.');
  if (expectedResultHash && hashContent(result) !== expectedResultHash) {
    throw new AppError('patch-rejected', 'Result hash does not match.');
  }
  return result;
}

/* ═══════════════════════════════════════════════════════════════════════
   Metrics
   ═══════════════════════════════════════════════════════════════════════ */

const metrics = {
  startedAt: Date.now(),
  messagesReceived: 0,
  messagesSent: 0,
  bytesReceived: 0,
  bytesSent: 0,
  fileUpdates: 0,
  fileDeletes: 0,
  filesCreated: 0,
  patchesApplied: 0,
  patchesRejected: 0,
  snapshotsCreated: 0,
  snapshotsFailed: 0,
  snapshotsDeleted: 0,
  snapshotsRestored: 0,
  commitsCreated: 0,
  commitsAuto: 0,
  commitsRestored: 0,
  branchesCreated: 0,
  branchesSwitched: 0,
  branchesDeleted: 0,
  mergesCompleted: 0,
  conflictsDetected: 0,
  errorsSent: 0,
  rateLimitHits: 0,
  wsConnectionsTotal: 0,
  wsConnectionsRejected: 0,
  initChunkedSent: 0,
  initLegacySent: 0,
  zipDownloads: 0,
};

/* ═══════════════════════════════════════════════════════════════════════
   FileStore
   ═══════════════════════════════════════════════════════════════════════ */

class FileStore {
  constructor() {
    this.files = new Map();
    this.lastEdits = new Map();
    this.totalBytes = 0;
    this.projectName = null;
  }
  get size() { return this.files.size; }
  get(p) { return this.files.get(p); }
  has(p) { return this.files.has(p); }
  entries() { return this.files.entries(); }
  keys() { return Array.from(this.files.keys()); }

  checkLimits() {
    if (this.files.size >= CONFIG.MAX_TOTAL_FILES) {
      return { ok: false, error: `file count limit (${CONFIG.MAX_TOTAL_FILES})` };
    }
    if (this.totalBytes >= CONFIG.MAX_TOTAL_BYTES) {
      return { ok: false, error: `memory limit (${formatBytes(CONFIG.MAX_TOTAL_BYTES)})` };
    }
    return { ok: true };
  }

  set(filePath, content, editor) {
    const previous = this.files.get(filePath);
    const prevBytes = previous !== undefined ? Buffer.byteLength(previous, 'utf8') : 0;
    const newBytes = Buffer.byteLength(content, 'utf8');

    let conflict;
    const lastEdit = this.lastEdits.get(filePath);
    const now = Date.now();
    if (
      lastEdit &&
      lastEdit.clientId !== editor.clientId &&
      now - lastEdit.ts < CONFIG.CONFLICT_WINDOW_MS
    ) {
      conflict = {
        withUsername: lastEdit.username,
        secondsAgo: Math.round((now - lastEdit.ts) / 1000),
      };
      metrics.conflictsDetected++;
    }

    this.lastEdits.set(filePath, {
      username: editor.username,
      clientId: editor.clientId,
      ts: now,
    });
    this.files.set(filePath, content);
    this.totalBytes = this.totalBytes - prevBytes + newBytes;

    const isNew = previous === undefined;
    if (isNew) metrics.filesCreated++;
    metrics.fileUpdates++;

    return { isNew, previous, conflict };
  }

  delete(filePath) {
    if (!this.files.has(filePath)) return null;
    const content = this.files.get(filePath);
    const bytesRemoved = Buffer.byteLength(content, 'utf8');
    this.files.delete(filePath);
    this.totalBytes -= bytesRemoved;
    this.lastEdits.delete(filePath);
    metrics.fileDeletes++;
    return { removed: content, bytesRemoved };
  }

  replaceAll(newFiles) {
    this.files = new Map(newFiles);
    this.lastEdits.clear();
    this.totalBytes = 0;
    for (const content of this.files.values()) {
      this.totalBytes += Buffer.byteLength(content, 'utf8');
    }
  }

  toArray() {
    return Array.from(this.files.entries()).map(([p, content]) => ({ path: p, content }));
  }

  clear() {
    this.files.clear();
    this.lastEdits.clear();
    this.totalBytes = 0;
  }

  computeHashes() {
    const out = {};
    for (const [p, content] of this.files) {
      out[p] = hashContent(content);
    }
    return out;
  }
}

/* ═══════════════════════════════════════════════════════════════════════
   ClientRegistry
   ═══════════════════════════════════════════════════════════════════════ */

class ClientRegistry {
  constructor() {
    this.clients = new Set();
    this.activeServerClient = null;
    this.nextClientId = 1;
  }

  register(ws, meta) {
    ws.clientId = this.nextClientId++;
    ws.username = meta.username;
    ws.role = meta.role;
    ws.remote = meta.remote;
    ws.isAlive = true;
    ws.connectedAt = Date.now();
    ws.rateBucket = { count: 0, windowStart: Date.now() };
    ws.initSent = false;
    ws.usernameRejected = false;
    ws.initFallbackTimer = null;
    ws.urlSecret = null;
    ws.protocolVersion = null;
    ws.capabilities = new Set();

    this.clients.add(ws);
    metrics.wsConnectionsTotal++;
    return ws.clientId;
  }

  unregister(ws) {
    if (ws.initFallbackTimer) {
      clearTimeout(ws.initFallbackTimer);
      ws.initFallbackTimer = null;
    }
    this.clients.delete(ws);
    if (this.activeServerClient === ws) {
      this.activeServerClient = null;
      log.info(`[S] Server role released (client #${ws.clientId})`);
    }
  }

  getOnline() {
    return Array.from(this.clients).filter((c) => c.readyState === WS_OPEN);
  }

  getOnlineUsernames() {
    return this.getOnline().map((c) => c.username || 'anonymous');
  }

  countByIp(ip) {
    if (!ip || ip === 'unknown') return 0;
    let count = 0;
    for (const c of this.clients) {
      if (c.remote === ip) count++;
    }
    return count;
  }

  isUsernameTaken(username, exclude) {
    for (const c of this.clients) {
      if (c === exclude) continue;
      if (c.readyState !== WS_OPEN) continue;
      if (c.username === username) return true;
    }
    return false;
  }

  claimServerRole(ws, providedSecret) {
    if (CONFIG.SERVER_SECRET) {
      if (!providedSecret || providedSecret !== CONFIG.SERVER_SECRET) {
        return { ok: false, reason: 'Invalid or missing server secret.' };
      }
    }
    if (this.activeServerClient && this.activeServerClient !== ws) {
      if (this.activeServerClient.readyState === WS_OPEN) {
        return {
          ok: false,
          reason: `Server role already held by "${this.activeServerClient.username}" (client #${this.activeServerClient.clientId}).`,
        };
      }
      log.warn(`[S] Stale server client #${this.activeServerClient.clientId} - releasing`);
      this.activeServerClient = null;
    }
    this.activeServerClient = ws;
    return { ok: true };
  }

  releaseServerRole(ws) {
    if (this.activeServerClient === ws) {
      this.activeServerClient = null;
      log.info(`[S] Server role released (client #${ws.clientId})`);
    }
  }

  checkRateLimit(ws) {
    const now = Date.now();
    const bucket = ws.rateBucket;
    if (now - bucket.windowStart >= CONFIG.RATE_LIMIT_WINDOW_MS) {
      bucket.count = 0;
      bucket.windowStart = now;
    }
    bucket.count++;
    if (bucket.count > CONFIG.RATE_LIMIT_MAX_MESSAGES) {
      metrics.rateLimitHits++;
      return false;
    }
    return true;
  }
}

/* ═══════════════════════════════════════════════════════════════════════
   SnapshotManager
   ═══════════════════════════════════════════════════════════════════════ */

class SnapshotManager {
  constructor(config, fileStore) {
    this.config = config;
    this.fs = fileStore;
    this.counter = 0;
    this.timer = null;
    this.inProgress = false;
    this.pending = false;
    this.pendingReason = null;
    this.lastAt = 0;
    this.restoring = false;
    this.history = [];
  }

  ensureDir() {
    if (!this.config.ENABLE_SNAPSHOTS) return;
    try { fs.mkdirSync(this.config.SNAPSHOT_DIR, { recursive: true }); }
    catch (e) { log.error(`[S] Failed to create snapshot dir: ${e.message}`); }
  }

  _countFilesRecursive(dir) {
    let count = 0;
    try {
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        const full = path.join(dir, entry.name);
        if (entry.isDirectory()) count += this._countFilesRecursive(full);
        else if (entry.isFile()) count++;
      }
    } catch { /* noop */ }
    return count;
  }

  _dirSizeRecursive(dir) {
    let size = 0;
    try {
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        const full = path.join(dir, entry.name);
        try {
          if (entry.isDirectory()) size += this._dirSizeRecursive(full);
          else if (entry.isFile()) size += fs.statSync(full).size;
        } catch { /* skip */ }
      }
    } catch { /* noop */ }
    return size;
  }

  scanExisting() {
    if (!this.config.ENABLE_SNAPSHOTS) return;
    if (!fs.existsSync(this.config.SNAPSHOT_DIR)) return;

    let maxCounter = 0;
    try {
      const entries = fs.readdirSync(this.config.SNAPSHOT_DIR);
      for (const name of entries) {
        const fullPath = path.join(this.config.SNAPSHOT_DIR, name);
        try {
          const stat = fs.statSync(fullPath);
          if (!stat.isDirectory()) continue;
          const match = name.match(/_(\d+)$/);
          if (match) {
            const n = Number.parseInt(match[1], 10);
            if (!Number.isNaN(n) && n > maxCounter) maxCounter = n;
          }
          this.history.push({
            name,
            createdAt: stat.mtimeMs,
            fileCount: this._countFilesRecursive(fullPath),
            size: this._dirSizeRecursive(fullPath),
          });
        } catch (e) {
          log.error(`[S] Failed to read snapshot "${name}": ${e.message}`);
        }
      }
      this.counter = maxCounter;
      this.history.sort((a, b) => a.createdAt - b.createdAt);
      if (this.history.length > this.config.SNAPSHOT_HISTORY) {
        this.history = this.history.slice(-this.config.SNAPSHOT_HISTORY);
      }
    } catch (e) {
      log.error(`[S] Snapshot dir scan failed: ${e.message}`);
    }
  }

  enforceRetention() {
    if (!this.config.ENABLE_SNAPSHOTS) return;
    const limit = Math.min(this.config.SNAPSHOT_RETAIN, this.config.SNAPSHOT_HISTORY);
    while (this.history.length > limit) {
      const oldest = this.history[0];
      const fullPath = path.join(this.config.SNAPSHOT_DIR, oldest.name);
      try {
        fs.rmSync(fullPath, { recursive: true, force: true });
        this.history.shift();
        metrics.snapshotsDeleted++;
      } catch (e) {
        log.error(`[S] Failed to delete snapshot "${oldest.name}": ${e.message}`);
        break;
      }
    }
  }

  restoreFromLatest() {
    if (!this.config.ENABLE_SNAPSHOTS || !this.config.RESTORE_FROM_SNAPSHOT) return false;
    if (this.history.length === 0) return false;

    const latest = this.history[this.history.length - 1];
    const snapDir = path.join(this.config.SNAPSHOT_DIR, latest.name);
    const nameMatch = latest.name.match(/^(.+)_(\d+)$/);
    const restoredProjectName = nameMatch ? nameMatch[1] : latest.name;

    try {
      const restored = new Map();
      const acc = { count: 0, bytes: 0 };
      this._walkRestore(snapDir, '', restored, acc);
      this.fs.replaceAll(restored);
      this.fs.projectName = restoredProjectName;
      log.info(`[S] Restored ${acc.count} files from "${latest.name}" (project: "${restoredProjectName}")`);
      return true;
    } catch (e) {
      log.error(`[S] Snapshot restore failed: ${e.message}`);
      this.fs.clear();
      this.fs.projectName = null;
      return false;
    }
  }

  _walkRestore(dir, basePath, out, acc) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      const rel = basePath ? `${basePath}/${entry.name}` : entry.name;
      if (entry.isDirectory()) {
        this._walkRestore(full, rel, out, acc);
      } else if (entry.isFile()) {
        if (entry.name === '_snapshot.json') continue;
        try {
          const stat = fs.statSync(full);
          if (stat.size > this.config.MAX_FILE_SIZE) continue;
          const content = fs.readFileSync(full, 'utf8');
          out.set(rel, content);
          acc.count++;
          acc.bytes += Buffer.byteLength(content, 'utf8');
        } catch (e) {
          log.error(`[S] Restore failed for "${rel}": ${e.message}`);
        }
      }
    }
  }

  async loadSnapshot(snapshotName) {
    const v = validateSnapshotName(snapshotName);
    if (!v.ok) throw new AppError('invalid-snapshot-name', v.error);
    const snapDir = path.join(this.config.SNAPSHOT_DIR, snapshotName);
    const stat = await fsp.stat(snapDir).catch(() => null);
    if (!stat || !stat.isDirectory()) {
      throw new AppError('snapshot-not-found', `Snapshot not found: ${snapshotName}`);
    }
    const restored = new Map();
    const acc = { count: 0, bytes: 0 };
    this._walkRestore(snapDir, '', restored, acc);
    return restored;
  }

  schedule(reason) {
    if (!this.config.ENABLE_SNAPSHOTS) return;
    if (this.timer) { this.pendingReason = reason; this.pending = true; return; }

    this.timer = setTimeout(async () => {
      this.timer = null;
      await this.create(reason);
      if (this.pending) {
        this.pending = false;
        const pendingReason = this.pendingReason || 'pending';
        this.pendingReason = null;
        this.schedule(pendingReason);
      }
    }, this.config.SNAPSHOT_DEBOUNCE_MS);
    if (this.timer.unref) this.timer.unref();
  }

  async create(reason) {
    if (!this.config.ENABLE_SNAPSHOTS) return null;
    if (this.inProgress) { this.pending = true; return null; }
    if (!this.fs.projectName) return null;
    if (this.fs.size === 0) return null;

    const now = Date.now();
    if (now - this.lastAt < this.config.SNAPSHOT_MIN_INTERVAL_MS) {
      this.pending = true;
      return null;
    }

    this.inProgress = true;
    this.lastAt = now;
    this.counter++;

    const snapshotName = `${this.fs.projectName}_${this.counter}`;
    const snapshotPath = path.join(this.config.SNAPSHOT_DIR, snapshotName);
    const tmpPath = `${snapshotPath}.tmp-${crypto.randomBytes(4).toString('hex')}`;

    try {
      await fsp.mkdir(tmpPath, { recursive: true });
      let fileCount = 0;
      let totalSize = 0;
      for (const [relPath, content] of this.fs.entries()) {
        const safeRel = sanitizeForDisk(relPath);
        const fullPath = path.join(tmpPath, safeRel);
        await fsp.mkdir(path.dirname(fullPath), { recursive: true });
        await fsp.writeFile(fullPath, content, 'utf8');
        fileCount++;
        totalSize += Buffer.byteLength(content, 'utf8');
      }
      const meta = {
        name: snapshotName,
        projectName: this.fs.projectName,
        createdAt: now,
        reason,
        fileCount,
        size: totalSize,
        version: SERVER_VERSION,
      };
      await fsp.writeFile(
        path.join(tmpPath, '_snapshot.json'),
        JSON.stringify(meta, null, 2),
        'utf8'
      );

      try {
        await fsp.rename(tmpPath, snapshotPath);
      } catch (renameErr) {
        if (renameErr.code === 'EEXIST' || renameErr.code === 'ENOTEMPTY') {
          await fsp.rm(tmpPath, { recursive: true, force: true });
          throw new Error(`Snapshot already exists: ${snapshotName}`);
        }
        await fsp.mkdir(snapshotPath, { recursive: true });
        for (const entry of await fsp.readdir(tmpPath)) {
          await fsp.rename(path.join(tmpPath, entry), path.join(snapshotPath, entry));
        }
        await fsp.rm(tmpPath, { recursive: true, force: true });
      }

      const entry = { name: snapshotName, createdAt: now, fileCount, size: totalSize };
      this.history.push(entry);
      metrics.snapshotsCreated++;
      if (this.history.length > this.config.SNAPSHOT_HISTORY) {
        this.history = this.history.slice(-this.config.SNAPSHOT_HISTORY);
      }
      this.enforceRetention();
      return entry;
    } catch (e) {
      log.error(`[S] Snapshot failed: ${e.message}`);
      this.counter--;
      metrics.snapshotsFailed++;
      await fsp.rm(tmpPath, { recursive: true, force: true }).catch(() => {});
      return null;
    } finally {
      this.inProgress = false;
    }
  }
}

/* ═══════════════════════════════════════════════════════════════════════
   VCS Manager
   ═══════════════════════════════════════════════════════════════════════ */

class VCSManager {
  constructor(config, fileStore) {
    this.config = config;
    this.fs = fileStore;
    this.meta = null;
    this.busy = false;
    this.busyReason = null;
    this.autoCommitTimer = null;
  }

  ensureDir() {
    if (!this.config.ENABLE_VCS) return;
    try { fs.mkdirSync(this.config.VCS_DIR, { recursive: true }); }
    catch (e) { log.error(`[VCS] Failed to create VCS dir: ${e.message}`); }
  }

  loadMeta() {
    if (!this.config.ENABLE_VCS) return;
    const metaPath = path.join(this.config.VCS_DIR, '_meta.json');
    try {
      if (fs.existsSync(metaPath)) {
        const raw = fs.readFileSync(metaPath, 'utf8');
        this.meta = JSON.parse(raw);
        log.info(`[VCS] Loaded metadata: ${Object.keys(this.meta.commits || {}).length} commits, ${Object.keys(this.meta.branches || {}).length} branches`);
      }
    } catch (e) {
      log.error(`[VCS] Failed to load meta: ${e.message}`);
    }

    if (!this.meta) {
      this.meta = {
        version: 1,
        projectName: null,
        activeBranch: 'main',
        nextCommitId: 1,
        branches: {
          main: { head: null, createdAt: Date.now(), createdBy: 'system', fromCommit: null },
        },
        commits: {},
      };
    }
    // Ensure structural integrity
    if (!this.meta.branches || typeof this.meta.branches !== 'object') {
      this.meta.branches = {
        main: { head: null, createdAt: Date.now(), createdBy: 'system', fromCommit: null },
      };
    }
    if (!this.meta.commits || typeof this.meta.commits !== 'object') {
      this.meta.commits = {};
    }
    if (!this.meta.activeBranch || !this.meta.branches[this.meta.activeBranch]) {
      this.meta.activeBranch = 'main';
      if (!this.meta.branches.main) {
        this.meta.branches.main = { head: null, createdAt: Date.now(), createdBy: 'system', fromCommit: null };
      }
    }
    if (typeof this.meta.nextCommitId !== 'number') {
      this.meta.nextCommitId = 1;
    }
    // Ensure nextCommitId is ahead of all existing commits
    let maxId = 0;
    for (const k of Object.keys(this.meta.commits)) {
      const n = Number.parseInt(k, 10);
      if (!Number.isNaN(n) && n > maxId) maxId = n;
    }
    if (this.meta.nextCommitId <= maxId) this.meta.nextCommitId = maxId + 1;
  }

  saveMeta() {
    if (!this.config.ENABLE_VCS || !this.meta) return;
    const metaPath = path.join(this.config.VCS_DIR, '_meta.json');
    const tmpPath = `${metaPath}.tmp`;
    try {
      fs.writeFileSync(tmpPath, JSON.stringify(this.meta, null, 2), 'utf8');
      fs.renameSync(tmpPath, metaPath);
    } catch (e) {
      log.error(`[VCS] Failed to save meta: ${e.message}`);
    }
  }

  _commitDirName(commit) {
    const projectName = commit.projectName || this.meta.projectName || 'project';
    return `${projectName}_${commit.branch}_${commit.id}`;
  }

  _getCommitDir(commit) {
    if (!commit) return null;
    return path.join(this.config.VCS_DIR, this._commitDirName(commit));
  }

  computeDirtyFiles() {
    const activeBranch = this.meta.activeBranch;
    const branch = this.meta.branches[activeBranch];
    if (!branch || !branch.head) {
      const files = [];
      for (const p of this.fs.keys()) files.push({ path: p, type: 'added' });
      return files;
    }

    const headCommit = this.meta.commits[String(branch.head)];
    if (!headCommit || !headCommit.hashes) {
      const files = [];
      for (const p of this.fs.keys()) files.push({ path: p, type: 'added' });
      return files;
    }

    const currentHashes = this.fs.computeHashes();
    const dirty = [];

    for (const p of Object.keys(currentHashes)) {
      if (currentHashes[p] !== headCommit.hashes[p]) {
        dirty.push({
          path: p,
          type: headCommit.hashes[p] === undefined ? 'added' : 'modified',
        });
      }
    }
    for (const p of Object.keys(headCommit.hashes)) {
      if (currentHashes[p] === undefined) dirty.push({ path: p, type: 'deleted' });
    }
    return dirty;
  }

  /**
   * Create a commit on a branch.
   * opts:
   *   branch      - branch name (default active)
   *   auto        - mark as auto commit
   *   _internal   - bypass busy check (for safety commits inside switch/restore/branch-create)
   *   broadcast   - default true; set false to suppress `commit-created` broadcast
   */
  createCommit(message, author, opts = {}) {
    if (!this.config.ENABLE_VCS) return { ok: false, error: 'VCS disabled' };
    if (this.busy && !opts._internal) return { ok: false, error: 'VCS busy: ' + this.busyReason };

    const branchName = opts.branch || this.meta.activeBranch;
    const branch = this.meta.branches[branchName];
    if (!branch) return { ok: false, error: `Branch not found: ${branchName}` };

    const dirty = this.computeDirtyFiles();
    if (dirty.length === 0) return { ok: false, error: 'No changes to commit', dirty: [] };

    const commitId = this.meta.nextCommitId++;
    const projectName = this.meta.projectName || 'project';
    const commitDirName = `${projectName}_${branchName}_${commitId}`;
    const commitDir = path.join(this.config.VCS_DIR, commitDirName);
    const tmpDir = `${commitDir}.tmp`;

    try {
      fs.mkdirSync(tmpDir, { recursive: true });

      const hashes = {};
      let fileCount = 0;
      let bytesTotal = 0;

      for (const [relPath, content] of this.fs.entries()) {
        const safeRel = sanitizeForDisk(relPath);
        const fullPath = path.join(tmpDir, safeRel);
        fs.mkdirSync(path.dirname(fullPath), { recursive: true });
        fs.writeFileSync(fullPath, content, 'utf8');
        hashes[relPath] = hashContent(content);
        fileCount++;
        bytesTotal += Buffer.byteLength(content, 'utf8');
      }

      const commitData = {
        id: commitId,
        branch: branchName,
        message: String(message || '').slice(0, 500),
        author: author || 'unknown',
        ts: Date.now(),
        parent: branch.head || null,
        parents: branch.head ? [branch.head] : [],
        auto: !!opts.auto,
        projectName,
        hashes,
        filesCount: fileCount,
        bytesTotal,
        changes: dirty.map(d => ({ path: d.path, type: d.type })),
      };

      fs.writeFileSync(
        path.join(tmpDir, '_commit.json'),
        JSON.stringify(commitData, null, 2),
        'utf8'
      );

      fs.renameSync(tmpDir, commitDir);

      this.meta.commits[String(commitId)] = commitData;
      branch.head = commitId;

      if (opts.auto) {
        metrics.commitsAuto++;
        this.pruneOldAutoCommits(branchName);
      } else {
        metrics.commitsCreated++;
      }

      this.saveMeta();

      if (opts.broadcast !== false) {
        broadcast({
          type: 'commit-created',
          commit: this._stripCommit(commitData),
        });
      }

      log.info(`[VCS] Commit #${commitId} created on "${branchName}" by "${author}": "${message}" (${fileCount} files)`);
      return { ok: true, commit: commitData };
    } catch (e) {
      try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
      this.meta.nextCommitId--;
      log.error(`[VCS] Commit failed: ${e.message}`);
      return { ok: false, error: e.message };
    }
  }

  pruneOldAutoCommits(branchName) {
    const branchCommits = Object.values(this.meta.commits)
      .filter(c => c.auto && c.branch === branchName)
      .sort((a, b) => a.id - b.id);

    if (branchCommits.length <= this.config.MAX_AUTO_COMMITS) return;

    const toDelete = branchCommits.slice(0, branchCommits.length - this.config.MAX_AUTO_COMMITS);
    for (const c of toDelete) {
      const dirPath = this._getCommitDir(c);
      if (dirPath) {
        try { fs.rmSync(dirPath, { recursive: true, force: true }); } catch {}
      }
      delete this.meta.commits[String(c.id)];
      log.debug(`[VCS] Pruned auto-commit #${c.id}`);
    }
  }

  _walkCommitDir(dir, basePath, out) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      const rel = basePath ? `${basePath}/${entry.name}` : entry.name;
      if (entry.isDirectory()) {
        this._walkCommitDir(full, rel, out);
      } else if (entry.isFile()) {
        if (entry.name === '_commit.json') continue;
        try {
          const content = fs.readFileSync(full, 'utf8');
          out.set(rel, content);
        } catch (e) {
          log.error(`[VCS] Failed to read "${rel}": ${e.message}`);
        }
      }
    }
  }

  _loadCommitFiles(commitId) {
    const commit = this.meta.commits[String(commitId)];
    if (!commit) return null;
    const dirPath = this._getCommitDir(commit);
    if (!dirPath || !fs.existsSync(dirPath)) return null;
    const files = new Map();
    this._walkCommitDir(dirPath, '', files);
    return files;
  }

  /**
   * Restore the filesystem to the state of a commit.
   * Only allows commits on the active branch (to avoid state corruption).
   * Creates a safety auto-commit first, then a new commit representing the restore.
   */
  restoreCommit(commitId, author) {
    if (!this.config.ENABLE_VCS) return { ok: false, error: 'VCS disabled' };
    if (this.busy) return { ok: false, error: 'VCS busy: ' + this.busyReason };

    const targetCommit = this.meta.commits[String(commitId)];
    if (!targetCommit) return { ok: false, error: `Commit #${commitId} not found` };
    if (targetCommit.branch !== this.meta.activeBranch) {
      return {
        ok: false,
        error: 'Only commits from the active branch can be restored. Switch to that branch first.',
      };
    }

    this.busy = true;
    this.busyReason = `restore #${commitId}`;

    try {
      // 1. Safety commit current state
      const safetyDirty = this.computeDirtyFiles();
      if (safetyDirty.length > 0) {
        const safetyResult = this.createCommit(
          `pre-restore safety (before #${commitId})`,
          author,
          { auto: true, _internal: true }
        );
        if (!safetyResult.ok) {
          log.warn(`[VCS] Pre-restore safety commit skipped: ${safetyResult.error}`);
        }
      }

      // 2. Load target commit files
      const restoredFiles = this._loadCommitFiles(commitId);
      if (!restoredFiles) {
        return { ok: false, error: `Commit files not found for #${commitId}` };
      }

      // 3. Replace filesystem
      this.fs.replaceAll(restoredFiles);

      // 4. Create new commit representing the restore
      const newCommitResult = this.createCommit(
        `restore to #${commitId}`,
        author,
        { _internal: true }
      );
      if (!newCommitResult.ok) {
        return { ok: false, error: 'Restore commit failed: ' + newCommitResult.error };
      }

      metrics.commitsRestored++;
      this.saveMeta();

      log.info(`[VCS] Restored #${commitId} → new commit #${newCommitResult.commit.id}`);
      return {
        ok: true,
        id: commitId,
        newCommitId: newCommitResult.commit.id,
        files: this.fs.toArray(),
        activeBranch: this.meta.activeBranch,
      };
    } finally {
      this.busy = false;
      this.busyReason = null;
    }
  }

  createBranch(name, fromBranch, author, autocommit) {
    if (this.busy) return { ok: false, error: 'VCS busy: ' + this.busyReason };
    if (this.meta.branches[name]) return { ok: false, error: `Branch "${name}" already exists` };
    const from = this.meta.branches[fromBranch];
    if (!from) return { ok: false, error: `Branch "${fromBranch}" not found` };

    if (autocommit) {
      const dirty = this.computeDirtyFiles();
      if (dirty.length > 0) {
        const result = this.createCommit(
          `pre-branch safety (${name})`,
          author,
          { branch: fromBranch, auto: true, _internal: true }
        );
        if (!result.ok) log.warn(`[VCS] Pre-branch safety skipped: ${result.error}`);
      }
    }

    const branch = {
      head: from.head,
      createdAt: Date.now(),
      createdBy: author,
      fromCommit: from.head,
    };
    this.meta.branches[name] = branch;
    metrics.branchesCreated++;
    this.saveMeta();

    log.info(`[VCS] Branch "${name}" created from "${fromBranch}" by "${author}"`);
    return { ok: true, branch };
  }

  switchBranch(name, author) {
    if (this.busy) return { ok: false, error: 'VCS busy: ' + this.busyReason };
    if (name === this.meta.activeBranch) return { ok: false, error: 'Already on this branch' };
    const target = this.meta.branches[name];
    if (!target) return { ok: false, error: `Branch "${name}" not found` };

    this.busy = true;
    this.busyReason = `switch to ${name}`;

    try {
      const safetyDirty = this.computeDirtyFiles();
      if (safetyDirty.length > 0) {
        const safetyResult = this.createCommit(
          `pre-switch safety (to ${name})`,
          author,
          { auto: true, _internal: true }
        );
        if (!safetyResult.ok) log.warn(`[VCS] Pre-switch safety skipped: ${safetyResult.error}`);
      }

      if (!target.head) {
        this.fs.replaceAll(new Map());
        this.meta.activeBranch = name;
        this.saveMeta();
        return {
          ok: true,
          name,
          commitId: null,
          files: [],
          branches: this._branchesAsArray(),
        };
      }

      const files = this._loadCommitFiles(target.head);
      if (!files) {
        return { ok: false, error: `Commit files not found for head #${target.head}` };
      }

      this.fs.replaceAll(files);
      this.meta.activeBranch = name;
      metrics.branchesSwitched++;
      this.saveMeta();

      log.info(`[VCS] Switched to branch "${name}" (head #${target.head}) by "${author}"`);
      return {
        ok: true,
        name,
        commitId: target.head,
        files: this.fs.toArray(),
        branches: this._branchesAsArray(),
      };
    } finally {
      this.busy = false;
      this.busyReason = null;
    }
  }

  deleteBranch(name) {
    if (this.busy) return { ok: false, error: 'VCS busy: ' + this.busyReason };
    if (name === 'main') return { ok: false, error: 'Cannot delete "main" branch' };
    if (name === this.meta.activeBranch) {
      return { ok: false, error: 'Cannot delete active branch - switch first' };
    }
    if (!this.meta.branches[name]) return { ok: false, error: `Branch "${name}" not found` };

    for (const c of Object.values(this.meta.commits)) {
      if (c.branch === name) {
        const dirPath = this._getCommitDir(c);
        if (dirPath) {
          try { fs.rmSync(dirPath, { recursive: true, force: true }); } catch {}
        }
        delete this.meta.commits[String(c.id)];
      }
    }

    delete this.meta.branches[name];
    metrics.branchesDeleted++;
    this.saveMeta();

    log.info(`[VCS] Branch "${name}" deleted`);
    return { ok: true };
  }

  findLCA(aId, bId) {
    if (aId === bId) return aId;

    const ancestorsA = new Set();
    let cur = aId;
    while (cur) {
      ancestorsA.add(cur);
      const c = this.meta.commits[String(cur)];
      if (!c) break;
      const parent = (c.parents && c.parents[0]) || c.parent || null;
      cur = parent;
    }

    cur = bId;
    while (cur) {
      if (ancestorsA.has(cur)) return cur;
      const c = this.meta.commits[String(cur)];
      if (!c) break;
      const parent = (c.parents && c.parents[0]) || c.parent || null;
      cur = parent;
    }
    return null;
  }

  mergePreview(fromBranchName, toBranchName) {
    if (this.busy) return { ok: false, error: 'VCS busy: ' + this.busyReason };
    const from = this.meta.branches[fromBranchName];
    const to = this.meta.branches[toBranchName];
    if (!from) return { ok: false, error: `Branch "${fromBranchName}" not found` };
    if (!to) return { ok: false, error: `Branch "${toBranchName}" not found` };
    if (!from.head) return { ok: false, error: 'Source branch has no commits' };

    const fromHead = from.head;
    const toHead = to.head || null;

    let baseId = null;
    if (toHead) baseId = this.findLCA(fromHead, toHead);

    const baseFiles = baseId ? (this._loadCommitFiles(baseId) || new Map()) : new Map();
    const fromFiles = this._loadCommitFiles(fromHead);
    const toFiles = toHead ? (this._loadCommitFiles(toHead) || new Map()) : new Map();

    if (!fromFiles) return { ok: false, error: 'Failed to load source commit' };

    const autoFiles = [];
    const conflicts = [];
    const allPaths = new Set([...fromFiles.keys(), ...toFiles.keys(), ...baseFiles.keys()]);

    for (const p of allPaths) {
      const b = baseFiles.get(p);
      const f = fromFiles.get(p);
      const t = toFiles.get(p);

      if (b === f) {
        continue;
      } else if (b === t) {
        if (f === undefined) autoFiles.push({ path: p, action: 'delete' });
        else if (t === undefined) autoFiles.push({ path: p, action: 'add' });
        else autoFiles.push({ path: p, action: 'modify' });
      } else if (f === t) {
        continue;
      } else {
        conflicts.push({ path: p });
      }
    }

    return { ok: true, from: fromBranchName, to: toBranchName, autoFiles, conflicts };
  }

  mergeBranches(fromBranchName, toBranchName, resolutions, author) {
    if (this.busy) return { ok: false, error: 'VCS busy: ' + this.busyReason };
    const from = this.meta.branches[fromBranchName];
    const to = this.meta.branches[toBranchName];
    if (!from || !to) return { ok: false, error: 'Branch not found' };
    if (!from.head) return { ok: false, error: 'Source branch has no commits' };

    this.busy = true;
    this.busyReason = `merge ${fromBranchName} → ${toBranchName}`;

    try {
      const fromHead = from.head;
      const toHead = to.head || null;
      const baseId = toHead ? this.findLCA(fromHead, toHead) : null;

      const baseFiles = baseId ? (this._loadCommitFiles(baseId) || new Map()) : new Map();
      const fromFiles = this._loadCommitFiles(fromHead);
      const toFiles = toHead ? (this._loadCommitFiles(toHead) || new Map()) : new Map();

      if (!fromFiles) return { ok: false, error: 'Failed to load source files' };

      const mergedFiles = new Map(toFiles);
      const allPaths = new Set([...fromFiles.keys(), ...toFiles.keys(), ...baseFiles.keys()]);
      const changes = [];

      for (const p of allPaths) {
        const b = baseFiles.get(p);
        const f = fromFiles.get(p);
        const t = toFiles.get(p);

        if (b === f) continue;
        if (b === t) {
          if (f === undefined) { mergedFiles.delete(p); changes.push({ path: p, type: 'deleted' }); }
          else {
            mergedFiles.set(p, f);
            changes.push({ path: p, type: t === undefined ? 'added' : 'modified' });
          }
        } else if (f === t) continue;
        else {
          const res = resolutions[p] || 'to';
          if (res === 'from') {
            if (f === undefined) { mergedFiles.delete(p); changes.push({ path: p, type: 'deleted' }); }
            else { mergedFiles.set(p, f); changes.push({ path: p, type: 'modified' }); }
          } else if (res === 'base') {
            if (b === undefined) { mergedFiles.delete(p); changes.push({ path: p, type: 'deleted' }); }
            else { mergedFiles.set(p, b); changes.push({ path: p, type: 'modified' }); }
          }
        }
      }

      const commitId = this.meta.nextCommitId++;
      const projectName = this.meta.projectName || 'project';
      const commitDirName = `${projectName}_${toBranchName}_${commitId}`;
      const commitDir = path.join(this.config.VCS_DIR, commitDirName);
      const tmpDir = `${commitDir}.tmp`;

      try {
        fs.mkdirSync(tmpDir, { recursive: true });

        const hashes = {};
        let fileCount = 0;
        let bytesTotal = 0;

        for (const [relPath, content] of mergedFiles) {
          const safeRel = sanitizeForDisk(relPath);
          const fullPath = path.join(tmpDir, safeRel);
          fs.mkdirSync(path.dirname(fullPath), { recursive: true });
          fs.writeFileSync(fullPath, content, 'utf8');
          hashes[relPath] = hashContent(content);
          fileCount++;
          bytesTotal += Buffer.byteLength(content, 'utf8');
        }

        const commitData = {
          id: commitId,
          branch: toBranchName,
          message: `merge: ${fromBranchName} → ${toBranchName}`,
          author: author || 'unknown',
          ts: Date.now(),
          parent: toHead,
          parents: toHead ? [toHead, fromHead] : [fromHead],
          auto: false,
          isMerge: true,
          projectName,
          hashes,
          filesCount: fileCount,
          bytesTotal,
          changes,
        };

        fs.writeFileSync(
          path.join(tmpDir, '_commit.json'),
          JSON.stringify(commitData, null, 2),
          'utf8'
        );

        fs.renameSync(tmpDir, commitDir);

        this.meta.commits[String(commitId)] = commitData;
        this.meta.branches[toBranchName].head = commitId;
        metrics.mergesCompleted++;
        metrics.commitsCreated++;

        let filesForClient = null;
        if (this.meta.activeBranch === toBranchName) {
          this.fs.replaceAll(mergedFiles);
          filesForClient = this.fs.toArray();
        }

        this.saveMeta();

        // Broadcast commit-created for the merge commit so client history stays in sync
        broadcast({
          type: 'commit-created',
          commit: this._stripCommit(commitData),
        });

        log.info(`[VCS] Merged "${fromBranchName}" into "${toBranchName}" → commit #${commitId}`);
        return {
          ok: true,
          commitId,
          files: filesForClient,
          branches: this._branchesAsArray(),
        };
      } catch (e) {
        try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
        this.meta.nextCommitId--;
        log.error(`[VCS] Merge commit failed: ${e.message}`);
        return { ok: false, error: e.message };
      }
    } finally {
      this.busy = false;
      this.busyReason = null;
    }
  }

  getHistory(limit = 200, includeAuto = false) {
    const commits = Object.values(this.meta.commits)
      .filter(c => includeAuto || !c.auto)
      .sort((a, b) => b.id - a.id)
      .slice(0, limit);

    return commits.map(c => {
      const out = { ...c };
      delete out.hashes;
      return out;
    });
  }

  getRecentCommits() {
    return this.getHistory(20, false);
  }

  _branchesAsArray() {
    return Object.entries(this.meta.branches).map(([name, b]) => ({
      name,
      head: b.head,
      createdAt: b.createdAt,
      createdBy: b.createdBy,
      fromCommit: b.fromCommit,
    }));
  }

  getState() {
    return {
      activeBranch: this.meta.activeBranch,
      branches: this._branchesAsArray(),
      recentCommits: this.getRecentCommits(),
    };
  }

  startAutoCommit() {
    if (!this.config.ENABLE_VCS) return;
    if (this.autoCommitTimer) clearInterval(this.autoCommitTimer);

    this.autoCommitTimer = setInterval(() => {
      if (this.busy) return;
      if (!this.fs.projectName || this.fs.size === 0) return;

      const dirty = this.computeDirtyFiles();
      if (dirty.length === 0) return;

      const time = new Date().toTimeString().slice(0, 5);
      const result = this.createCommit(`auto: ${time}`, 'auto-commit', { auto: true });
      if (result.ok) {
        log.info(`[VCS] Auto-commit #${result.commit.id} (${dirty.length} files)`);
      }
    }, this.config.AUTO_COMMIT_INTERVAL_MS);

    if (this.autoCommitTimer.unref) this.autoCommitTimer.unref();
  }

  _stripCommit(c) {
    const out = { ...c };
    delete out.hashes;
    return out;
  }

  stopAutoCommit() {
    if (this.autoCommitTimer) {
      clearInterval(this.autoCommitTimer);
      this.autoCommitTimer = null;
    }
  }
}

/* ═══════════════════════════════════════════════════════════════════════
   Application
   ═══════════════════════════════════════════════════════════════════════ */

const fileStore = new FileStore();
const registry = new ClientRegistry();
const snapshots = new SnapshotManager(CONFIG, fileStore);
const vcs = new VCSManager(CONFIG, fileStore);

/* ═══════════════════════════════════════════════════════════════════════
   Broadcast helpers
   ═══════════════════════════════════════════════════════════════════════ */

function broadcast(msg, except = null) {
  const payload = JSON.stringify(msg);
  const size = Buffer.byteLength(payload, 'utf8');
  for (const c of registry.clients) {
    if (c === except) continue;
    if (c.readyState !== WS_OPEN) continue;
    try {
      c.send(payload);
      metrics.messagesSent++;
      metrics.bytesSent += size;
    } catch (e) {
      log.error(`[!] Broadcast failed (client #${c.clientId}): ${e.message}`);
    }
  }
}

function broadcastClientCount() {
  const online = registry.getOnline();
  broadcast({
    type: 'client-count',
    count: online.length,
    users: online.map((c) => c.username || 'anonymous'),
  });
}

function sendToClient(ws, msg) {
  if (ws.readyState !== WS_OPEN) return false;
  if (ws.bufferedAmount > CONFIG.MAX_WS_BUFFERED_BYTES) {
    log.warn(`[!] Client #${ws.clientId} too slow - message dropped`);
    return false;
  }
  try {
    const payload = JSON.stringify(msg);
    ws.send(payload);
    metrics.messagesSent++;
    metrics.bytesSent += Buffer.byteLength(payload, 'utf8');
    return true;
  } catch (e) {
    log.error(`[!] Client #${ws.clientId} send error: ${e.message}`);
    return false;
  }
}

function sendError(ws, code, message, filePath, requestId) {
  metrics.errorsSent++;
  sendToClient(ws, {
    type: 'error',
    code,
    message,
    ...(filePath ? { path: filePath } : {}),
    ...(requestId ? { requestId } : {}),
  });
}

function sendInit(ws) {
  if (ws.initSent) return;
  ws.initSent = true;

  const allFiles = fileStore.toArray();
  const onlineCount = registry.getOnline().length;
  const vcsState = CONFIG.ENABLE_VCS ? vcs.getState() : null;

  if (allFiles.length <= CONFIG.INIT_CHUNK_THRESHOLD) {
    metrics.initLegacySent++;
    sendToClient(ws, {
      type: 'init',
      clientId: ws.clientId,
      clientCount: onlineCount,
      projectName: fileStore.projectName,
      files: allFiles,
      snapshotCount: snapshots.counter,
      protocolVersion: PROTOCOL_VERSION,
      capabilities: SERVER_CAPABILITIES,
      vcs: vcsState,
    });
  } else {
    metrics.initChunkedSent++;
    sendChunkedInit(ws, allFiles, vcsState).catch((e) => {
      log.error(`[!] Chunked init failed (client #${ws.clientId}): ${e.message}`);
    });
  }
}

async function sendChunkedInit(ws, allFiles, vcsState) {
  const onlineCount = registry.getOnline().length;

  sendToClient(ws, {
    type: 'init-begin',
    clientId: ws.clientId,
    clientCount: onlineCount,
    projectName: fileStore.projectName,
    snapshotCount: snapshots.counter,
    fileCount: allFiles.length,
    protocolVersion: PROTOCOL_VERSION,
    capabilities: SERVER_CAPABILITIES,
    vcs: vcsState,
  });

  for (let i = 0; i < allFiles.length; i += CONFIG.INIT_CHUNK_SIZE) {
    if (ws.readyState !== WS_OPEN) return;
    const chunk = allFiles.slice(i, i + CONFIG.INIT_CHUNK_SIZE);
    sendToClient(ws, { type: 'init-files', chunk });
    if (i + CONFIG.INIT_CHUNK_SIZE < allFiles.length) {
      await new Promise((r) => setTimeout(r, CONFIG.INIT_CHUNK_DELAY_MS));
    }
  }

  if (ws.readyState === WS_OPEN) {
    sendToClient(ws, { type: 'init-end' });
  }
}

/* ═══════════════════════════════════════════════════════════════════════
   HTTP server
   ═══════════════════════════════════════════════════════════════════════ */

function jsonResponse(res, statusCode, data, reqId) {
  const body = JSON.stringify(data, null, 2);
  const headers = {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(body, 'utf8'),
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'Referrer-Policy': 'no-referrer',
    Server: `CodeSync/${SERVER_VERSION}`,
  };
  if (reqId) headers['X-Request-Id'] = reqId;
  res.writeHead(statusCode, headers);
  res.end(body);
}

function methodNotAllowed(res, allowed, reqId) {
  const body = JSON.stringify({
    error: 'method-not-allowed',
    message: `Only ${allowed.join(', ')} allowed.`,
  }, null, 2);
  res.writeHead(405, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(body, 'utf8'),
    Allow: allowed.join(', '),
    ...(reqId ? { 'X-Request-Id': reqId } : {}),
  });
  res.end(body);
}

const httpServer = http.createServer(async (req, res) => {
  const url = req.url || '/';
  const method = req.method || 'GET';
  const baseUrl = url.split('?')[0];
  const reqId = newRequestId();

  try {
    if ((baseUrl === '/health' || baseUrl === '/api/health') && method === 'GET') {
      const online = registry.getOnline();
      const servers = online.filter((c) => c.role === 'server').length;
      const mem = process.memoryUsage();
      jsonResponse(res, 200, {
        status: 'ok',
        version: SERVER_VERSION,
        protocolVersion: PROTOCOL_VERSION,
        capabilities: SERVER_CAPABILITIES,
        uptime: Math.floor(process.uptime()),
        startedAt: metrics.startedAt,
        projectName: fileStore.projectName,
        files: fileStore.size,
        bytes: fileStore.totalBytes,
        snapshots: snapshots.counter,
        snapshotHistory: snapshots.history.length,
        vcs: CONFIG.ENABLE_VCS ? {
          activeBranch: vcs.meta.activeBranch,
          branches: vcs._branchesAsArray(),
          commits: Object.keys(vcs.meta.commits).length,
          busy: vcs.busy,
        } : null,
        clients: {
          total: online.length,
          servers,
          spokes: online.length - servers,
          users: online.map((c) => ({
            id: c.clientId,
            username: c.username,
            role: c.role,
            connectedAt: c.connectedAt,
          })),
        },
        metrics: { ...metrics },
        memory: {
          rss: mem.rss,
          heapUsed: mem.heapUsed,
          heapTotal: mem.heapTotal,
          external: mem.external,
        },
      }, reqId);
      return;
    }

    if (baseUrl === '/healthz' && method === 'GET') {
      jsonResponse(res, 200, { status: 'alive' }, reqId);
      return;
    }

    if (baseUrl === '/readyz' && method === 'GET') {
      const ready = httpServer.listening && !shuttingDown;
      jsonResponse(res, ready ? 200 : 503, { status: ready ? 'ready' : 'not-ready' }, reqId);
      return;
    }

    if (baseUrl === '/api/version' && method === 'GET') {
      jsonResponse(res, 200, {
        server: SERVER_VERSION,
        protocol: PROTOCOL_VERSION,
        capabilities: SERVER_CAPABILITIES,
        node: process.version,
        archiverAvailable: !!ArchiverClass,
        patchAvailable: !!dmp,
      }, reqId);
      return;
    }

    if (baseUrl === '/api/snapshots' && method === 'GET') {
      jsonResponse(res, 200, {
        projectName: fileStore.projectName,
        counter: snapshots.counter,
        directory: CONFIG.SNAPSHOT_DIR,
        snapshots: snapshots.history,
      }, reqId);
      return;
    }

    if (baseUrl === '/api/snapshot') {
      if (method !== 'POST') return methodNotAllowed(res, ['POST'], reqId);
      if (CONFIG.SERVER_SECRET) {
        const provided = req.headers['x-codesync-secret'];
        if (provided !== CONFIG.SERVER_SECRET) {
          jsonResponse(res, 403, { error: 'forbidden', message: 'Invalid secret header.' }, reqId);
          return;
        }
      }
      if (!CONFIG.ENABLE_SNAPSHOTS) {
        jsonResponse(res, 400, { error: 'snapshots-disabled', message: 'Snapshot disabled.' }, reqId);
        return;
      }
      const result = await snapshots.create('http-api');
      if (!result) {
        jsonResponse(res, 500, { error: 'snapshot-failed', message: 'Could not create.' }, reqId);
        return;
      }
      jsonResponse(res, 200, { ok: true, snapshot: result }, reqId);
      return;
    }

    if (baseUrl === '/api/files' && method === 'GET') {
      const files = fileStore.toArray().map(({ path: p, content }) => ({
        path: p,
        size: Buffer.byteLength(content, 'utf8'),
      }));
      jsonResponse(res, 200, { count: files.length, files }, reqId);
      return;
    }

    if (baseUrl === '/api/download' && method === 'GET') {
      if (!ArchiverClass) {
        jsonResponse(res, 501, {
          error: 'download-unavailable',
          message: 'ZIP download is not available. Install with: npm install archiver',
        }, reqId);
        return;
      }
      if (fileStore.size === 0) {
        jsonResponse(res, 404, {
          error: 'no-files',
          message: 'No files to archive.',
        }, reqId);
        return;
      }
      try {
        const safeProject = (fileStore.projectName || 'project').replace(/[^a-zA-Z0-9._-]/g, '_');
        const filename = `codesync-${safeProject}-${Date.now()}.zip`;
        res.writeHead(200, {
          'Content-Type': 'application/zip',
          'Content-Disposition': `attachment; filename="${filename}"`,
          'Cache-Control': 'no-cache',
          'X-Content-Type-Options': 'nosniff',
          Server: `CodeSync/${SERVER_VERSION}`,
          'X-Request-Id': reqId,
        });
        const archive = new ArchiverClass('zip', { zlib: { level: 9 } });
        archive.on('warning', (err) => {
          if (err.code === 'ENOENT') log.warn(`[!] Archive warning: ${err.message}`);
          else log.error(`[!] Archive warning: ${err.message}`);
        });
        archive.on('error', (err) => {
          log.error(`[!] Archive error: ${err.message}`);
          try { res.destroy(); } catch {}
        });
        archive.pipe(res);
        for (const [relPath, content] of fileStore.entries()) {
          const safePath = sanitizeZipPath(relPath);
          if (!safePath) continue;
          try {
            archive.append(content, { name: safePath });
          } catch (e) {
            log.warn(`[!] Archive append failed for "${safePath}": ${e.message}`);
          }
        }
        metrics.zipDownloads++;
        log.info(`[ZIP] Download requested: ${fileStore.size} files → ${filename}`);
        await archive.finalize();
      } catch (e) {
        log.error(`[!] Download error: ${e.message}`);
        try { res.end(); } catch {}
      }
      return;
    }

    if (baseUrl === '/api/vcs/state' && method === 'GET') {
      if (!CONFIG.ENABLE_VCS) {
        jsonResponse(res, 404, { error: 'vcs-disabled' }, reqId);
        return;
      }
      jsonResponse(res, 200, vcs.getState(), reqId);
      return;
    }

    if (baseUrl === '/api/vcs/history' && method === 'GET') {
      if (!CONFIG.ENABLE_VCS) {
        jsonResponse(res, 404, { error: 'vcs-disabled' }, reqId);
        return;
      }
      const urlObj = new URL(url, 'http://localhost');
      const limit = parseInt(urlObj.searchParams.get('limit') || '200', 10) || 200;
      const includeAuto = urlObj.searchParams.get('includeAuto') === '1';
      jsonResponse(res, 200, { commits: vcs.getHistory(limit, includeAuto) }, reqId);
      return;
    }

    if (baseUrl === '/' || baseUrl === '/index.html') {
      try {
        const html = await fsp.readFile(path.join(__dirname, 'index.html'));
        res.writeHead(200, {
          'Content-Type': 'text/html; charset=utf-8',
          'Cache-Control': 'no-cache',
          'Content-Length': html.length,
          'X-Content-Type-Options': 'nosniff',
          'X-Frame-Options': 'DENY',
          'Referrer-Policy': 'no-referrer',
          Server: `CodeSync/${SERVER_VERSION}`,
          'X-Request-Id': reqId,
        });
        res.end(html);
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end(`index.html not found: ${e.message}`);
      }
      return;
    }

    jsonResponse(res, 404, {
      error: 'not-found',
      message: `Endpoint not found: ${method} ${baseUrl}`,
    }, reqId);
  } catch (e) {
    log.error(`[!] HTTP handler error: ${e.message}`);
    try {
      jsonResponse(res, 500, { error: 'internal-error', message: 'Internal server error.' }, reqId);
    } catch { /* noop */ }
  }
});

httpServer.keepAliveTimeout = CONFIG.HTTP_KEEP_ALIVE_TIMEOUT_MS;
httpServer.headersTimeout = CONFIG.HTTP_HEADERS_TIMEOUT_MS;
httpServer.requestTimeout = CONFIG.HTTP_REQUEST_TIMEOUT_MS;

/* ═══════════════════════════════════════════════════════════════════════
   WebSocket server
   ═══════════════════════════════════════════════════════════════════════ */

const wss = new WebSocketServer({
  server: httpServer,
  path: '/ws',
  maxPayload: CONFIG.MAX_MESSAGE_BYTES,
  perMessageDeflate: false,
});

wss.on('connection', (ws, req) => {
  const remote = getClientIp(req);

  if (registry.clients.size >= CONFIG.MAX_CONNECTIONS) {
    metrics.wsConnectionsRejected++;
    try {
      ws.send(JSON.stringify({
        type: 'error',
        code: 'too-many-connections',
        message: `Server at capacity (${CONFIG.MAX_CONNECTIONS} connections).`,
      }), () => {
        try { ws.close(1013, 'Server at capacity'); } catch {}
      });
    } catch {
      try { ws.close(1013, 'Server at capacity'); } catch {}
    }
    return;
  }

  if (registry.countByIp(remote) >= CONFIG.MAX_CONNECTIONS_PER_IP) {
    metrics.wsConnectionsRejected++;
    try {
      ws.send(JSON.stringify({
        type: 'error',
        code: 'too-many-connections',
        message: `Too many connections from your IP (limit: ${CONFIG.MAX_CONNECTIONS_PER_IP}).`,
      }), () => {
        try { ws.close(1013, 'Too many connections from IP'); } catch {}
      });
    } catch {
      try { ws.close(1013, 'Too many connections from IP'); } catch {}
    }
    return;
  }

  const id = registry.register(ws, { username: 'anonymous', role: 'spoke', remote });

  try {
    const parsed = new URL(req.url, 'http://localhost');
    ws.urlSecret = parsed.searchParams.get('secret');
  } catch { /* noop */ }

  log.info(`[+] Client #${id} connected (${remote}) - total: ${registry.clients.size}`);

  ws.initFallbackTimer = setTimeout(() => {
    if (!ws.initSent && ws.readyState === WS_OPEN) {
      log.warn(`[!] Client #${id} did not send hello (${CONFIG.INIT_FALLBACK_TIMEOUT_MS}ms) - sending init anyway`);
      sendInit(ws);
    }
  }, CONFIG.INIT_FALLBACK_TIMEOUT_MS);
  if (ws.initFallbackTimer.unref) ws.initFallbackTimer.unref();

  broadcastClientCount();

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    metrics.messagesReceived++;
    metrics.bytesReceived += raw.length || 0;

    if (!registry.checkRateLimit(ws)) {
      sendError(ws, 'rate-limited', 'Too many messages. Slow down.');
      return;
    }

    let msg;
    try {
      msg = JSON.parse(raw.toString('utf8'));
    } catch {
      sendError(ws, 'invalid-json', 'Message is not valid JSON.');
      return;
    }

    if (!msg || typeof msg !== 'object' || typeof msg.type !== 'string') {
      sendError(ws, 'invalid-message', 'Message must be an object with a "type" field.');
      return;
    }

    if (msg.type === 'hello' && !ws.initSent) {
      if (ws.initFallbackTimer) {
        clearTimeout(ws.initFallbackTimer);
        ws.initFallbackTimer = null;
      }
      handleHello(ws, msg)
        .then(() => {
          if (ws.readyState === WS_OPEN && !ws.usernameRejected && !ws.initSent) {
            sendInit(ws);
          }
        })
        .catch((err) => {
          log.error(`[!] Client #${id} hello error: ${err.message}`);
          sendError(ws, 'internal-error', 'Could not process hello.');
        });
      return;
    }

    handleMessage(ws, msg).catch((err) => {
      if (err instanceof AppError) {
        log.warn(`[!] Client #${id} ${msg.type} error: ${err.code} - ${err.message}`);
        sendError(ws, err.code, err.message, err.meta && err.meta.path, err.meta && err.meta.requestId);
      } else {
        log.error(`[!] Client #${id} ${msg.type} error: ${err.message}`);
        if (CONFIG.DEBUG && err.stack) log.error(err.stack);
        sendError(ws, 'internal-error', 'Could not process message.');
      }
    });
  });

  ws.on('close', () => {
    registry.unregister(ws);
    log.info(`[-] Client #${id} (${ws.username}/${ws.role}) disconnected - total: ${registry.clients.size}`);
    broadcastClientCount();
  });

  ws.on('error', (err) => {
    log.error(`[!] Client #${id} error: ${err.message}`);
  });
});

/* ═══════════════════════════════════════════════════════════════════════
   Hello handler
   ═══════════════════════════════════════════════════════════════════════ */

async function handleHello(ws, msg) {
  ws.protocolVersion = typeof msg.protocolVersion === 'number' ? msg.protocolVersion : 1;
  ws.capabilities = new Set(Array.isArray(msg.capabilities) ? msg.capabilities : []);

  if (ws.protocolVersion !== PROTOCOL_VERSION) {
    log.warn(`[!] Client #${ws.clientId} uses protocol v${ws.protocolVersion} (server: v${PROTOCOL_VERSION})`);
  }

  const userCheck = validateUsername(msg.username);
  if (!userCheck.ok) {
    if (msg.username !== undefined) {
      sendError(ws, 'invalid-username', userCheck.error);
      ws.usernameRejected = true;
      return;
    }
    ws.username = `anonymous-${ws.clientId}`;
  } else {
    if (registry.isUsernameTaken(userCheck.value, ws)) {
      sendError(ws, 'username-taken', `"${userCheck.value}" is already in use.`);
      ws.usernameRejected = true;
      return;
    }
    ws.username = userCheck.value;
  }

  if (msg.role === 'server') {
    const secret = msg.secret || ws.urlSecret || null;
    const claim = registry.claimServerRole(ws, secret);

    if (claim.ok) {
      ws.role = 'server';
      log.info(`[S] Client #${ws.clientId} "${ws.username}" -> SERVER role`);
      sendToClient(ws, {
        type: 'role-assigned',
        role: 'server',
        reason: 'Server role assigned successfully.',
      });
    } else {
      ws.role = 'spoke';
      metrics.wsConnectionsRejected++;
      log.warn(`[!] Client #${ws.clientId} "${ws.username}" requested server role - DENIED: ${claim.reason}`);
      sendError(ws, 'role-rejected', claim.reason);
      sendToClient(ws, {
        type: 'role-assigned',
        role: 'spoke',
        reason: claim.reason,
      });
    }
  } else if (msg.role === 'spoke') {
    if (ws.role === 'server') registry.releaseServerRole(ws);
    ws.role = 'spoke';
    sendToClient(ws, { type: 'role-assigned', role: 'spoke' });
  }

  log.info(
    `[U] Client #${ws.clientId} -> "${ws.username}" (${ws.role}) ` +
    `[v${ws.protocolVersion}, caps=${Array.from(ws.capabilities).join(',') || 'none'}]`
  );
  broadcastClientCount();
}

/* ═══════════════════════════════════════════════════════════════════════
   File content handling
   ═══════════════════════════════════════════════════════════════════════ */

function applyFileContent(ws, filePath, content, reqId, source) {
  const previous = fileStore.get(filePath);
  if (previous === content) return;

  if (previous === undefined) {
    const memCheck = fileStore.checkLimits();
    if (!memCheck.ok) {
      sendError(ws, 'memory-limit', memCheck.error, filePath, reqId);
      return;
    }
  }

  const result = fileStore.set(filePath, content, {
    username: ws.username,
    clientId: ws.clientId,
  });

  if (result.conflict) {
    log.warn(
      `[!] Conflict: ${ws.username} (#${ws.clientId}) overwrote "${filePath}" ` +
      `(previous: ${result.conflict.withUsername}, ${result.conflict.secondsAgo}s ago)`
    );
    broadcast({
      type: 'conflict-detected',
      path: filePath,
      winner: ws.username,
      loser: result.conflict.withUsername,
      ts: Date.now(),
    });
  }

  const byteSize = Buffer.byteLength(content, 'utf8');
  const action = result.isNew ? 'created' : source === 'file-patch' ? 'patched' : 'updated';
  log.info(`[~] ${ws.username} ${action}: ${filePath} (${formatBytes(byteSize)})`);

  broadcast({
    type: 'file-update',
    path: filePath,
    content,
    senderId: ws.clientId,
    username: ws.username,
  }, ws);

  snapshots.schedule(result.isNew ? 'new-file' : source);
}

/* ═══════════════════════════════════════════════════════════════════════
   Message handlers
   ═══════════════════════════════════════════════════════════════════════ */

async function handleMessage(ws, msg) {
  const reqId = newRequestId();

  switch (msg.type) {

    case 'hello':
      await handleHello(ws, msg);
      break;

    case 'set-project-name': {
      if (ws.role !== 'server') {
        throw new AppError('permission-denied', 'Only server role can set project name.', { requestId: reqId });
      }
      const nameCheck = validateProjectName(msg.name);
      if (!nameCheck.ok) throw new AppError('invalid-name', nameCheck.error, { requestId: reqId });
      if (nameCheck.value === fileStore.projectName) return;

      const oldName = fileStore.projectName;
      fileStore.projectName = nameCheck.value;
      if (CONFIG.ENABLE_VCS) {
        vcs.meta.projectName = nameCheck.value;
        vcs.saveMeta();
      }
      log.info(`[P] Project name: "${oldName || '(none)'}" -> "${fileStore.projectName}" (${ws.username})`);
      broadcast({ type: 'project-name', name: fileStore.projectName }, ws);
      if (fileStore.size > 0 && snapshots.counter === 0) {
        snapshots.schedule('initial');
      }
      break;
    }

    case 'file-update': {
      if (vcs.busy) {
        throw new AppError('vcs-busy', `VCS operation in progress: ${vcs.busyReason}`, { path: msg.path, requestId: reqId });
      }
      const v = validatePath(msg.path);
      if (!v.ok) throw new AppError('invalid-path', v.error, { path: msg.path, requestId: reqId });
      if (typeof msg.content !== 'string') {
        throw new AppError('invalid-content', 'content must be a string.', { path: msg.path, requestId: reqId });
      }
      const byteSize = Buffer.byteLength(msg.content, 'utf8');
      if (byteSize > CONFIG.MAX_FILE_SIZE) {
        throw new AppError('file-too-large', `File size exceeds limit (${formatBytes(CONFIG.MAX_FILE_SIZE)}).`, { path: msg.path, requestId: reqId });
      }
      if (typeof msg.hash === 'string' && msg.hash.length > 0) {
        if (hashContent(msg.content) !== msg.hash) {
          throw new AppError('hash-mismatch', 'Content hash does not match.', { path: msg.path, requestId: reqId });
        }
      }
      applyFileContent(ws, msg.path, msg.content, reqId, 'file-update');
      break;
    }

    case 'file-patch': {
      if (vcs.busy) {
        throw new AppError('vcs-busy', `VCS operation in progress: ${vcs.busyReason}`, { path: msg.path, requestId: reqId });
      }
      if (!dmp) {
        metrics.patchesRejected++;
        throw new AppError('patch-rejected', 'Patch protocol is not enabled.', { path: msg.path, requestId: reqId });
      }
      const v = validatePath(msg.path);
      if (!v.ok) throw new AppError('invalid-path', v.error, { path: msg.path, requestId: reqId });
      if (typeof msg.patch !== 'string' || msg.patch.length === 0) {
        throw new AppError('patch-rejected', 'patch must be a non-empty string.', { path: msg.path, requestId: reqId });
      }
      const base = fileStore.get(msg.path);
      if (base === undefined) {
        metrics.patchesRejected++;
        throw new AppError('patch-base-mismatch', 'No base content on server.', { path: msg.path, requestId: reqId });
      }
      let result;
      try {
        result = applyPatchToContent(base, msg.patch, msg.baseHash, msg.resultHash);
      } catch (e) {
        metrics.patchesRejected++;
        throw e instanceof AppError
          ? new AppError(e.code, e.message, { path: msg.path, requestId: reqId })
          : new AppError('patch-rejected', e.message, { path: msg.path, requestId: reqId });
      }
      const byteSize = Buffer.byteLength(result, 'utf8');
      if (byteSize > CONFIG.MAX_FILE_SIZE) {
        throw new AppError('file-too-large', `Patched size exceeds limit.`, { path: msg.path, requestId: reqId });
      }
      metrics.patchesApplied++;
      applyFileContent(ws, msg.path, result, reqId, 'file-patch');
      break;
    }

    case 'file-delete': {
      if (ws.role !== 'server') {
        throw new AppError('permission-denied', 'Only server role can delete files.', { path: msg.path, requestId: reqId });
      }
      if (vcs.busy) {
        throw new AppError('vcs-busy', `VCS operation in progress: ${vcs.busyReason}`, { path: msg.path, requestId: reqId });
      }
      const v = validatePath(msg.path);
      if (!v.ok) throw new AppError('invalid-path', v.error, { path: msg.path, requestId: reqId });
      const removed = fileStore.delete(msg.path);
      if (!removed) return;

      log.info(`[-] ${ws.username} (server) deleted: ${msg.path}`);
      broadcast({
        type: 'file-delete',
        path: msg.path,
        senderId: ws.clientId,
        username: ws.username,
      }, ws);

      snapshots.schedule('file-delete');
      break;
    }

    case 'snapshot-request': {
      if (ws.role !== 'server') {
        throw new AppError('permission-denied', 'Only server role can manage snapshots.', { requestId: reqId });
      }
      log.info(`[S] ${ws.username} requested manual snapshot`);
      const entry = await snapshots.create('manual');
      if (entry) {
        broadcast({
          type: 'snapshot-created',
          snapshot: {
            name: entry.name,
            count: snapshots.counter,
            fileCount: entry.fileCount,
            size: entry.size,
            createdAt: entry.createdAt,
          },
        });
      } else {
        throw new AppError('snapshot-failed', 'Snapshot could not be created.', { requestId: reqId });
      }
      break;
    }

    case 'snapshot-restore': {
      if (ws.role !== 'server') {
        throw new AppError('permission-denied', 'Only server role can restore snapshots.', { requestId: reqId });
      }
      const nameCheck = validateSnapshotName(msg.name);
      if (!nameCheck.ok) throw new AppError('invalid-name', nameCheck.error, { requestId: reqId });
      if (snapshots.restoring) {
        throw new AppError('restore-busy', 'Another restore is in progress.', { requestId: reqId });
      }
      snapshots.restoring = true;

      try {
        log.info(`[S] ${ws.username} requested restore of "${nameCheck.value}"`);
        let restored;
        try {
          restored = await snapshots.loadSnapshot(nameCheck.value);
        } catch (e) {
          throw new AppError('restore-failed', e.message, { requestId: reqId });
        }

        fileStore.replaceAll(restored);
        metrics.snapshotsRestored++;

        const nameMatch = nameCheck.value.match(/^(.+)_\d+$/);
        if (nameMatch) {
          fileStore.projectName = nameMatch[1];
          if (CONFIG.ENABLE_VCS) {
            vcs.meta.projectName = nameMatch[1];
            vcs.saveMeta();
          }
          broadcast({ type: 'project-name', name: fileStore.projectName });
        }

        log.info(`[S] Restored ${restored.size} files from "${nameCheck.value}"`);

        broadcast({
          type: 'filesystem-replaced',
          files: fileStore.toArray(),
          source: `snapshot:${nameCheck.value}`,
        });

        broadcast({
          type: 'snapshot-restored',
          name: nameCheck.value,
          fileCount: restored.size,
        });

        snapshots.schedule('post-restore');
      } finally {
        snapshots.restoring = false;
      }
      break;
    }

    /* ─── VCS handlers ─── */
    case 'commit': {
      if (!CONFIG.ENABLE_VCS) throw new AppError('vcs-disabled', 'VCS is disabled on this server.', { requestId: reqId });
      if (ws.role !== 'server') {
        throw new AppError('permission-denied', 'Only server role can commit.', { requestId: reqId });
      }
      if (typeof msg.message !== 'string' || !msg.message.trim()) {
        throw new AppError('invalid-message', 'Commit message is required.', { requestId: reqId });
      }

      const result = vcs.createCommit(msg.message.trim(), ws.username);
      if (!result.ok) {
        throw new AppError('vcs-commit-failed', result.error, { requestId: reqId });
      }
      log.info(`[VCS] ${ws.username} committed #${result.commit.id}: "${msg.message}"`);
      break;
    }

    case 'commit-request-dirty': {
      if (!CONFIG.ENABLE_VCS) throw new AppError('vcs-disabled', 'VCS disabled', { requestId: reqId });
      if (ws.role !== 'server') {
        throw new AppError('permission-denied', 'Only server role can request dirty files.', { requestId: reqId });
      }
      const files = vcs.computeDirtyFiles();
      sendToClient(ws, { type: 'dirty-files', files });
      break;
    }

    case 'branch-create': {
      if (!CONFIG.ENABLE_VCS) throw new AppError('vcs-disabled', 'VCS disabled', { requestId: reqId });
      if (ws.role !== 'server') {
        throw new AppError('permission-denied', 'Only server role can create branches.', { requestId: reqId });
      }
      const nameCheck = validateBranchName(msg.name);
      if (!nameCheck.ok) throw new AppError('invalid-name', nameCheck.error, { requestId: reqId });

      const fromBranch = msg.from || vcs.meta.activeBranch;
      const autocommit = msg.autocommit !== false;

      const result = vcs.createBranch(nameCheck.value, fromBranch, ws.username, autocommit);
      if (!result.ok) {
        throw new AppError('vcs-branch-failed', result.error, { requestId: reqId });
      }

      broadcast({
        type: 'branch-created',
        branch: result.branch,
      });

      log.info(`[VCS] ${ws.username} created branch "${nameCheck.value}" from "${fromBranch}"`);
      break;
    }

    case 'branch-switch': {
      if (!CONFIG.ENABLE_VCS) throw new AppError('vcs-disabled', 'VCS disabled', { requestId: reqId });
      if (ws.role !== 'server') {
        throw new AppError('permission-denied', 'Only server role can switch branches.', { requestId: reqId });
      }
      const nameCheck = validateBranchName(msg.name);
      if (!nameCheck.ok) throw new AppError('invalid-name', nameCheck.error, { requestId: reqId });

      // Notify clients that a switch is starting (correct semantic name)
      broadcast({ type: 'branch-switch-started', from: vcs.meta.activeBranch, to: nameCheck.value });

      const result = vcs.switchBranch(nameCheck.value, ws.username);
      if (!result.ok) {
        throw new AppError('vcs-switch-failed', result.error, { requestId: reqId });
      }

      broadcast({
        type: 'branch-switched',
        name: result.name,
        commitId: result.commitId,
        files: result.files,
        branches: result.branches,
      });

      log.info(`[VCS] ${ws.username} switched to "${nameCheck.value}"`);
      break;
    }

    case 'branch-delete': {
      if (!CONFIG.ENABLE_VCS) throw new AppError('vcs-disabled', 'VCS disabled', { requestId: reqId });
      if (ws.role !== 'server') {
        throw new AppError('permission-denied', 'Only server role can delete branches.', { requestId: reqId });
      }
      const nameCheck = validateBranchName(msg.name);
      if (!nameCheck.ok) throw new AppError('invalid-name', nameCheck.error, { requestId: reqId });

      const result = vcs.deleteBranch(nameCheck.value);
      if (!result.ok) throw new AppError('vcs-branch-delete-failed', result.error, { requestId: reqId });

      broadcast({ type: 'branch-deleted', name: nameCheck.value });
      log.info(`[VCS] ${ws.username} deleted branch "${nameCheck.value}"`);
      break;
    }

    case 'branch-merge-preview': {
      if (!CONFIG.ENABLE_VCS) throw new AppError('vcs-disabled', 'VCS disabled', { requestId: reqId });
      if (ws.role !== 'server') {
        throw new AppError('permission-denied', 'Only server role can merge.', { requestId: reqId });
      }

      const result = vcs.mergePreview(msg.from, msg.to);
      if (!result.ok) throw new AppError('vcs-merge-preview-failed', result.error, { requestId: reqId });

      sendToClient(ws, {
        type: 'merge-preview',
        from: result.from,
        to: result.to,
        autoFiles: result.autoFiles,
        conflicts: result.conflicts,
      });
      break;
    }

    case 'branch-merge': {
      if (!CONFIG.ENABLE_VCS) throw new AppError('vcs-disabled', 'VCS disabled', { requestId: reqId });
      if (ws.role !== 'server') {
        throw new AppError('permission-denied', 'Only server role can merge.', { requestId: reqId });
      }

      broadcast({ type: 'merge-started', from: msg.from, to: msg.to });

      const result = vcs.mergeBranches(msg.from, msg.to, msg.resolutions || {}, ws.username);
      if (!result.ok) throw new AppError('vcs-merge-failed', result.error, { requestId: reqId });

      const payload = {
        type: 'merge-completed',
        commitId: result.commitId,
        branches: result.branches,
      };
      if (result.files) payload.files = result.files;

      broadcast(payload);
      log.info(`[VCS] ${ws.username} merged "${msg.from}" into "${msg.to}" -> commit #${result.commitId}`);
      break;
    }

    case 'commit-restore': {
      if (!CONFIG.ENABLE_VCS) throw new AppError('vcs-disabled', 'VCS disabled', { requestId: reqId });
      if (ws.role !== 'server') {
        throw new AppError('permission-denied', 'Only server role can restore commits.', { requestId: reqId });
      }
      const id = Number.parseInt(msg.id, 10);
      if (!Number.isFinite(id) || id <= 0) {
        throw new AppError('invalid-id', 'Commit id must be a positive integer.', { requestId: reqId });
      }

      const result = vcs.restoreCommit(id, ws.username);
      if (!result.ok) throw new AppError('vcs-restore-failed', result.error, { requestId: reqId });

      broadcast({
        type: 'commit-restored',
        id: result.id,
        newCommitId: result.newCommitId,
        files: result.files,
        activeBranch: result.activeBranch,
        branches: vcs._branchesAsArray(),
      });

      log.info(`[VCS] ${ws.username} restored commit #${id} -> new #${result.newCommitId}`);
      break;
    }

    case 'history-request': {
      if (!CONFIG.ENABLE_VCS) throw new AppError('vcs-disabled', 'VCS disabled', { requestId: reqId });
      const limit = typeof msg.limit === 'number' ? Math.max(1, Math.min(msg.limit, 1000)) : 200;
      const includeAuto = !!msg.includeAuto;
      sendToClient(ws, {
        type: 'history',
        commits: vcs.getHistory(limit, includeAuto),
      });
      break;
    }

    case 'ping': {
      sendToClient(ws, { type: 'pong', t: msg.t });
      break;
    }

    default:
      log.debug(`[?] Client #${ws.clientId} sent unknown message type: ${msg.type}`);
      break;
  }
}

/* ═══════════════════════════════════════════════════════════════════════
   Heartbeat
   ═══════════════════════════════════════════════════════════════════════ */

const heartbeatTimer = setInterval(() => {
  for (const ws of registry.clients) {
    if (ws.isAlive === false) {
      log.info(`[!] Client #${ws.clientId} (${ws.username}) unresponsive - terminating`);
      registry.unregister(ws);
      try { ws.terminate(); } catch {}
      broadcastClientCount();
      continue;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, CONFIG.HEARTBEAT_INTERVAL_MS);
if (heartbeatTimer.unref) heartbeatTimer.unref();

/* ═══════════════════════════════════════════════════════════════════════
   Graceful shutdown
   ═══════════════════════════════════════════════════════════════════════ */

let shuttingDown = false;

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;

  console.log(`\n[!] Received ${signal} - shutting down...`);

  clearInterval(heartbeatTimer);
  if (snapshots.timer) { clearTimeout(snapshots.timer); snapshots.timer = null; }
  vcs.stopAutoCommit();

  if (CONFIG.ENABLE_SNAPSHOTS && fileStore.projectName && fileStore.size > 0) {
    console.log('[S] Creating final snapshot before shutdown...');
    try {
      snapshots.inProgress = false;
      snapshots.lastAt = 0;
      await snapshots.create('shutdown');
    } catch (e) {
      log.error(`[S] Final snapshot failed: ${e.message}`);
    }
  }

  if (CONFIG.ENABLE_VCS && fileStore.projectName && fileStore.size > 0) {
    try {
      const dirty = vcs.computeDirtyFiles();
      if (dirty.length > 0) {
        console.log('[VCS] Creating final auto-commit before shutdown...');
        vcs.createCommit(`shutdown: ${new Date().toISOString()}`, 'shutdown', { auto: true, _internal: true, broadcast: false });
      }
    } catch (e) {
      log.error(`[VCS] Final commit failed: ${e.message}`);
    }
  }

  for (const ws of registry.clients) {
    try {
      sendToClient(ws, { type: 'error', code: 'shutdown', message: 'Server shutting down.' });
    } catch {}
    try { ws.close(1001, 'Server shutting down'); } catch {}
  }
  registry.clients.clear();
  registry.activeServerClient = null;

  await new Promise((resolve) => {
    wss.close(() => {
      httpServer.close(() => resolve());
    });
    setTimeout(resolve, 4000);
  });

  console.log('[!] Server stopped.');
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

process.on('uncaughtException', (err) => {
  log.error('[!] Uncaught exception:', err);
});

process.on('unhandledRejection', (err) => {
  log.error('[!] Unhandled rejection:', err);
});

/* ═══════════════════════════════════════════════════════════════════════
   Startup banner
   ═══════════════════════════════════════════════════════════════════════ */

function printBanner() {
  const ips = getLocalIps();
  const primaryIp = ips[0] || '127.0.0.1';
  const secretStatus = CONFIG.SERVER_SECRET
    ? 'required (CODESYNC_SERVER_SECRET)'
    : 'not set - first-come-first-served';
  const patchStatus = dmp ? 'enabled' : 'disabled - npm install diff-match-patch to enable';
  const zipStatus = ArchiverClass ? 'enabled' : 'disabled - npm install archiver to enable';
  const vcsStatus = CONFIG.ENABLE_VCS
    ? `enabled (auto-commit every ${Math.round(CONFIG.AUTO_COMMIT_INTERVAL_MS / 1000)}s)`
    : 'disabled';
  const vcsInfo = CONFIG.ENABLE_VCS ? {
    branches: Object.keys(vcs.meta.branches).length,
    commits: Object.keys(vcs.meta.commits).length,
    activeBranch: vcs.meta.activeBranch,
  } : null;

  const lines = [
    '',
    '  ====================================================================',
    `          CodeSync Broker v${SERVER_VERSION}  (protocol v${PROTOCOL_VERSION})`,
    '  ====================================================================',
    '',
    `  HTTP server:          http://localhost:${CONFIG.PORT}`,
    `  WebSocket (local):    ws://localhost:${CONFIG.PORT}/ws`,
    `  WebSocket (LAN):      ws://${primaryIp}:${CONFIG.PORT}/ws`,
    `  Health check:         http://localhost:${CONFIG.PORT}/health`,
    `  Snapshots list:       http://localhost:${CONFIG.PORT}/api/snapshots`,
    `  Manual snapshot:      POST http://localhost:${CONFIG.PORT}/api/snapshot`,
    `  Files list:           http://localhost:${CONFIG.PORT}/api/files`,
    `  ZIP download:         http://localhost:${CONFIG.PORT}/api/download`,
    `  VCS state:            http://localhost:${CONFIG.PORT}/api/vcs/state`,
    `  VCS history:          http://localhost:${CONFIG.PORT}/api/vcs/history`,
    '',
    `  Patch protocol:       ${patchStatus}`,
    `  ZIP download:         ${zipStatus}`,
    `  Capabilities:         ${SERVER_CAPABILITIES.length > 0 ? SERVER_CAPABILITIES.join(', ') : '(none)'}`,
    '',
    `  Snapshot directory:   ${CONFIG.SNAPSHOT_DIR}`,
    `  Snapshots enabled:    ${CONFIG.ENABLE_SNAPSHOTS ? 'yes' : 'no'}`,
    `  Restore from snap:    ${CONFIG.RESTORE_FROM_SNAPSHOT ? 'yes' : 'no'}`,
    `  Snapshot counter:     ${snapshots.counter}`,
    `  Snapshot history:     ${snapshots.history.length}`,
    '',
    `  VCS:                  ${vcsStatus}`,
    `  VCS directory:        ${CONFIG.VCS_DIR}`,
    `  VCS state:            ${vcsInfo ? `branch=${vcsInfo.activeBranch}, branches=${vcsInfo.branches}, commits=${vcsInfo.commits}` : 'N/A'}`,
    `  Max auto-commits:     ${CONFIG.MAX_AUTO_COMMITS} per branch`,
    '',
    `  Server role auth:     ${secretStatus}`,
    `  Max connections:      ${CONFIG.MAX_CONNECTIONS} (global) / ${CONFIG.MAX_CONNECTIONS_PER_IP} (per IP)`,
    `  Rate limit:           ${CONFIG.RATE_LIMIT_MAX_MESSAGES} msg / ${CONFIG.RATE_LIMIT_WINDOW_MS} ms`,
    '',
    '  Waiting for connections...',
    '',
  ];
  console.log(lines.join('\n'));

  if (!CONFIG.SERVER_SECRET) {
    console.warn('  WARNING: CODESYNC_SERVER_SECRET is not set.');
    console.warn('           Any client can claim the "server" role (first-come-first-served).');
    console.warn('           For production: export CODESYNC_SERVER_SECRET="random-string"');
    console.warn('');
  }
  if (!dmp) {
    console.warn('  WARNING: diff-match-patch is not installed.');
    console.warn('           Patch protocol disabled - clients use full-content sync.');
    console.warn('');
  }
  if (!ArchiverClass) {
    console.warn('  WARNING: archiver is not installed.');
    console.warn('           /api/download will return 501. Install: npm install archiver');
    console.warn('');
  }
}

/* ═══════════════════════════════════════════════════════════════════════
   Bootstrap
   ═══════════════════════════════════════════════════════════════════════ */

(function bootstrap() {
  snapshots.ensureDir();
  snapshots.scanExisting();

  const restored = snapshots.restoreFromLatest();
  if (!restored) {
    log.info('[S] No snapshot to restore - starting clean.');
  }

  if (CONFIG.ENABLE_VCS) {
    vcs.ensureDir();
    vcs.loadMeta();
    vcs.startAutoCommit();
  }

  httpServer.listen(CONFIG.PORT, CONFIG.HOST, () => {
    printBanner();
  });

  httpServer.on('error', (err) => {
    log.error(`[!] HTTP server error: ${err.message}`);
    if (err.code === 'EADDRINUSE') {
      log.error(`[!] Port ${CONFIG.PORT} is already in use. Try a different PORT.`);
    }
    process.exit(1);
  });
})();