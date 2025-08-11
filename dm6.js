#!/usr/bin/env node
/**
 * dm2_inplace.js
 *
 * A modification of the copy-safe script that operates *in-place* on the
 * provided target instead of creating a working copy. Use carefully — this
 * WILL modify files and folders you point it at.
 *
 * Usage:
 *   node dm2_inplace.js <target-file-or-folder> [output_assets_folder] [store_folder]
 *
 * Example (defaults inside target):
 *   node dm2_inplace.js ./cari
 *   -> uses ./cari/assets and ./cari/copy for outputs and stores
 *
 * To preserve originals, make a manual backup before running.
 */

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const crypto = require("crypto");
const axios = require("axios");
const FileType = require("file-type"); // npm package
// const cliProgress = require("cli-progress"); // removed per request
const pLimit = require("p-limit"); // simple concurrency helper

// ---------------- CONFIG (can be overridden via args) ----------------
const args = process.argv.slice(2);
if (args.length === 0) {
  console.error(
    "Usage: node dm2_inplace.js <target-file-or-folder> [output_assets_folder] [store_folder]"
  );
  process.exit(2);
}
const RAW_TARGET = args[0]; // json file or root folder (user-provided)
let TARGET = path.resolve(RAW_TARGET);
let OUTPUT_ROOT_ARG = args[1] ? path.resolve(process.cwd(), args[1]) : null;
let STORE_DIR_ARG = args[2] ? path.resolve(process.cwd(), args[2]) : null;

// We'll set OUTPUT_ROOT and STORE_DIR after determining the target
let OUTPUT_ROOT;
let STORE_DIR;

const URL_MAP_NAME = "urlMap.json";
const CONTENT_MAP_NAME = "contentMap.json";
const QUEUE_NAME = "queue.json";
const FAILED_NAME = "failed_downloads.json";

const GLOBAL_CONCURRENCY = 10;
const PER_FILE_CONCURRENCY = 10;
const CHUNK_SIZE = 1024 * 1024; // 1MB
const CHUNK_THRESHOLD = 1024 * 1024 * 1; // 1MB threshold for segmented downloads
const RETRY_LIMIT = 5;
const BACKOFF_BASE = 500; // ms
const TMP_SUFFIX = ".tmp";
// const PROGRESS_POLL_MS = 500; // removed (progress bar removed)
// Timeout for individual HTTP download requests (ms). Prevents infinite hangs.
const DOWNLOAD_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes

// ---------------------------------------------------------------------

const globalLimiter = pLimit(GLOBAL_CONCURRENCY);

// ------------------- Helpers -------------------
async function pathExists(p) {
  try {
    await fsp.access(p);
    return true;
  } catch (e) {
    return false;
  }
}

async function ensureDir(d) {
  await fsp.mkdir(d, { recursive: true });
}
function sha256hex(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}
function md5hexFileSync(bufferOrPath) {
  if (Buffer.isBuffer(bufferOrPath))
    return crypto.createHash("md5").update(bufferOrPath).digest("hex");
  const data = fs.readFileSync(bufferOrPath);
  return crypto.createHash("md5").update(data).digest("hex");
}
async function fileSha256(pathFile) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash("sha256");
    const rs = fs.createReadStream(pathFile);
    rs.on("data", (c) => hash.update(c));
    rs.on("end", () => resolve(hash.digest("hex")));
    rs.on("error", reject);
  });
}
async function fileMd5(pathFile) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash("md5");
    const rs = fs.createReadStream(pathFile);
    rs.on("data", (c) => hash.update(c));
    rs.on("end", () => resolve(hash.digest("hex")));
    rs.on("error", reject);
  });
}

function makeFileName(url, ext = "") {
  const date = new Date().toISOString().slice(0, 10).replace(/-/g, "");
  const h = sha256hex(url).slice(0, 16);
  return `nabilaba_${date}_${h}${ext || ""}`;
}

function extFromUrl(u) {
  try {
    const p = new URL(u).pathname;
    const last = path.basename(p).split("?")[0];
    if (last.includes(".")) {
      const e = path.extname(last).toLowerCase();
      if (e === ".image") return "";
      return e;
    }
  } catch (e) {}
  return "";
}

function detectFolderKind(url) {
  const lower = (url || "").toLowerCase();
  if (
    lower.match(/\.(jpg|jpeg|png|gif|webp|bmp|image)$/) ||
    lower.includes("image") ||
    lower.includes("ibytedimg")
  )
    return "images";
  if (
    lower.match(/\.(mp4|webm|mov|m4v)$/) ||
    lower.includes("video") ||
    lower.includes("tiktokcdn")
  )
    return "videos";
  if (lower.match(/\.(zip|rar|7z)$/)) return "archives";
  return "files";
}

// ---------------- Persistent stores ----------------
async function loadJsonIfExists(p, def = {}) {
  try {
    const s = await fsp.readFile(p, "utf8");
    return JSON.parse(s);
  } catch (e) {
    return def;
  }
}
const fileWriteLocks = new Map();

async function acquireFileLock(key) {
  while (fileWriteLocks.get(key)) {
    await new Promise((r) => setTimeout(r, 30));
  }
  fileWriteLocks.set(key, true);
}
function releaseFileLock(key) {
  fileWriteLocks.delete(key);
}

async function saveJsonAtomic(p, obj) {
  await ensureDir(path.dirname(p));
  const content = JSON.stringify(obj, null, 2);
  const maxAttempts = 5;
  let attempt = 0;
  const key = path.resolve(p);

  await acquireFileLock(key);
  try {
    while (attempt < maxAttempts) {
      attempt++;
      const rnd = Math.floor(Math.random() * 0xffff).toString(16);
      const tmp = `${p}.saving.${Date.now()}.${rnd}`;

      try {
        await fsp.writeFile(tmp, content, "utf8");
        await fsp.rename(tmp, p);
        return;
      } catch (err) {
        try {
          await fsp.unlink(tmp).catch(() => {});
        } catch (_) {}
        if (attempt >= maxAttempts) {
          throw err;
        }
        const backoff = 80 * attempt;
        await new Promise((r) => setTimeout(r, backoff));
      }
    }
  } finally {
    releaseFileLock(key);
  }
}

// ----------------- Preprocess & sanitize -----------------
function preprocessData(root) {
  let countCoverUrl = 0;
  let countTrackThumbnail = 0;
  let countVideoUrl = 0;
  let videoQuality = "";

  function walk(node) {
    if (Array.isArray(node)) {
      return node.map(walk);
    }
    if (node && typeof node === "object") {
      if (node.common_attr && typeof node.common_attr === "object") {
        if (
          node.common_attr.cover_url &&
          typeof node.common_attr.cover_url === "object"
        ) {
          const ca = node.common_attr.cover_url;
          if (ca.small && typeof ca.small === "string" && ca.small.trim()) {
            Object.keys(ca).forEach((key) => {
              if (typeof ca[key] === "string" && ca[key].startsWith("https")) {
                if (ca[key] !== ca.small) {
                  ca[key] = ca.small;
                  countCoverUrl++;
                }
              }
            });
          }
        }
      }

      if (node.sticker && typeof node.sticker === "object") {
        const st = node.sticker;
        if (
          st.large_image &&
          st.large_image.image_url &&
          st.large_image.image_url.trim()
        ) {
          if (st.track_thumbnail !== st.large_image.image_url) {
            st.track_thumbnail = st.large_image.image_url;
            countTrackThumbnail++;
          }
        }
      }

      if (node.digital_human && typeof node.digital_human === "object") {
        const replaceVideoUrls = (videoObj) => {
          if (
            videoObj &&
            videoObj.origin_video &&
            videoObj.origin_video.video_url
          ) {
            let highestQualityUrl = videoObj.origin_video.video_url.replace(
              /[^\u0000-\u007F]/g,
              ""
            );
            const videoQualities = ["360p", "480p", "540p", "720p", "1080p"];
            if (
              videoObj.transcoded_video &&
              typeof videoObj.transcoded_video === "object"
            ) {
              for (const quality of videoQualities.reverse()) {
                if (videoObj.transcoded_video[quality]) {
                  highestQualityUrl =
                    videoObj.transcoded_video[quality].video_url;
                  videoQuality = quality;
                  break;
                }
              }
            }
            if (highestQualityUrl) {
              for (const quality of videoQualities) {
                if (
                  videoObj.transcoded_video &&
                  videoObj.transcoded_video[quality]
                ) {
                  videoObj.transcoded_video[quality].video_url =
                    highestQualityUrl;
                  countVideoUrl++;
                }
              }
              videoObj.origin_video.video_url = highestQualityUrl;
              countVideoUrl++;
            }
          }
        };
        if (node.digital_human.preview_video)
          replaceVideoUrls(node.digital_human.preview_video);
        if (node.digital_human.mask_video)
          replaceVideoUrls(node.digital_human.mask_video);
      }

      for (const k of Object.keys(node)) {
        node[k] = walk(node[k]);
      }
      return node;
    }
    return node;
  }

  const out = walk(root);

  if (countCoverUrl > 0) {
    console.log(`🖼️ Replaced ${countCoverUrl} cover_url fields`);
  }
  if (countTrackThumbnail > 0) {
    console.log(`😃 Replaced ${countTrackThumbnail} track_thumbnail fields`);
  }
  if (countVideoUrl > 0) {
    console.log(
      `🎥 Replaced ${countVideoUrl} video_url fields with ${videoQuality}`
    );
  }

  return out;
}

function removeKeysRecursive(obj, keysToRemove = ["systime", "logid"]) {
  if (Array.isArray(obj)) {
    return obj.map((x) => removeKeysRecursive(x, keysToRemove));
  }
  if (obj && typeof obj === "object") {
    for (const k of Object.keys(obj)) {
      if (keysToRemove.includes(k)) {
        delete obj[k];
        continue;
      }
      obj[k] = removeKeysRecursive(obj[k], keysToRemove);
    }
    return obj;
  }
  return obj;
}

// ---------------- Networking helpers ----------------
async function probeUrl(url) {
  try {
    const res = await axios.head(url, { maxRedirects: 5, timeout: 15000 });
    return {
      contentLength: parseInt(res.headers["content-length"] || "0", 10),
      contentType: res.headers["content-type"] || "",
      acceptRanges:
        (res.headers["accept-ranges"] || "").toLowerCase() === "bytes",
    };
  } catch (e) {
    try {
      const r = await axios.get(url, {
        headers: { Range: "bytes=0-0" },
        responseType: "arraybuffer",
        maxRedirects: 5,
        timeout: 15000,
      });
      return {
        contentLength: parseInt(r.headers["content-length"] || "0", 10),
        contentType: r.headers["content-type"] || "",
        acceptRanges:
          (r.headers["accept-ranges"] || "").toLowerCase() === "bytes" ||
          r.status === 206,
      };
    } catch (_) {
      return { contentLength: 0, contentType: "", acceptRanges: false };
    }
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
async function retryWithBackoff(fn, attempts = RETRY_LIMIT) {
  let attempt = 0;
  while (attempt < attempts) {
    try {
      return await fn();
    } catch (err) {
      attempt++;
      if (attempt >= attempts) throw err;
      const backoff = BACKOFF_BASE * Math.pow(2, attempt - 1);
      console.warn(`Attempt ${attempt} failed, retrying in ${backoff}ms`);
      await sleep(backoff);
    }
  }
}

async function downloadRange(url, start, end, partPath) {
  return retryWithBackoff(async () => {
    console.log(
      `[downloadRange] start ${url} bytes=${start}-${end} -> ${partPath}`
    );
    await ensureDir(path.dirname(partPath));
    const res = await axios.get(url, {
      responseType: "stream",
      headers: { Range: `bytes=${start}-${end}` },
      maxRedirects: 5,
      timeout: DOWNLOAD_TIMEOUT_MS,
    });
    const writer = fs.createWriteStream(partPath, { flags: "w" });
    await new Promise((resolve, reject) => {
      res.data.on("error", (err) => {
        try {
          writer.destroy();
        } catch (_) {}
        reject(err);
      });
      writer.on("error", reject);
      writer.on("finish", resolve);
      res.data.pipe(writer);
    });
    console.log(`[downloadRange] done ${url} -> ${partPath}`);
    return true;
  }, RETRY_LIMIT);
}

async function combineParts(finalPath, parts) {
  await ensureDir(path.dirname(finalPath));
  const writer = fs.createWriteStream(finalPath, { flags: "w" });
  for (const p of parts) {
    await new Promise((res, rej) => {
      const rs = fs.createReadStream(p);
      rs.on("error", rej);
      rs.pipe(writer, { end: false });
      rs.on("end", res);
    });
  }
  writer.end();
  // wait for writer to flush to disk before removing parts
  await new Promise((resolve, reject) => {
    writer.on("finish", resolve);
    writer.on("error", reject);
  });
  for (const p of parts) {
    try {
      await fsp.unlink(p);
    } catch (e) {}
  }
}

async function detectExtensionFromBuffer(buffer) {
  const ft = await FileType.fromBuffer(buffer).catch(() => null);
  if (ft && ft.ext) return "." + ft.ext;
  return "";
}

// Progress bar removed: downloadWhole no longer accepts/uses a progress bar
async function downloadWhole(url, tmpPath, probe, currentJsonFile = "unknown") {
  return retryWithBackoff(async () => {
    console.log(
      `[${path.basename(
        currentJsonFile
      )}] [downloadWhole] start ${url} -> ${tmpPath}`
    );
    let start = 0;
    try {
      const st = await fsp.stat(tmpPath);
      start = st.size;
    } catch (e) {
      start = 0;
    }
    const headers = {};
    if (start > 0 && probe.acceptRanges) headers.Range = `bytes=${start}-`;
    else if (start > 0) {
      await fsp.unlink(tmpPath).catch(() => {});
      start = 0;
    }
    const res = await axios.get(url, {
      responseType: "stream",
      headers,
      maxRedirects: 5,
      timeout: DOWNLOAD_TIMEOUT_MS,
    });
    await ensureDir(path.dirname(tmpPath));
    const writer = fs.createWriteStream(tmpPath, {
      flags: start > 0 ? "a" : "w",
    });

    return await new Promise((resolve, reject) => {
      let downloaded = start;

      function cleanupAndReject(err) {
        reject(err);
      }

      res.data.on("data", (chunk) => {
        downloaded += chunk.length;
      });

      res.data.on("error", cleanupAndReject);
      writer.on("error", cleanupAndReject);

      writer.on("finish", () => {
        resolve();
      });

      res.data.pipe(writer);
    });
  }, RETRY_LIMIT);
}

function toWebPath(p) {
  if (!p) return p;
  return p.split(path.sep).join("/");
}

// ----------------- Persistent Queue helpers -----------------
async function loadQueue(queuePath) {
  const arr = await loadJsonIfExists(queuePath, []);
  return arr.map((e) => {
    if (!e.files) e.files = e.file ? [e.file] : [];
    if (typeof e.attempts !== "number") e.attempts = 0;
    return {
      url: e.url,
      files: Array.from(new Set(e.files || [])),
      attempts: e.attempts,
    };
  });
}
async function saveQueue(queue, queuePath) {
  await saveJsonAtomic(queuePath, queue);
}

// ----------------- Failed downloads helpers -----------------
async function loadFailed(failedPath) {
  return await loadJsonIfExists(failedPath, {});
}
async function saveFailed(obj, failedPath) {
  await saveJsonAtomic(failedPath, obj);
}
async function markFailedForFile(filePath, url, failedPath) {
  const failed = await loadFailed(failedPath);
  if (!failed[filePath]) failed[filePath] = [];
  if (!failed[filePath].includes(url)) failed[filePath].push(url);
  await saveFailed(failed, failedPath);
}

// ----------------- URL download workflow -----------------
async function downloadUrl(
  url,
  outDir,
  urlMap,
  contentMap,
  urlMapPath,
  contentMapPath,
  currentJsonFile = "unknown"
) {
  if (urlMap[url]) return urlMap[url];

  await ensureDir(outDir);
  const probe = await probeUrl(url);
  let ext = extFromUrl(url) || "";
  let filename = makeFileName(url, ext);
  let finalPath = path.join(outDir, filename);

  if (
    probe.acceptRanges &&
    probe.contentLength &&
    probe.contentLength > CHUNK_THRESHOLD
  ) {
    const total = probe.contentLength;
    const partCount = Math.max(1, Math.ceil(total / CHUNK_SIZE));
    const parts = [];
    const perFileLimiter = pLimit(PER_FILE_CONCURRENCY);
    for (let i = 0; i < partCount; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(total - 1, (i + 1) * CHUNK_SIZE - 1);
      const partPath = path.join(outDir, `${filename}.part.${i}`);
      parts.push(partPath);
      try {
        const st = await fsp.stat(partPath);
        const expected = end - start + 1;
        if (st.size === expected) continue;
        await fsp.unlink(partPath).catch(() => {});
      } catch (e) {}
      await perFileLimiter(() => downloadRange(url, start, end, partPath));
    }
    const tmpFinal = finalPath + TMP_SUFFIX;
    await combineParts(tmpFinal, parts);
    const headBuf = Buffer.alloc(8192);
    const fd = fs.openSync(tmpFinal, "r");
    const bytesRead = fs.readSync(fd, headBuf, 0, headBuf.length, 0);
    fs.closeSync(fd);
    let guessed = await detectExtensionFromBuffer(headBuf.slice(0, bytesRead));
    if (!guessed && probe.contentType) {
      if (probe.contentType.includes("png")) guessed = ".png";
      else if (probe.contentType.includes("jpeg")) guessed = ".jpg";
      else if (probe.contentType.includes("gif")) guessed = ".gif";
      else if (probe.contentType.includes("webp")) guessed = ".webp";
    }
    if (guessed && ext !== guessed) {
      ext = guessed;
      const newName = makeFileName(url, ext);
      finalPath = path.join(outDir, newName);
    }
    await fsp.rename(tmpFinal, finalPath);
  } else {
    const tmpFinal = finalPath + TMP_SUFFIX;
    // progressBar removed
    await downloadWhole(url, tmpFinal, probe, currentJsonFile);

    const head = Buffer.alloc(8192);
    const fd = fs.openSync(tmpFinal, "r");
    const bytesRead = fs.readSync(fd, head, 0, head.length, 0);
    fs.closeSync(fd);
    let guessed = await detectExtensionFromBuffer(head.slice(0, bytesRead));
    if (!guessed && probe.contentType) {
      if (probe.contentType.includes("png")) guessed = ".png";
      else if (probe.contentType.includes("jpeg")) guessed = ".jpg";
      else if (probe.contentType.includes("gif")) guessed = ".gif";
      else if (probe.contentType.includes("webp")) guessed = ".webp";
    }
    if (guessed && ext !== guessed) {
      ext = guessed;
      const newName = makeFileName(url, ext);
      finalPath = path.join(outDir, newName);
    }
    await fsp.rename(tmpFinal, finalPath);
  }

  const sha = await fileSha256(finalPath);
  const md5 = await fileMd5(finalPath);

  if (contentMap[sha]) {
    const existing = contentMap[sha];
    await fsp.unlink(finalPath).catch(() => {});
    urlMap[url] = existing;
    await saveJsonAtomic(urlMapPath, urlMap);
    return existing;
  } else {
    // Make the returned path relative to the parent of OUTPUT_ROOT so any enclosing
    // folder name (e.g. 'modif') is not included in the JSON paths.
    // Fallback to process.cwd() if OUTPUT_ROOT is not set.
    const baseForRel = OUTPUT_ROOT ? path.dirname(OUTPUT_ROOT) : process.cwd();
    const rel = toWebPath(path.relative(baseForRel, finalPath));
    contentMap[sha] = rel;
    urlMap[url] = rel;
    await saveJsonAtomic(contentMapPath, contentMap);
    await saveJsonAtomic(urlMapPath, urlMap);
    return rel;
  }
}

// ----------------- JSON scanning helpers -----------------
function findUrlsInObject(obj, keyPath = []) {
  const matches = [];
  const urlRegex = /^https?:\/\//i;
  if (Array.isArray(obj)) {
    obj.forEach((v, i) => {
      matches.push(...findUrlsInObject(v, keyPath.concat(String(i))));
    });
    return matches;
  }
  if (obj && typeof obj === "object") {
    for (const k of Object.keys(obj)) {
      const v = obj[k];
      if (typeof v === "string" && urlRegex.test(v.trim())) {
        matches.push({ path: keyPath.concat(k), url: v.trim() });
      } else if (Array.isArray(v)) {
        v.forEach((item, idx) => {
          if (typeof item === "string" && urlRegex.test(item.trim())) {
            matches.push({
              path: keyPath.concat(k, String(idx)),
              url: item.trim(),
            });
          } else {
            matches.push(
              ...findUrlsInObject(item, keyPath.concat(k, String(idx)))
            );
          }
        });
      } else if (v && typeof v === "object") {
        matches.push(...findUrlsInObject(v, keyPath.concat(k)));
      }
    }
  }
  return matches;
}
function setByPath(obj, pathArr, value) {
  let cur = obj;
  for (let i = 0; i < pathArr.length - 1; i++) {
    const k = pathArr[i];
    if (!(k in cur)) cur[k] = {};
    cur = cur[k];
  }
  cur[pathArr[pathArr.length - 1]] = value;
}

// ----------------- queue helpers -----------------
function ensureQueueHas(queue, url, filePath) {
  let entry = queue.find((q) => q.url === url);
  if (!entry) {
    entry = { url, files: filePath ? [filePath] : [], attempts: 0 };
    queue.push(entry);
  } else if (filePath && !entry.files.includes(filePath)) {
    entry.files.push(filePath);
  }
  return queue;
}

// ---------------- Process one JSON file -----------------
async function processJsonFile(filePath, urlMap, contentMap, queue, paths) {
  console.log("📄 Processing:", filePath);
  const text = await fsp.readFile(filePath, "utf8");
  let root;
  try {
    root = JSON.parse(text);
  } catch (e) {
    console.warn("Invalid JSON:", filePath);
    return;
  }

  root = preprocessData(root);
  root = removeKeysRecursive(root);

  const found = findUrlsInObject(root);
  console.log(`Found ${found.length} URL-like strings in ${filePath}`);

  const uniqueUrls = [];
  const seen = new Set();
  for (const f of found) {
    if (!seen.has(f.url)) {
      uniqueUrls.push({ url: f.url, positions: [] });
      seen.add(f.url);
    }
  }
  for (const f of found) {
    const entry = uniqueUrls.find((e) => e.url === f.url);
    if (entry) entry.positions.push(f.path);
  }

  for (const entry of uniqueUrls) {
    if (!urlMap[entry.url]) {
      ensureQueueHas(queue, entry.url, filePath);
    }
  }
  await saveQueue(queue, paths.queue);

  const promises = uniqueUrls.map((u) =>
    globalLimiter(async () => {
      if (urlMap[u.url]) {
        for (const pos of u.positions) setByPath(root, pos, urlMap[u.url]);
        return;
      }
      try {
        const outDir = path.join(OUTPUT_ROOT, detectFolderKind(u.url));
        const local = await downloadUrl(
          u.url,
          outDir,
          urlMap,
          contentMap,
          paths.urlMap,
          paths.contentMap,
          filePath
        );
        for (const pos of u.positions) setByPath(root, pos, local);
        const idx = queue.findIndex((q) => q.url === u.url);
        if (idx >= 0) {
          queue.splice(idx, 1);
          await saveQueue(queue, paths.queue);
        }
      } catch (err) {
        console.error(
          "Download failed for",
          u.url,
          err.message || err.toString()
        );
        const q = queue.find((x) => x.url === u.url);
        if (q) {
          q.attempts = (q.attempts || 0) + 1;
          if (q.attempts >= RETRY_LIMIT) {
            for (const f of q.files || []) {
              await markFailedForFile(f, u.url, paths.failed);
            }
            const idx = queue.findIndex((x) => x.url === u.url);
            if (idx >= 0) queue.splice(idx, 1);
          }
          await saveQueue(queue, paths.queue);
        } else {
          await markFailedForFile(filePath, u.url, paths.failed);
        }
      }
    })
  );
  await Promise.all(promises);

  // write updated JSON back to the target (in-place)
  await fsp.writeFile(filePath, JSON.stringify(root, null, 2), "utf8");
  console.log("Updated JSON file:", filePath);
}

// ---------------- Main -----------------
async function listJsonFiles(root) {
  const res = [];
  async function walk(dir) {
    const items = await fsp.readdir(dir, { withFileTypes: true });
    for (const it of items) {
      const full = path.join(dir, it.name);
      if (it.isDirectory()) await walk(full);
      else if (it.isFile() && it.name.toLowerCase().endsWith(".json"))
        res.push(full);
    }
  }
  const st = await fsp.stat(root);
  if (st.isDirectory()) await walk(root);
  else if (st.isFile()) res.push(root);

  // Sort files by size (largest first). If you prefer ascending, tell me and I can change it.
  const filesWithSize = await Promise.all(
    res.map(async (p) => {
      try {
        const s = await fsp.stat(p);
        return { path: p, size: s.size };
      } catch (e) {
        return { path: p, size: 0 };
      }
    })
  );
  filesWithSize.sort((a, b) => a.size - b.size);
  return filesWithSize.map((f) => f.path);
}

async function main() {
  console.log("RAW_TARGET", RAW_TARGET);

  // operate in-place on TARGET (no working copy created)
  if (!(await pathExists(TARGET))) {
    console.error("Target does not exist:", TARGET);
    process.exit(2);
  }
  const working = TARGET;
  console.log("Operating in-place on:", working);

  // If OUTPUT_ROOT / STORE_DIR were passed as args, use them (absolute). Otherwise
  // default them to be inside the target (assets & copy folders).
  OUTPUT_ROOT = OUTPUT_ROOT_ARG || path.join(working, "assets");
  STORE_DIR = STORE_DIR_ARG || path.join(working, "stats");

  // Ensure store & output directories exist
  await ensureDir(STORE_DIR);
  await ensureDir(OUTPUT_ROOT);

  const URL_MAP_PATH = path.join(STORE_DIR, URL_MAP_NAME);
  const CONTENT_MAP_PATH = path.join(STORE_DIR, CONTENT_MAP_NAME);
  const QUEUE_PATH = path.join(STORE_DIR, QUEUE_NAME);
  const FAILED_PATH = path.join(STORE_DIR, FAILED_NAME);

  console.log("OUTPUT_ROOT", OUTPUT_ROOT);
  console.log("STORE_DIR", STORE_DIR);
  console.log(
    "Config: concurrency",
    GLOBAL_CONCURRENCY,
    "retry_limit",
    RETRY_LIMIT
  );

  const urlMap = await loadJsonIfExists(URL_MAP_PATH, {});
  const contentMap = await loadJsonIfExists(CONTENT_MAP_PATH, {});
  const queue = await loadQueue(QUEUE_PATH);
  await loadFailed(FAILED_PATH);

  const files = await listJsonFiles(working);

  const paths = {
    urlMap: URL_MAP_PATH,
    contentMap: CONTENT_MAP_PATH,
    queue: QUEUE_PATH,
    failed: FAILED_PATH,
  };

  for (const f of files) {
    await processJsonFile(f, urlMap, contentMap, queue, paths);
  }

  console.log("Done.");
  console.log("Stores:", {
    urlMap: URL_MAP_PATH,
    contentMap: CONTENT_MAP_PATH,
    queue: QUEUE_PATH,
    failed: FAILED_PATH,
  });
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
