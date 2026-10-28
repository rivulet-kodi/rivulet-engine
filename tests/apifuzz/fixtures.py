# SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
#
# SPDX-License-Identifier: MIT
"""Offline fixtures for the stateful pass of scripts/api-fuzz.sh.

Stdlib only: it runs inside the pinned Schemathesis image. Three subcommands:

  build DIR        pack the media in DIR (clip.mp4/clip.ts, made by the engine
                   image's ffmpeg) into the formats the engine opens offline:
                   zip, tar, tgz, 7z, rar, plus an NZB, a subtitle file and a
                   local-files library.
  serve DIR        serve DIR over HTTP (:8080, Range supported) and the NZB's
                   articles from a fake NNTP server (:1119). Stays up until
                   killed.
  setup URL OUT    create, on the engine at URL, the sessions the stateful pass
                   addresses by fixed key, record the DVR fixtures (finished
                   recordings, and ones still running from /live.ts) and write
                   the dictionaries holding their (random) recording ids to
                   OUT/stateful/.

The 7z and rar archives are written here by hand (stored, no compression)
because no packer is available offline; both are listed and read back by the
engine's own readers, so a malformed one fails the setup, not the fuzz run.
"""

import http.server
import json
import os
import re
import shutil
import socketserver
import struct
import sys
import tarfile
import threading
import time
import urllib.error
import urllib.request
import zipfile
import zlib

HTTP_PORT = 8080
NNTP_PORT = 1119
SEGMENT = 4096  # bytes per NNTP article

README = b"rivulet-engine api-fuzz fixture\n"
SUBTITLE = b"1\n00:00:00,000 --> 00:00:01,000\nfuzz\n\n2\n00:00:01,000 --> 00:00:02,000\nclip\n"


# --- build ----------------------------------------------------------------------


def _vint(n):
    """RAR5 variable-length integer: 7 bits per byte, low group first."""
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _rar_block(header_type, flags, fields, data_size=None):
    body = _vint(header_type) + _vint(flags)
    if data_size is not None:
        body += _vint(data_size)
    body += fields
    sized = _vint(len(body)) + body
    return struct.pack("<I", zlib.crc32(sized)) + sized


def write_rar(path, files):
    """A RAR5 archive with stored (method 0) entries."""
    out = bytearray(b"Rar!\x1a\x07\x01\x00")
    out += _rar_block(1, 0, _vint(0))  # main header, no archive flags
    for name, data in files:
        raw = name.encode()
        fields = (
            _vint(0x04)  # file flags: CRC32 present
            + _vint(len(data))  # unpacked size
            + _vint(0o644)  # attributes
            + struct.pack("<I", zlib.crc32(data))
            + _vint(0)  # compression: version 0, store
            + _vint(1)  # host OS: unix
            + _vint(len(raw))
            + raw
        )
        out += _rar_block(2, 0x02, fields, data_size=len(data))
        out += data
    out += _rar_block(5, 0, _vint(0))  # end of archive
    with open(path, "wb") as f:
        f.write(out)


def _num7z(n):
    """7z variable-length number."""
    if n < 0x80:
        return bytes([n])
    for extra in range(1, 9):
        if n < 1 << (7 * (extra + 1)):
            first = (0xFF << (8 - extra)) & 0xFF | (n >> (8 * extra))
            return bytes([first]) + (n & ((1 << (8 * extra)) - 1)).to_bytes(extra, "little")
    return b"\xff" + n.to_bytes(8, "little")


def write_7z(path, files):
    """A 7z archive with one stored (Copy coder) folder per file."""
    packed = b"".join(data for _, data in files)
    h = bytearray(b"\x01\x04")  # Header, MainStreamsInfo
    h += b"\x06" + _num7z(0) + _num7z(len(files)) + b"\x09"  # PackInfo: sizes
    for _, data in files:
        h += _num7z(len(data))
    h += b"\x00"
    h += b"\x07\x0b" + _num7z(len(files)) + b"\x00"  # UnpackInfo, Folder, not external
    for _ in files:
        h += b"\x01\x01\x00"  # one coder: simple, id size 1, id 00 = Copy
    h += b"\x0c"  # CodersUnpackSize
    for _, data in files:
        h += _num7z(len(data))
    h += b"\x0a\x01" + b"".join(struct.pack("<I", zlib.crc32(d)) for _, d in files)
    h += b"\x00"  # end UnpackInfo
    h += b"\x08\x00"  # SubStreamsInfo: one substream per folder
    h += b"\x00"  # end StreamsInfo
    names = b"".join(n.encode("utf-16-le") + b"\x00\x00" for n, _ in files)
    h += b"\x05" + _num7z(len(files)) + b"\x11" + _num7z(len(names) + 1) + b"\x00" + names
    h += b"\x00\x00"  # end FilesInfo, end Header
    start = struct.pack("<QQI", len(packed), len(h), zlib.crc32(h))
    sig = b"7z\xbc\xaf\x27\x1c\x00\x04" + struct.pack("<I", zlib.crc32(start)) + start
    with open(path, "wb") as f:
        f.write(sig + packed + bytes(h))


def segments(size):
    """(number, begin, end) with 1-based inclusive byte offsets."""
    return [(i + 1, off + 1, min(off + SEGMENT, size)) for i, off in enumerate(range(0, size, SEGMENT))]


def message_id(n):
    return f"clip-{n}@fixtures.invalid"


def write_nzb(path, size):
    segs = "".join(
        f'      <segment bytes="{end - begin + 1}" number="{n}">{message_id(n)}</segment>\n'
        for n, begin, end in segments(size)
    )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">\n'
        '  <file poster="fuzz@fixtures.invalid" date="1700000000" '
        'subject="[1/1] - &quot;clip.mp4&quot; yEnc (1/%d)">\n'
        "    <groups><group>alt.binaries.fuzz</group></groups>\n"
        "    <segments>\n%s    </segments>\n  </file>\n</nzb>\n" % (len(segments(size)), segs)
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write(xml)


def build(root):
    with open(os.path.join(root, "clip.mp4"), "rb") as f:
        clip = f.read()
    files = [("clip.mp4", clip), ("readme.txt", README)]

    with zipfile.ZipFile(os.path.join(root, "clip.zip"), "w") as z:
        z.writestr(zipfile.ZipInfo("clip.mp4", (2020, 1, 1, 0, 0, 0)), clip, zipfile.ZIP_STORED)
        z.writestr(zipfile.ZipInfo("readme.txt", (2020, 1, 1, 0, 0, 0)), README, zipfile.ZIP_DEFLATED)
    for name, mode in (("clip.tar", "w"), ("clip.tgz", "w:gz")):
        with tarfile.open(os.path.join(root, name), mode) as t:
            for entry, data in files:
                info = tarfile.TarInfo(entry)
                info.size, info.mtime, info.mode = len(data), 1577836800, 0o644
                t.addfile(info, fileobj=_BytesReader(data))
    write_7z(os.path.join(root, "clip.7z"), files)
    write_rar(os.path.join(root, "clip.rar"), files)

    write_nzb(os.path.join(root, "clip.nzb"), len(clip))
    with open(os.path.join(root, "clip.srt"), "wb") as f:
        f.write(SUBTITLE)
    with open(os.path.join(root, "readme.txt"), "wb") as f:
        f.write(README)
    library = os.path.join(root, "library")
    os.makedirs(library, exist_ok=True)
    shutil.copyfile(os.path.join(root, "clip.mp4"), os.path.join(library, LIBRARY_FILE))


class _BytesReader:
    def __init__(self, data):
        self.data, self.pos = data, 0

    def read(self, n=-1):
        end = len(self.data) if n < 0 else self.pos + n
        chunk, self.pos = self.data[self.pos : end], min(end, len(self.data))
        return chunk


# The local-files library holds one video; the engine addresses it as
# sha256(abs path)[:8] (internal/api/localaddon.go localID). tests/apifuzz/stateful.toml
# lists that hash for the path below.
LIBRARY_FILE = "Fuzz Clip (2020).mp4"
LIBRARY_HASH = "99775d46aa43242f"  # sha256("/fixtures/library/Fuzz Clip (2020).mp4")[:8], hex


# --- serve ----------------------------------------------------------------------


def yenc_article(data, number, total, begin, end, size):
    """The BODY of one article: a yEnc part of clip.mp4."""
    part = data[begin - 1 : end]
    lines, line = [], bytearray()
    for b in part:
        c = (b + 42) & 0xFF
        # NUL, LF, CR and '=' must be escaped; so must '.', so no line starts
        # with a dot (NNTP dot-stuffing).
        enc = b"=" + bytes([(c + 64) & 0xFF]) if c in (0, 10, 13, 61, 46) else bytes([c])
        if len(line) + len(enc) > 128:
            lines.append(bytes(line))
            line = bytearray()
        line += enc
    if line:
        lines.append(bytes(line))
    head = [
        b"=ybegin part=%d total=%d line=128 size=%d name=clip.mp4" % (number, total, size),
        b"=ypart begin=%d end=%d" % (begin, end),
    ]
    tail = [b"=yend size=%d part=%d pcrc32=%08x" % (len(part), number, zlib.crc32(part))]
    return b"\r\n".join(head + lines + tail) + b"\r\n"


class NNTPHandler(socketserver.StreamRequestHandler):
    """Just enough NNTP for the engine: greeting, AUTHINFO, BODY, QUIT."""

    clip = b""
    segs = []

    def reply(self, text):
        self.wfile.write(text.encode() + b"\r\n")

    def handle(self):
        self.reply("200 fuzz fixture ready")
        for raw in self.rfile:
            line = raw.decode("latin-1").strip()
            cmd = line.split(" ", 1)[0].upper()
            if cmd == "AUTHINFO":
                self.reply("381 password required" if line.upper().startswith("AUTHINFO USER") else "281 ok")
            elif cmd == "QUIT":
                self.reply("205 bye")
                return
            elif cmd == "BODY":
                mid = line.split(" ", 1)[1].strip("<>") if " " in line else ""
                match = re.fullmatch(r"clip-(\d+)@fixtures\.invalid", mid)
                n = int(match.group(1)) if match else 0
                if not 1 <= n <= len(self.segs):
                    self.reply("430 no such article")
                    continue
                _, begin, end = self.segs[n - 1]
                self.reply("222 0 <%s>" % mid)
                self.wfile.write(yenc_article(self.clip, n, len(self.segs), begin, end, len(self.clip)))
                self.wfile.write(b".\r\n")
            else:
                self.reply("500 unknown command")


# One MPEG-TS null packet (PID 0x1fff): carries nothing, keeps the connection busy.
TS_NULL = b"\x47\x1f\xff\x10" + b"\xff" * 184
LIVE_SECONDS = 600  # how long /live.ts stays open: longer than a fuzz run


class RangeHandler(http.server.SimpleHTTPRequestHandler):
    """Static files with single-range support (ffmpeg and /proxy ask for ranges),
    plus /live.ts: clip.ts followed by null packets, a stream that does not end,
    so a recording of it stays active until the fuzzer stops it."""

    def log_message(self, *_):
        pass

    def do_GET(self):
        if self.path.split("?", 1)[0] != "/live.ts":
            return super().do_GET()
        with open(os.path.join(self.directory, "clip.ts"), "rb") as f:
            clip = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "video/mp2t")
        self.end_headers()
        try:
            self.wfile.write(clip)
            for _ in range(LIVE_SECONDS):
                self.wfile.write(TS_NULL * 4)
                self.wfile.flush()
                time.sleep(1)
        except OSError:
            pass
        return None

    def send_head(self):
        rng = self.headers.get("Range")
        path = self.translate_path(self.path)
        if not rng or not os.path.isfile(path):
            return super().send_head()
        m = re.fullmatch(r"bytes=(\d*)-(\d*)", rng.strip())
        size = os.path.getsize(path)
        if not m or not (m.group(1) or m.group(2)):
            return super().send_head()
        if m.group(1):
            begin = int(m.group(1))
            end = min(int(m.group(2)), size - 1) if m.group(2) else size - 1
        else:
            begin, end = max(size - int(m.group(2)), 0), size - 1
        if begin > end or begin >= size:
            self.send_response(416)
            self.send_header("Content-Range", f"bytes */{size}")
            self.end_headers()
            return None
        f = open(path, "rb")
        f.seek(begin)
        self.send_response(206)
        self.send_header("Content-Type", self.guess_type(path))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Range", f"bytes {begin}-{end}/{size}")
        self.send_header("Content-Length", str(end - begin + 1))
        self.end_headers()
        self._left = end - begin + 1
        return f

    def copyfile(self, source, outputfile):
        left = getattr(self, "_left", None)
        if left is None:
            return super().copyfile(source, outputfile)
        while left > 0:
            chunk = source.read(min(65536, left))
            if not chunk:
                break
            outputfile.write(chunk)
            left -= len(chunk)
        return None


class _Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def serve(root):
    with open(os.path.join(root, "clip.mp4"), "rb") as f:
        NNTPHandler.clip = f.read()
    NNTPHandler.segs = segments(len(NNTPHandler.clip))
    nntp = _Server(("0.0.0.0", NNTP_PORT), NNTPHandler)
    threading.Thread(target=nntp.serve_forever, daemon=True).start()
    handler = lambda *a, **kw: RangeHandler(*a, directory=root, **kw)  # noqa: E731
    web = http.server.ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), handler)
    print(f"fixtures: http :{HTTP_PORT}, nntp :{NNTP_PORT}", flush=True)
    web.serve_forever()


# --- setup ----------------------------------------------------------------------

FIXTURES = f"http://fixtures:{HTTP_PORT}"
ORIGIN = "https://web.stremio.com"


def call(base, method, path, body=None, expect=(200,)):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=data, method=method)
    req.add_header("Origin", ORIGIN)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            status, payload = resp.status, resp.read()
    except urllib.error.HTTPError as err:
        status, payload = err.code, err.read()
    if status not in expect:
        sys.exit(f"setup: {method} {path} answered {status}, expected {expect}: {payload[:300]!r}")
    return payload


def wait_recording(base, rec_id):
    for _ in range(60):
        rec = json.loads(call(base, "GET", f"/api/recordings/{rec_id}"))
        if rec["status"] not in ("starting", "recording"):
            return rec
        time.sleep(1)
    sys.exit(f"setup: recording {rec_id} did not finish")


def start_live_recording(base, name):
    """A recording that stays active: its source never ends. The query keeps the
    sources distinct, as the recorder refuses a second recording of a running one."""
    rec = json.loads(
        call(base, "POST", "/api/recordings/start", {"url": f"{FIXTURES}/live.ts?n={name}", "name": name}, expect=(201,))
    )
    # The size is only known once a recording ends; a recorder that cannot reach
    # its source flips to "failed" within a moment, so "still recording" after a
    # few seconds means ffmpeg is reading the stream.
    time.sleep(4)
    rec = json.loads(call(base, "GET", f"/api/recordings/{rec['id']}"))
    if rec["status"] != "recording":
        sys.exit(f"setup: live recording {rec['id']} is not recording: {rec}")
    return rec["id"]


def start_recording(base, name):
    rec = json.loads(
        call(base, "POST", "/api/recordings/start", {"url": f"{FIXTURES}/clip.ts", "name": name}, expect=(201,))
    )
    done = wait_recording(base, rec["id"])
    size = done.get("file_size_bytes") or 0
    if done["status"] != "completed" or not size:
        sys.exit(f"setup: recording {rec['id']} ended as {done['status']} with {size} bytes: {done}")
    return rec["id"]


def write_dictionary(path, values):
    with open(path, "w", encoding="utf-8") as f:
        for v in values:
            f.write(f'"{v}"\n')


def setup(base, out):
    base = base.rstrip("/")
    call(base, "POST", "/settings", {"localAddonEnabled": True})

    for ext, fname in (("zip", "clip.zip"), ("tar", "clip.tar"), ("tgz", "clip.tgz"), ("7zip", "clip.7z"), ("rar", "clip.rar")):
        key = json.loads(call(base, "POST", f"/{ext}/create/fuzz-{ext}", {"url": f"/fixtures/{fname}"}))["key"]
        if key != f"fuzz-{ext}":
            sys.exit(f"setup: /{ext}/create answered key {key!r}")
        # Reading the entry back proves the archive is well formed.
        body = call(base, "GET", f"/{ext}/stream/{key}/readme.txt")
        if body != README:
            sys.exit(f"setup: /{ext}/stream/{key}/readme.txt returned {body!r}")

    nzb = {"servers": [f"nntp://fixtures:{NNTP_PORT}"], "nzbUrl": f"{FIXTURES}/clip.nzb"}
    call(base, "POST", "/nzb/create/fuzz-nzb", nzb)
    clip = call(base, "GET", "/nzb/stream/fuzz-nzb/clip.mp4")
    with urllib.request.urlopen(f"{FIXTURES}/clip.mp4", timeout=30) as resp:
        if clip != resp.read():
            sys.exit("setup: /nzb/stream/fuzz-nzb/clip.mp4 differs from the fixture it was built from")

    master = call(base, "GET", f"/hlsv2/fuzz-hls/master.m3u8?mediaURL={FIXTURES}/clip.mp4")
    if b"#EXTM3U" not in master:
        sys.exit(f"setup: /hlsv2/fuzz-hls/master.m3u8 is not a playlist: {master[:80]!r}")

    catalog = json.loads(call(base, "GET", "/local-addon/catalog/movie/localmovies.json"))
    ids = [m.get("id") for m in catalog.get("metas", [])]
    if ids != [f"local:{LIBRARY_HASH}"]:
        sys.exit(f"setup: the local-files catalog lists {ids}, expected local:{LIBRARY_HASH} (stateful.toml)")

    keep = [start_recording(base, "fuzz-keep")]
    drop = [start_recording(base, f"fuzz-drop-{i}") for i in range(1, 5)]
    live = [start_live_recording(base, f"fuzz-live-{i}") for i in range(1, 5)]
    stateful = os.path.join(out, "stateful")
    os.makedirs(stateful, exist_ok=True)
    write_dictionary(os.path.join(stateful, "recordings-keep.dict"), keep)
    write_dictionary(os.path.join(stateful, "recordings-drop.dict"), drop)
    write_dictionary(os.path.join(stateful, "recordings-live.dict"), live)
    print(f"setup: sessions fuzz-zip/tar/tgz/7zip/rar, fuzz-nzb, fuzz-hls; recordings keep={keep} drop={drop} live={live}")


def main(argv):
    if len(argv) == 3 and argv[1] == "build":
        build(argv[2])
    elif len(argv) == 3 and argv[1] == "serve":
        serve(argv[2])
    elif len(argv) == 4 and argv[1] == "setup":
        setup(argv[2], argv[3])
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv)
