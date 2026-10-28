// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package streamproxy

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
)

func init() {
	// serveStream owns the freshly read segment it hands over, so the hook
	// decrypts in place rather than allocating a second full-size buffer.
	segmentDecryptor = drmDecryptInPlace
}

// aesBlockCache caches AES cipher.Block values keyed by the raw key bytes (as string).
// AES cipher.Block (aes.aesCipher) is goroutine-safe for concurrent Encrypt calls, so
// sharing one block across requests avoids repeated key-scheduling (~176 bytes of work
// per 16-byte key) for streams that reuse the same key across many segments.
//
// aesBlockMaxEntries bounds aesBlockCache; a client sending many distinct
// DRM keys must not grow server memory without limit. There is no natural
// TTL for a key→cipher.Block mapping, so once the cap is hit the whole
// cache is cleared rather than tracking per-entry recency for a cache this
// small and low-value — the next lookup simply re-derives the block.
const aesBlockMaxEntries = 256

var (
	aesBlockMu    sync.Mutex
	aesBlockCache = make(map[string]cipher.Block)
)

// cachedAESBlock returns a cached AES cipher.Block for key, creating and storing it on
// first use.  Subsequent calls with the same key bytes skip key scheduling entirely.
func cachedAESBlock(key []byte) (cipher.Block, error) {
	k := string(key)
	aesBlockMu.Lock()
	if b, ok := aesBlockCache[k]; ok {
		aesBlockMu.Unlock()
		return b, nil
	}
	aesBlockMu.Unlock()

	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	aesBlockMu.Lock()
	defer aesBlockMu.Unlock()
	if existing, ok := aesBlockCache[k]; ok {
		return existing, nil // another goroutine won the race
	}
	if len(aesBlockCache) >= aesBlockMaxEntries {
		aesBlockCache = make(map[string]cipher.Block)
	}
	aesBlockCache[k] = b
	return b, nil
}

// drmSubsample describes a single subsample unit inside a CENC-protected sample.
// Clear bytes are copied verbatim; Encrypted bytes are decrypted with AES-CTR.
type drmSubsample struct {
	Clear     int
	Encrypted int
}

// drmBox represents one ISO BMFF box parsed from a byte slice.
type drmBox struct {
	Type    string // 4-char code, or "uuid" with 16-byte usertype suffix
	Start   int    // byte offset of the box start in the outer slice
	HdrSize int    // size of the box header (size field + type + optional largesize + optional uuid)
	Size    int    // total box size in bytes (0 = to EOF, resolved on parse)
	Payload []byte // box body (after header)
}

// drmDecryptInPlace is drmDecrypt for a caller that owns segment and no
// longer needs the ciphertext: it decrypts inside segment's own backing array
// instead of allocating a full-size copy. The returned slice aliases segment
// (CBC additionally trims the PKCS7 padding); segment must not be used again,
// including after an error, as it may be partially decrypted.
//
// In-place is safe for every supported method: CBC decrypts with dst == src
// (cipher.BlockMode permits exact overlap), and the CENC/CBCS path already
// did all of its parsing and rewriting on a private copy of the segment, so
// pointing it at segment itself changes nothing but whose bytes get rewritten.
func drmDecryptInPlace(h *Handler, p DecryptParams, segment []byte) ([]byte, error) {
	return drmDecryptBuf(h, p, segment, true)
}

// drmDecryptBuf implements drmDecrypt/drmDecryptInPlace.
func drmDecryptBuf(_ *Handler, p DecryptParams, segment []byte, inPlace bool) ([]byte, error) {
	method := strings.TrimSpace(strings.ToUpper(p.Method))
	switch method {
	case "":
		return segment, nil

	case "AES-128":
		if len(p.Key) != 16 {
			return nil, fmt.Errorf("AES-128: key must be 16 bytes, got %d", len(p.Key))
		}
		if len(p.IV) != 16 {
			return nil, fmt.Errorf("AES-128: IV must be 16 bytes, got %d", len(p.IV))
		}
		return drmCBC(p.Key, p.IV, segment, inPlace)

	case "SAMPLE-AES":
		return nil, fmt.Errorf("SAMPLE-AES decryption is not supported")

	case "CENC", "CBCS":
		if len(p.Key) != 16 {
			return nil, fmt.Errorf("%s: key must be 16 bytes, got %d", method, len(p.Key))
		}
		return drmDecryptCENC(method, p.Key, p.IV, segment, inPlace)

	default:
		return nil, fmt.Errorf("unsupported decryption method %q", p.Method)
	}
}

// drmCBC is drmDecryptCBC with the option to decrypt inside data itself.
func drmCBC(key, iv, data []byte, inPlace bool) ([]byte, error) {
	if len(key) != 16 {
		return nil, fmt.Errorf("CBC: key must be 16 bytes, got %d", len(key))
	}
	if len(iv) != 16 {
		return nil, fmt.Errorf("CBC: IV must be 16 bytes, got %d", len(iv))
	}
	if len(data) == 0 {
		return nil, errors.New("CBC: empty segment")
	}
	if len(data)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("CBC: segment length %d is not a multiple of 16", len(data))
	}
	block, err := cachedAESBlock(key) // F2: reuse cached block; avoids key-scheduling per segment
	if err != nil {
		return nil, fmt.Errorf("CBC: %w", err)
	}
	out := data
	if !inPlace {
		out = make([]byte, len(data))
		copy(out, data)
	}
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(out, out)
	return drmStripPKCS7(out)
}

// drmStripPKCS7 validates and removes PKCS7 padding from a decrypted block.
func drmStripPKCS7(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, errors.New("CBC: empty plaintext after decrypt")
	}
	pad := int(b[len(b)-1])
	if pad == 0 || pad > aes.BlockSize {
		return nil, fmt.Errorf("CBC: invalid PKCS7 padding byte %d", pad)
	}
	if pad > len(b) {
		return nil, fmt.Errorf("CBC: padding length %d exceeds data length %d", pad, len(b))
	}
	// Validate all padding bytes are equal.
	for i := len(b) - pad; i < len(b); i++ {
		if b[i] != byte(pad) {
			return nil, fmt.Errorf("CBC: invalid PKCS7 padding at byte %d", i)
		}
	}
	return b[:len(b)-pad], nil
}

// ---------------------------------------------------------------------------
// CENC fMP4 decryption (AES-CTR for CENC/default; AES-CBC subsample for CBCS)
// ---------------------------------------------------------------------------

// drmDecryptCENC walks an fMP4 segment, locates moof/traf boxes, reads senc/trun/tfhd,
// and decrypts the corresponding mdat ranges in place. With inPlace the ranges are
// rewritten inside segment itself; otherwise inside a private copy.
func drmDecryptCENC(method string, key, iv []byte, segment []byte, inPlace bool) ([]byte, error) {
	out := segment
	if !inPlace {
		out = make([]byte, len(segment))
		copy(out, segment)
	}

	boxes, err := drmParseBoxes(out)
	if err != nil {
		return nil, fmt.Errorf("CENC: box parse: %w", err)
	}
	tenc := drmFindTenc(boxes)

	// Collect moof boxes; each moof is paired with the immediately-following mdat.
	for i, box := range boxes {
		if box.Type != "moof" {
			continue
		}
		// Find following mdat.
		mdatIdx := -1
		for j := i + 1; j < len(boxes); j++ {
			if boxes[j].Type == "mdat" {
				mdatIdx = j
				break
			}
		}
		if mdatIdx < 0 {
			return nil, errors.New("CENC: moof without following mdat")
		}
		mdat := boxes[mdatIdx]
		mdatBodyStart := mdat.Start + mdat.HdrSize
		mdatData := out[mdatBodyStart : mdat.Start+mdat.Size]

		if err := drmDecryptMoof(method, key, iv, tenc, box, mdatData, mdatBodyStart); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// drmDecryptMoof decrypts all samples in one moof box's traf children.
// mdatBodyStart is the absolute byte offset of the first mdat data byte in the segment.
func drmDecryptMoof(method string, key, iv []byte, tenc drmTenc, moof drmBox, mdatData []byte, mdatBodyStart int) error {
	children, err := drmParseBoxes(moof.Payload)
	if err != nil {
		return fmt.Errorf("CENC: moof children: %w", err)
	}

	moofBase := moof.Start

	for _, child := range children {
		if child.Type != "traf" {
			continue
		}
		trafBoxes, err := drmParseBoxes(child.Payload)
		if err != nil {
			return fmt.Errorf("CENC: traf children: %w", err)
		}
		if err := drmDecryptTraf(method, key, iv, tenc, trafBoxes, moofBase, mdatData, mdatBodyStart); err != nil {
			return err
		}
	}
	return nil
}

// drmTfhd holds the fields we care about from a Track Fragment Header box.
type drmTfhd struct {
	baseDataOffsetPresent        bool
	defaultBaseIsMoof            bool
	defaultSampleSizePresent     bool
	defaultSampleDurationPresent bool
	defaultSampleFlagsPresent    bool
	baseDataOffset               uint64
	defaultSampleSize            uint32
	defaultSampleDuration        uint32
}

// drmTrunSample holds per-sample values from trun.
type drmTrunSample struct {
	Duration  uint32
	Size      uint32
	Flags     uint32
	CTSOffset int32
}

// drmSencEntry holds per-sample IV and optional subsamples from senc.
type drmSencEntry struct {
	IV         []byte
	Subsamples []drmSubsample
}

// Default cbcs pattern (ISO/IEC 23001-7 §10.4 / CMAF) used for video when the
// segment carries no tenc box: encrypt 1 block, skip 9.
const (
	drmDefaultCryptBlocks = 1
	drmDefaultSkipBlocks  = 9
)

// drmTenc holds the fields of a Track Encryption Box (tenc) that influence
// decryption.  found is false when the segment carried no tenc (media
// segments usually omit moov), in which case all accessors return neutral
// "unknown" values.
type drmTenc struct {
	found           bool
	hasPattern      bool // tenc version >= 1: crypt/skip byte blocks are present
	crypt, skip     int
	perSampleIVSize int
	constIV         []byte
}

// ivFallback returns the IV to use for samples that carry no per-sample IV:
// the tenc default constant IV when present, otherwise the caller-supplied iv.
func (t drmTenc) ivFallback(iv []byte) []byte {
	if t.found && len(t.constIV) > 0 {
		return t.constIV
	}
	return iv
}

// ivSizeHint returns the tenc default_Per_Sample_IV_Size, or -1 if unknown.
func (t drmTenc) ivSizeHint() int {
	if !t.found {
		return -1
	}
	return t.perSampleIVSize
}

// pattern returns the cbcs crypt/skip byte-block pattern for a sample.  The
// tenc values win when present.  Without a tenc, samples with subsample
// tables (video) use the 1:9 default; whole-sample-encrypted data (audio)
// has no pattern (crypt=skip=0 means every full block is encrypted).
func (t drmTenc) pattern(hasSubsamples bool) (crypt, skip int) {
	if t.found && t.hasPattern {
		return t.crypt, t.skip
	}
	if hasSubsamples {
		return drmDefaultCryptBlocks, drmDefaultSkipBlocks
	}
	return 0, 0
}

// drmParseTenc parses a tenc box payload (version/flags included).
func drmParseTenc(p []byte) (drmTenc, error) {
	// version(1) flags(3) reserved(1) pattern/reserved(1) isProtected(1)
	// perSampleIVSize(1) KID(16) [constIVSize(1) constIV(n)]
	if len(p) < 24 {
		return drmTenc{}, errors.New("tenc too short")
	}
	t := drmTenc{found: true}
	if p[0] >= 1 {
		t.hasPattern = true
		t.crypt = int(p[5] >> 4)
		t.skip = int(p[5] & 0x0f)
	}
	isProtected := p[6]
	t.perSampleIVSize = int(p[7])
	if isProtected == 1 && t.perSampleIVSize == 0 {
		if len(p) < 25 {
			return drmTenc{}, errors.New("tenc: missing constant IV size")
		}
		n := int(p[24])
		if n != 8 && n != 16 || len(p) < 25+n {
			return drmTenc{}, fmt.Errorf("tenc: invalid constant IV size %d", n)
		}
		t.constIV = append([]byte(nil), p[25:25+n]...)
	}
	return t, nil
}

// drmFindTenc locates the first tenc box inside the sample descriptions
// (stsd → encv/enca → sinf → schi → tenc) of any moov found in boxes.  It
// returns a zero drmTenc (found == false) when none is present or parseable.
func drmFindTenc(boxes []drmBox) drmTenc {
	for _, b := range boxes {
		if b.Type != "stsd" {
			continue
		}
		pl := b.Payload
		// Sample entries have a fixed-size prefix whose length depends on the
		// handler, so locate the sinf box by its type tag and validate that it
		// begins with a frma child.
		for i := 4; i+4 <= len(pl); i++ {
			if string(pl[i:i+4]) != "sinf" {
				continue
			}
			start := i - 4
			size := int(binary.BigEndian.Uint32(pl[start:]))
			if size < 8+8 || start+size > len(pl) || string(pl[start+12:start+16]) != "frma" {
				continue
			}
			children, err := drmParseBoxes(pl[start+8 : start+size])
			if err != nil {
				continue
			}
			for _, c := range children {
				if c.Type == "tenc" {
					if t, err := drmParseTenc(c.Payload); err == nil {
						return t
					}
				}
			}
		}
	}
	return drmTenc{}
}

func drmDecryptTraf(method string, key, iv []byte, tenc drmTenc, trafBoxes []drmBox, moofStart int, mdatData []byte, mdatBodyStart int) error {
	var tfhd drmTfhd
	var samples []drmTrunSample
	var sencEntries []drmSencEntry
	hasTfhd := false
	hasTrun := false
	hasSenc := false
	dataOffset := int64(0) // trun data-offset; 0 when flag absent

	for _, b := range trafBoxes {
		switch b.Type {
		case "tfhd":
			var err error
			tfhd, err = drmParseTfhd(b.Payload)
			if err != nil {
				return fmt.Errorf("CENC: tfhd: %w", err)
			}
			hasTfhd = true

		case "trun":
			var err error
			var off int32
			samples, off, err = drmParseTrun(b.Payload)
			if err != nil {
				return fmt.Errorf("CENC: trun: %w", err)
			}
			dataOffset = int64(off)
			hasTrun = true

		case "senc":
			var err error
			sencEntries, err = drmParseSenc(b.Payload, tenc.ivFallback(iv), tenc.ivSizeHint())
			if err != nil {
				return fmt.Errorf("CENC: senc: %w", err)
			}
			hasSenc = true

		case "saiz", "saio":
			// senc is preferred; if absent, we cannot proceed (handled below).
		}
	}

	if !hasTfhd || !hasTrun {
		return nil
	}
	if !hasSenc {
		return errors.New("unsupported CENC layout: senc box absent, saiz/saio not supported")
	}

	// Resolve the absolute base for sample data per ISO 14496-12 §8.8.8:
	//   base-data-offset-present → explicit absolute file offset.
	//   default-base-is-moof (or implicit for first traf) → start of enclosing moof.
	// trun.data_offset is a signed offset relative to that base.
	// mdatBodyStart is the absolute position of mdatData[0] in the segment buffer.
	var base int64
	if tfhd.baseDataOffsetPresent {
		base = int64(tfhd.baseDataOffset)
	} else {
		// Both default-base-is-moof and the implicit-first-traf rule set the base
		// to the start of the enclosing moof box.
		base = int64(moofStart)
	}

	sampleStart := base + dataOffset - int64(mdatBodyStart)
	if sampleStart < 0 {
		return fmt.Errorf("CENC: computed sample start %d is before mdat body (base=%d, dataOffset=%d, mdatBodyStart=%d)",
			sampleStart, base, dataOffset, mdatBodyStart)
	}

	if len(sencEntries) != len(samples) {
		return fmt.Errorf("CENC: senc entry count %d != trun sample count %d", len(sencEntries), len(samples))
	}

	pos := sampleStart
	for i, samp := range samples {
		size := int64(samp.Size)
		if size == 0 && tfhd.defaultSampleSizePresent {
			size = int64(tfhd.defaultSampleSize)
		}
		if size == 0 {
			continue
		}
		if pos+size > int64(len(mdatData)) {
			return fmt.Errorf("CENC: sample %d at offset %d size %d exceeds mdat length %d",
				i, pos, size, len(mdatData))
		}

		entry := sencEntries[i]
		if len(entry.IV) == 0 {
			return fmt.Errorf("CENC: sample %d has no IV (per-sample IV size 0 and no constant IV available)", i)
		}
		sampData := mdatData[pos : pos+size]

		var decErr error
		if method == "CBCS" {
			crypt, skip := tenc.pattern(len(entry.Subsamples) > 0)
			decErr = drmDecryptCBCSubsamples(key, entry.IV, sampData, entry.Subsamples, crypt, skip)
		} else {
			// AES-CTR: decrypt in place; drmDecryptCTRSubsamples XORs sampData directly.
			_, decErr = drmDecryptCTRSubsamples(key, entry.IV, sampData, entry.Subsamples)
		}
		if decErr != nil {
			return fmt.Errorf("CENC: sample %d decrypt: %w", i, decErr)
		}

		pos += size
	}
	return nil
}

// drmParseTfhd parses the Track Fragment Header box payload.
func drmParseTfhd(p []byte) (drmTfhd, error) {
	// version(1) + flags(3) = 4 bytes, then track_ID(4).
	if len(p) < 8 {
		return drmTfhd{}, errors.New("tfhd too short")
	}
	version := p[0]
	_ = version
	flags := uint32(p[1])<<16 | uint32(p[2])<<8 | uint32(p[3])
	off := 4
	// track_ID
	if len(p) < off+4 {
		return drmTfhd{}, errors.New("tfhd: missing track_ID")
	}
	off += 4

	var tfhd drmTfhd
	const (
		tfhdBaseDataOffsetPresent         = 0x000001
		tfhdSampleDescriptionIndexPresent = 0x000002
		tfhdDefaultSampleDurationPresent  = 0x000008
		tfhdDefaultSampleSizePresent      = 0x000010
		tfhdDefaultSampleFlagsPresent     = 0x000020
		tfhdDurationIsEmpty               = 0x010000
		tfhdDefaultBaseIsMoof             = 0x020000
	)

	tfhd.defaultBaseIsMoof = flags&tfhdDefaultBaseIsMoof != 0

	if flags&tfhdBaseDataOffsetPresent != 0 {
		if len(p) < off+8 {
			return drmTfhd{}, errors.New("tfhd: missing base_data_offset")
		}
		tfhd.baseDataOffsetPresent = true
		tfhd.baseDataOffset = binary.BigEndian.Uint64(p[off:])
		off += 8
	}
	if flags&tfhdSampleDescriptionIndexPresent != 0 {
		if len(p) < off+4 {
			return drmTfhd{}, errors.New("tfhd: missing sample_description_index")
		}
		off += 4
	}
	if flags&tfhdDefaultSampleDurationPresent != 0 {
		if len(p) < off+4 {
			return drmTfhd{}, errors.New("tfhd: missing default_sample_duration")
		}
		tfhd.defaultSampleDurationPresent = true
		tfhd.defaultSampleDuration = binary.BigEndian.Uint32(p[off:])
		off += 4
	}
	if flags&tfhdDefaultSampleSizePresent != 0 {
		if len(p) < off+4 {
			return drmTfhd{}, errors.New("tfhd: missing default_sample_size")
		}
		tfhd.defaultSampleSizePresent = true
		tfhd.defaultSampleSize = binary.BigEndian.Uint32(p[off:])
		off += 4
	}
	if flags&tfhdDefaultSampleFlagsPresent != 0 {
		if len(p) < off+4 {
			return drmTfhd{}, errors.New("tfhd: missing default_sample_flags")
		}
		tfhd.defaultSampleFlagsPresent = true
		off += 4
	}
	_ = off
	return tfhd, nil
}

// drmParseTrun parses the Track Run box payload.
// Returns per-sample records and the data offset (0 if flag absent).
func drmParseTrun(p []byte) ([]drmTrunSample, int32, error) {
	// version(1) flags(3) sample_count(4) [data-offset(4)] [first-sample-flags(4)] [per-sample fields]
	if len(p) < 8 {
		return nil, 0, errors.New("trun too short")
	}
	version := p[0]
	flags := uint32(p[1])<<16 | uint32(p[2])<<8 | uint32(p[3])
	sampleCount := binary.BigEndian.Uint32(p[4:8])
	off := 8

	const (
		trunDataOffsetPresent       = 0x000001
		trunFirstSampleFlagsPresent = 0x000004
		trunSampleDurationPresent   = 0x000100
		trunSampleSizePresent       = 0x000200
		trunSampleFlagsPresent      = 0x000400
		trunSampleCTSOffsetPresent  = 0x000800
	)

	var dataOffset int32
	if flags&trunDataOffsetPresent != 0 {
		if len(p) < off+4 {
			return nil, 0, errors.New("trun: missing data_offset")
		}
		dataOffset = int32(binary.BigEndian.Uint32(p[off:]))
		off += 4
	}
	if flags&trunFirstSampleFlagsPresent != 0 {
		if len(p) < off+4 {
			return nil, 0, errors.New("trun: missing first_sample_flags")
		}
		off += 4
	}

	// Bound sample_count against remaining box bytes to prevent hostile allocations.
	// Compute the minimum bytes consumed per sample for the active flags.
	perSampleBytes := 0
	if flags&trunSampleDurationPresent != 0 {
		perSampleBytes += 4
	}
	if flags&trunSampleSizePresent != 0 {
		perSampleBytes += 4
	}
	if flags&trunSampleFlagsPresent != 0 {
		perSampleBytes += 4
	}
	if flags&trunSampleCTSOffsetPresent != 0 {
		perSampleBytes += 4
	}
	if perSampleBytes > 0 {
		maxCount := uint32((len(p) - off) / perSampleBytes)
		if sampleCount > maxCount {
			return nil, 0, fmt.Errorf("trun: sample_count %d exceeds box bounds (%d bytes remaining, %d bytes/sample)",
				sampleCount, len(p)-off, perSampleBytes)
		}
	} else if sampleCount > uint32(len(p)) {
		return nil, 0, fmt.Errorf("trun: sample_count %d exceeds box size %d", sampleCount, len(p))
	}

	samples := make([]drmTrunSample, sampleCount)
	for i := uint32(0); i < sampleCount; i++ {
		var s drmTrunSample
		if flags&trunSampleDurationPresent != 0 {
			if len(p) < off+4 {
				return nil, 0, fmt.Errorf("trun: sample %d missing duration", i)
			}
			s.Duration = binary.BigEndian.Uint32(p[off:])
			off += 4
		}
		if flags&trunSampleSizePresent != 0 {
			if len(p) < off+4 {
				return nil, 0, fmt.Errorf("trun: sample %d missing size", i)
			}
			s.Size = binary.BigEndian.Uint32(p[off:])
			off += 4
		}
		if flags&trunSampleFlagsPresent != 0 {
			if len(p) < off+4 {
				return nil, 0, fmt.Errorf("trun: sample %d missing flags", i)
			}
			s.Flags = binary.BigEndian.Uint32(p[off:])
			off += 4
		}
		if flags&trunSampleCTSOffsetPresent != 0 {
			if len(p) < off+4 {
				return nil, 0, fmt.Errorf("trun: sample %d missing CTS offset", i)
			}
			if version == 1 {
				s.CTSOffset = int32(binary.BigEndian.Uint32(p[off:]))
			} else {
				s.CTSOffset = int32(binary.BigEndian.Uint32(p[off:]))
			}
			off += 4
		}
		samples[i] = s
	}
	return samples, dataOffset, nil
}

// drmParseSenc parses the Sample Encryption box payload.
// fallbackIV is used when the box does not contain per-sample IVs (IV size 0,
// i.e. a constant IV taken from the tenc box or supplied by the caller).
//
// The ISO BMFF senc box does not encode the per-sample IV size; that value is
// carried by the Track Encryption Box (tenc) in the moov hierarchy.  When the
// segment carried a tenc, ivSizeHint holds its default_Per_Sample_IV_Size and
// is tried first; pass -1 when it is unknown.  Otherwise (or when the hint
// does not fit the box) we resolve the ambiguity structurally: we try IV
// sizes 0 (only meaningful with subsample tables), 8 and 16 and require that
// exactly one fully consumes the box payload.  When several sizes would
// consume all bytes (only possible when sampleCount is zero, or by structural
// coincidence) a zero sample count is returned as unambiguously empty and a
// non-zero count is an error rather than a silent guess.
func drmParseSenc(p []byte, fallbackIV []byte, ivSizeHint int) ([]drmSencEntry, error) {
	// version(1) flags(3) sample_count(4) then per-sample: IV[ivSize] [subsample_count(2) pairs...]
	if len(p) < 8 {
		return nil, errors.New("senc too short")
	}
	flags := uint32(p[1])<<16 | uint32(p[2])<<8 | uint32(p[3])
	sampleCount := binary.BigEndian.Uint32(p[4:8])
	off := 8

	const useSubsampleEncryption = 0x000002
	hasSubs := flags&useSubsampleEncryption != 0

	// Sanity-bound sampleCount before probing: every sample needs at least one byte.
	if sampleCount > uint32(len(p)-off) {
		return nil, fmt.Errorf("senc: sample_count %d exceeds remaining box bytes %d", sampleCount, len(p)-off)
	}

	payload := p[off:]
	boxLen := len(payload)

	// F5: use count-only probes (no entry/IV/subsample allocations) to determine which
	// IV size exactly fits the box, then allocate once on the confirmed pass.
	fits := func(ivSize int) (bool, error) {
		consumed, err := drmParseSencConsumed(payload, int(sampleCount), ivSize, hasSubs)
		if err != nil {
			return false, err
		}
		if consumed != boxLen {
			return false, fmt.Errorf("consumed %d of %d payload bytes", consumed, boxLen)
		}
		return true, nil
	}

	ivSize := -1
	if ivSizeHint == 0 && hasSubs || ivSizeHint == 8 || ivSizeHint == 16 {
		if ok, _ := fits(ivSizeHint); ok {
			ivSize = ivSizeHint
		}
	}

	if ivSize < 0 {
		candidates := []int{8, 16}
		if hasSubs {
			// Constant-IV schemes (cbcs): no per-sample IV, only subsample tables.
			candidates = []int{0, 8, 16}
		}
		var matches []int
		var probeErrs []error
		for _, c := range candidates {
			ok, err := fits(c)
			if ok {
				matches = append(matches, c)
			} else {
				probeErrs = append(probeErrs, fmt.Errorf("%d-byte: %w", c, err))
			}
		}
		switch {
		case len(matches) == 1:
			ivSize = matches[0]
		case len(matches) > 1:
			// Several IV sizes parse cleanly and consume all bytes.  For a
			// non-zero sample count this is structurally ambiguous; return an
			// error rather than guessing.  When sampleCount is zero every
			// probe produces an equivalent empty list.
			if sampleCount > 0 {
				return nil, fmt.Errorf("senc: IV size ambiguous: IV sizes %v all exactly consume the box payload", matches)
			}
			return []drmSencEntry{}, nil
		default:
			// No IV size results in a parse that exactly consumes the box payload.
			return nil, fmt.Errorf("senc: IV size ambiguous or corrupt: %w", errors.Join(probeErrs...))
		}
	}

	// Single allocating pass with the confirmed IV size.
	entries, _, err := drmParseSencWithIVSize(payload, int(sampleCount), ivSize, hasSubs)
	if err != nil {
		return nil, err
	}

	// Pad 8-byte IVs to 16 bytes: CENC uses the IV as the high 64 bits of the
	// 128-bit AES-CTR counter block; the low 64 bits start at zero.
	for i := range entries {
		if len(entries[i].IV) == 8 {
			padded := make([]byte, 16)
			copy(padded, entries[i].IV)
			entries[i].IV = padded
		}
		if len(entries[i].IV) == 0 && len(fallbackIV) > 0 {
			entries[i].IV = fallbackIV
		}
	}
	return entries, nil
}

// drmParseSencWithIVSize attempts to parse the senc box payload p with the
// given per-sample IV size.  It returns the parsed entries, the total number
// of bytes consumed from p, and any structural error.  The caller MUST verify
// that consumed == len(p) to confirm the IV size is unambiguous.
func drmParseSencWithIVSize(p []byte, count, ivSize int, hasSubs bool) ([]drmSencEntry, int, error) {
	// Bound count against box size before allocating: each sample needs at least ivSize bytes.
	minPerSample := ivSize
	if hasSubs {
		minPerSample += 2 // subsample_count field
	}
	if minPerSample > 0 && count > len(p)/minPerSample {
		return nil, 0, fmt.Errorf("senc: sample_count %d exceeds box bounds (%d bytes, min %d bytes/sample)",
			count, len(p), minPerSample)
	}

	entries := make([]drmSencEntry, count)
	off := 0
	for i := 0; i < count; i++ {
		if len(p) < off+ivSize {
			return nil, off, fmt.Errorf("senc: sample %d: need %d IV bytes, have %d", i, ivSize, len(p)-off)
		}
		iv := make([]byte, ivSize)
		copy(iv, p[off:off+ivSize])
		off += ivSize

		var subs []drmSubsample
		if hasSubs {
			if len(p) < off+2 {
				return nil, off, fmt.Errorf("senc: sample %d: missing subsample_count", i)
			}
			subCount := int(binary.BigEndian.Uint16(p[off:]))
			off += 2
			// Bound subCount: each subsample is 6 bytes (2 clear + 4 encrypted).
			if subCount > (len(p)-off)/6 {
				return nil, off, fmt.Errorf("senc: sample %d: subsample_count %d exceeds remaining box bytes %d",
					i, subCount, len(p)-off)
			}
			subs = make([]drmSubsample, subCount)
			for j := 0; j < subCount; j++ {
				if len(p) < off+6 {
					return nil, off, fmt.Errorf("senc: sample %d sub %d: too short", i, j)
				}
				clear := int(binary.BigEndian.Uint16(p[off:]))
				enc := int(binary.BigEndian.Uint32(p[off+2:]))
				subs[j] = drmSubsample{Clear: clear, Encrypted: enc}
				off += 6
			}
		}
		entries[i] = drmSencEntry{IV: iv, Subsamples: subs}
	}
	return entries, off, nil
}

// drmParseSencConsumed walks the senc payload with the given parameters and returns
// the number of bytes that would be consumed, WITHOUT allocating any entry/IV/subsample
// slices.  Used by drmParseSenc to probe which IV size fits the box before committing
// to a single allocating parse pass (F5: eliminates the wrong-probe's N×allocs).
func drmParseSencConsumed(p []byte, count, ivSize int, hasSubs bool) (consumed int, err error) {
	minPerSample := ivSize
	if hasSubs {
		minPerSample += 2 // subsample_count field
	}
	if minPerSample > 0 && count > len(p)/minPerSample {
		return 0, fmt.Errorf("senc: sample_count %d exceeds box bounds (%d bytes, min %d bytes/sample)",
			count, len(p), minPerSample)
	}
	off := 0
	for i := 0; i < count; i++ {
		if len(p) < off+ivSize {
			return off, fmt.Errorf("senc: sample %d: need %d IV bytes, have %d", i, ivSize, len(p)-off)
		}
		off += ivSize
		if hasSubs {
			if len(p) < off+2 {
				return off, fmt.Errorf("senc: sample %d: missing subsample_count", i)
			}
			subCount := int(binary.BigEndian.Uint16(p[off:]))
			off += 2
			if subCount > (len(p)-off)/6 {
				return off, fmt.Errorf("senc: sample %d: subsample_count %d exceeds remaining box bytes %d",
					i, subCount, len(p)-off)
			}
			off += subCount * 6
		}
	}
	return off, nil
}

// drmParseBoxes parses ISO BMFF boxes from b, recursing into container boxes.
// Returned boxes are in order; container children appear after the parent.
func drmParseBoxes(b []byte) ([]drmBox, error) {
	return drmParseBoxesAt(b, 0, len(b), 0)
}

// drmContainerTypes lists box types that contain child boxes.
var drmContainerTypes = map[string]bool{
	"moof": true,
	"traf": true,
	"moov": true,
	"trak": true,
	"mdia": true,
	"minf": true,
	"stbl": true,
	"edts": true,
	"dinf": true,
	"udta": true,
	"schi": true,
}

func drmParseBoxesAt(b []byte, startOffset, limit, depth int) ([]drmBox, error) {
	const maxBoxDepth = 32
	if depth > maxBoxDepth {
		return nil, fmt.Errorf("BMFF box nesting exceeds maximum depth (%d)", maxBoxDepth)
	}
	var boxes []drmBox
	pos := 0
	for pos < len(b) {
		if len(b)-pos < 8 {
			// Fewer than 8 bytes: not a valid box header; stop.
			break
		}
		boxStart := pos
		size32 := binary.BigEndian.Uint32(b[pos:])
		typeBytes := b[pos+4 : pos+8]
		boxType := string(typeBytes)
		pos += 8
		hdrSize := 8

		var boxSize int
		switch size32 {
		case 0:
			// Box extends to end of buffer.
			boxSize = len(b) - boxStart
		case 1:
			// Large size: next 8 bytes are the actual size.
			if len(b) < pos+8 {
				return nil, fmt.Errorf("box at %d: size==1 but no largesize", boxStart)
			}
			large := binary.BigEndian.Uint64(b[pos:])
			pos += 8
			hdrSize += 8
			if large > uint64(len(b)-boxStart) {
				return nil, fmt.Errorf("box at %d: largesize %d exceeds buffer", boxStart, large)
			}
			boxSize = int(large)
		default:
			if int(size32) < 8 {
				return nil, fmt.Errorf("box at %d: size %d < 8", boxStart, size32)
			}
			if boxStart+int(size32) > len(b) {
				return nil, fmt.Errorf("box at %d: size %d exceeds buffer len %d", boxStart, size32, len(b))
			}
			boxSize = int(size32)
		}

		// Handle uuid: a 16-byte usertype follows the 4-byte type field
		// (ISO/IEC 14496-12 §4.2: extended_type[16]).
		if boxType == "uuid" {
			if len(b) < pos+16 {
				return nil, fmt.Errorf("uuid box at %d: too short for usertype", boxStart)
			}
			pos += 16
			hdrSize += 16
		}

		// The declared box size must be able to hold at least the header we
		// just consumed (8 bytes, +8 for a largesize field, +16 for uuid's
		// usertype); otherwise pos would run past endPos below and the
		// payload slice expression would panic with low > high.
		if boxSize < hdrSize {
			return nil, fmt.Errorf("box at %d: size %d smaller than header size %d", boxStart, boxSize, hdrSize)
		}

		endPos := boxStart + boxSize
		if endPos > len(b) {
			endPos = len(b)
		}

		payload := b[pos:endPos]
		box := drmBox{
			Type:    boxType,
			Start:   startOffset + boxStart,
			HdrSize: hdrSize,
			Size:    boxSize,
			Payload: payload,
		}
		boxes = append(boxes, box)

		// Recurse into known containers.
		if drmContainerTypes[boxType] {
			children, err := drmParseBoxesAt(payload, startOffset+pos, len(payload), depth+1)
			if err != nil {
				return boxes, fmt.Errorf("box %q at %d: child parse error: %w", boxType, box.Start, err)
			}
			boxes = append(boxes, children...)
		}

		pos = boxStart + boxSize
		if boxSize == 0 {
			// size==0 means to EOF; stop.
			break
		}
	}
	return boxes, nil
}

// drmDecryptCTRSubsamples decrypts the encrypted spans of data using AES-CTR, in place.
// CENC: IV is the 8-byte (or 16-byte) initialization vector. For 8-byte IVs the
// high 64 bits of the 128-bit counter are set from IV; the low 64 bits start at 0.
// When subs is nil the entire buffer is treated as encrypted.
// Clear bytes are NOT advanced through the CTR keystream.
// Returns data (same slice) on success so callers can chain; the argument is modified.
func drmDecryptCTRSubsamples(key, iv []byte, data []byte, subs []drmSubsample) ([]byte, error) {
	if len(key) != 16 {
		return nil, fmt.Errorf("CTR: key must be 16 bytes, got %d", len(key))
	}

	// Build a 16-byte counter block: IV in high bytes, zeros in low bytes.
	ctr := make([]byte, 16)
	if len(iv) >= 16 {
		copy(ctr, iv[:16])
	} else if len(iv) == 8 {
		// CENC: IV occupies the first 8 bytes; the block counter (low 8) starts at 0.
		copy(ctr[:8], iv)
	} else if len(iv) > 0 {
		copy(ctr, iv)
	}

	block, err := cachedAESBlock(key) // F2: reuse cached block; avoids key-scheduling per segment
	if err != nil {
		return nil, fmt.Errorf("CTR: %w", err)
	}

	if len(subs) == 0 {
		// Whole buffer encrypted: XOR in place (cipher.Stream allows src==dst).
		cipher.NewCTR(block, ctr).XORKeyStream(data, data)
		return data, nil
	}

	// Subsample mode: XOR only the encrypted spans in place; skip clear bytes.
	// The CTR keystream advances solely over encrypted bytes (clear bytes do not
	// consume keystream), so we maintain our own per-block position manually.
	ctrBlock := [16]byte{}
	copy(ctrBlock[:], ctr)

	genKeyBlock := func(cb [16]byte) [16]byte {
		var kb [16]byte
		block.Encrypt(kb[:], cb[:])
		return kb
	}

	// Increment the counter on the low 8 bytes (big-endian) per CENC spec.
	incCounter := func(cb *[16]byte) {
		for i := 15; i >= 8; i-- {
			cb[i]++
			if cb[i] != 0 {
				break
			}
		}
	}

	ksPos := 0 // byte position within the current keystream block
	ksBlock := genKeyBlock(ctrBlock)

	dataOff := 0
	for _, sub := range subs {
		// Advance past clear bytes without consuming keystream.
		if sub.Clear > 0 {
			end := dataOff + sub.Clear
			if end > len(data) {
				end = len(data)
			}
			dataOff = end
		}

		// XOR encrypted bytes in place, advancing the keystream.
		enc := sub.Encrypted
		for enc > 0 {
			if dataOff >= len(data) {
				break
			}
			avail := 16 - ksPos
			take := enc
			if take > avail {
				take = avail
			}
			if dataOff+take > len(data) {
				take = len(data) - dataOff
			}
			for i := 0; i < take; i++ {
				data[dataOff+i] ^= ksBlock[ksPos+i]
			}
			dataOff += take
			ksPos += take
			enc -= take
			if ksPos == 16 {
				incCounter(&ctrBlock)
				ksBlock = genKeyBlock(ctrBlock)
				ksPos = 0
			}
		}
	}
	return data, nil
}

// drmDecryptCBCSubsamples decrypts the protected ranges of one sample with
// AES-CBC as specified for the CENC 'cbcs' scheme (ISO/IEC 23001-7 §10.4).
//
// Each protected range (every subsample's encrypted span, or the whole sample
// when subs is empty) is processed with pattern encryption: cryptBlocks
// 16-byte blocks are encrypted, then skipBlocks blocks are left clear, and the
// pattern repeats to the end of the range.  A trailing partial block (fewer
// than 16 bytes) is always left clear.  cryptBlocks == 0 means no pattern:
// every full block of the range is encrypted.  Typical video uses 1:9.
func drmDecryptCBCSubsamples(key, iv []byte, data []byte, subs []drmSubsample, cryptBlocks, skipBlocks int) error {
	if len(key) != 16 {
		return fmt.Errorf("CBC subsample: key must be 16 bytes, got %d", len(key))
	}
	if len(iv) != 16 {
		// Pad or reject.
		if len(iv) < 16 {
			padded := make([]byte, 16)
			copy(padded, iv)
			iv = padded
		} else {
			iv = iv[:16]
		}
	}
	if cryptBlocks < 0 || skipBlocks < 0 {
		return fmt.Errorf("CBC subsample: invalid pattern %d:%d", cryptBlocks, skipBlocks)
	}

	block, err := cachedAESBlock(key) // F2: reuse cached block; avoids key-scheduling per segment
	if err != nil {
		return fmt.Errorf("CBC subsample: %w", err)
	}

	if len(subs) == 0 {
		drmCBCSPattern(block, iv, data, cryptBlocks, skipBlocks)
		return nil
	}

	// Per the CENC 'cbcs' scheme, AES-CBC is re-initialised with the constant
	// sample IV at the START of each encrypted subsample span — the chain does
	// NOT carry across subsamples (matches FFmpeg cbcs_scheme_decrypt). A fresh
	// CBC decrypter is therefore created per span (inside drmCBCSPattern).
	off := 0
	for _, sub := range subs {
		off += sub.Clear
		if off > len(data) {
			return fmt.Errorf("CBC subsample: clear span exceeds data len %d", len(data))
		}
		if sub.Encrypted == 0 {
			continue
		}
		end := off + sub.Encrypted
		if end > len(data) {
			return fmt.Errorf("CBC subsample: encrypted span [%d:%d] exceeds data len %d", off, end, len(data))
		}
		drmCBCSPattern(block, iv, data[off:end], cryptBlocks, skipBlocks)
		off = end
	}
	return nil
}

// drmCBCSPattern decrypts span in place with one CBC chain started from iv,
// applying the crypt:skip block pattern.  The chain continues across crypt
// runs (skipped blocks take no part in it); a trailing partial block stays clear.
func drmCBCSPattern(block cipher.Block, iv, span []byte, cryptBlocks, skipBlocks int) {
	full := len(span) / aes.BlockSize // number of complete blocks
	if full == 0 {
		return
	}
	dec := cipher.NewCBCDecrypter(block, iv)
	if cryptBlocks == 0 {
		dec.CryptBlocks(span[:full*aes.BlockSize], span[:full*aes.BlockSize])
		return
	}
	for blk := 0; blk < full; blk += cryptBlocks + skipBlocks {
		n := cryptBlocks
		if blk+n > full {
			n = full - blk
		}
		run := span[blk*aes.BlockSize : (blk+n)*aes.BlockSize]
		dec.CryptBlocks(run, run)
	}
}
