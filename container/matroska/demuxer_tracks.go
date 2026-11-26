package matroska

import (
	"io"
	"math"

	"github.com/thesyncim/goav/container/ebml"
)

// markSeenBit rejects duplicate singleton child elements using a compact bitmask.
func markSeenBit(seen *uint64, bit uint) error {
	mask := uint64(1) << bit
	if *seen&mask != 0 {
		return ErrInvalidData
	}
	*seen |= mask
	return nil
}

func readOnceBoolFlag(r io.Reader, size uint64, seen *uint64, bit uint, dst, set *bool) error {
	if err := markSeenBit(seen, bit); err != nil {
		return err
	}
	v, err := readBoolFlagPayload(r, size)
	if err != nil {
		return err
	}
	*dst = v
	if set != nil {
		*set = true
	}
	return nil
}

func readOnceString(r io.Reader, size uint64, seen *uint64, bit uint, dst *string) error {
	if err := markSeenBit(seen, bit); err != nil {
		return err
	}
	v, err := readStringPayload(r, size)
	if err != nil {
		return err
	}
	*dst = v
	return nil
}

func readOnceUInt(r io.Reader, size uint64, seen *uint64, bit uint, dst *uint64, set *bool) error {
	if err := markSeenBit(seen, bit); err != nil {
		return err
	}
	v, err := readUIntPayload(r, size)
	if err != nil {
		return err
	}
	*dst = v
	if set != nil {
		*set = true
	}
	return nil
}

func readOnceIntFromUInt(r io.Reader, size uint64, seen *uint64, bit uint, dst *int, set *bool) error {
	if err := markSeenBit(seen, bit); err != nil {
		return err
	}
	v, err := readIntPayloadFromUInt(r, size)
	if err != nil {
		return err
	}
	*dst = v
	if set != nil {
		*set = true
	}
	return nil
}

func readOnceNonZeroInt(r io.Reader, size uint64, seen *uint64, bit uint, dst *int) error {
	if err := markSeenBit(seen, bit); err != nil {
		return err
	}
	v, err := readUIntPayload(r, size)
	if err != nil {
		return err
	}
	*dst, err = nonZeroIntFromUint(v)
	return err
}

func readOnceNonNegativeInt64(r io.Reader, size uint64, seen *uint64, bit uint, dst *int64) error {
	if err := markSeenBit(seen, bit); err != nil {
		return err
	}
	v, err := readUIntPayload(r, size)
	if err != nil {
		return err
	}
	if v > uint64(math.MaxInt64) {
		return ErrInvalidData
	}
	*dst = int64(v)
	return nil
}

func readOnceFloat(r io.Reader, size uint64, seen *uint64, bit uint, dst *float64, set *bool, fn func(io.Reader, uint64) (float64, error)) error {
	if err := markSeenBit(seen, bit); err != nil {
		return err
	}
	v, err := fn(r, size)
	if err != nil {
		return err
	}
	*dst = v
	if set != nil {
		*set = true
	}
	return nil
}

const (
	teSeenTrackNumber uint = iota
	teSeenTrackUID
	teSeenTrackType
	teSeenCodecID
	teSeenFlagEnabled
	teSeenFlagDefault
	teSeenFlagForced
	teSeenFlagHearingImpaired
	teSeenFlagVisualImpaired
	teSeenFlagTextDescriptions
	teSeenFlagOriginal
	teSeenFlagCommentary
	teSeenFlagLacing
	teSeenName
	teSeenLanguage
	teSeenLanguageBCP
	teSeenCodecName
	teSeenMinCache
	teSeenMaxCache
	teSeenDefaultDuration
	teSeenDefaultDecodedDuration
	teSeenTrackTimestampScale
	teSeenTrackOffset
	teSeenMaxBlockAdditionID
	teSeenCodecDecodeAll
	teSeenCodecDelay
	teSeenSeekPreRoll
	teSeenContentEncodings
	teSeenCodecPrivate
	teSeenVideo
	teSeenAudio
)

func (d *Demuxer) parseTrackEntry(parent io.Reader, header ebml.Header) (Track, error) {
	if header.Size.Unknown {
		return Track{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return Track{}, err
	}
	track := Track{Language: "und", TimebaseNum: 1, TimebaseDen: timeNS, CodecDecodeAll: true, FlagEnabled: true, FlagDefault: true, FlagLacing: true}
	var codecID string
	var seen uint64
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return Track{}, err
		}
		r := master.Reader()
		sz := child.Size.Value
		switch child.ID {
		case idTrackNumber:
			if err := markSeenBit(&seen, teSeenTrackNumber); err != nil {
				return Track{}, err
			}
			value, err := readUIntPayload(r, sz)
			if err != nil {
				return Track{}, err
			}
			track.ID, err = trackIDFromUint(value)
			if err != nil {
				return Track{}, err
			}
		case idTrackUID:
			if err := markSeenBit(&seen, teSeenTrackUID); err != nil {
				return Track{}, err
			}
			value, err := readUIntPayload(r, sz)
			if err != nil || value == 0 {
				if err != nil {
					return Track{}, err
				}
				return Track{}, ErrInvalidData
			}
			track.UID = value
		case idTrackType:
			if err := markSeenBit(&seen, teSeenTrackType); err != nil {
				return Track{}, err
			}
			value, err := readUIntPayload(r, sz)
			if err != nil {
				return Track{}, err
			}
			switch value {
			case matroskaTrackAudio:
				track.Type = TrackAudio
			case matroskaTrackVideo:
				track.Type = TrackVideo
			case matroskaTrackSubtitle:
				track.Type = TrackSubtitle
			default:
				track.Type = TrackUnknown
			}
		case idFlagEnabled:
			err = readOnceBoolFlag(r, sz, &seen, teSeenFlagEnabled, &track.FlagEnabled, &track.FlagEnabledSet)
		case idFlagDefault:
			err = readOnceBoolFlag(r, sz, &seen, teSeenFlagDefault, &track.FlagDefault, &track.FlagDefaultSet)
		case idFlagForced:
			err = readOnceBoolFlag(r, sz, &seen, teSeenFlagForced, &track.FlagForced, &track.FlagForcedSet)
		case idFlagHearingImpaired:
			err = readOnceBoolFlag(r, sz, &seen, teSeenFlagHearingImpaired, &track.FlagHearingImpaired, &track.FlagHearingImpairedSet)
		case idFlagVisualImpaired:
			err = readOnceBoolFlag(r, sz, &seen, teSeenFlagVisualImpaired, &track.FlagVisualImpaired, &track.FlagVisualImpairedSet)
		case idFlagTextDescriptions:
			err = readOnceBoolFlag(r, sz, &seen, teSeenFlagTextDescriptions, &track.FlagTextDescriptions, &track.FlagTextDescriptionsSet)
		case idFlagOriginal:
			err = readOnceBoolFlag(r, sz, &seen, teSeenFlagOriginal, &track.FlagOriginal, &track.FlagOriginalSet)
		case idFlagCommentary:
			err = readOnceBoolFlag(r, sz, &seen, teSeenFlagCommentary, &track.FlagCommentary, &track.FlagCommentarySet)
		case idFlagLacing:
			err = readOnceBoolFlag(r, sz, &seen, teSeenFlagLacing, &track.FlagLacing, &track.FlagLacingSet)
		case idCodecDecodeAll:
			err = readOnceBoolFlag(r, sz, &seen, teSeenCodecDecodeAll, &track.CodecDecodeAll, &track.CodecDecodeAllSet)
		case idName:
			err = readOnceString(r, sz, &seen, teSeenName, &track.Name)
		case idLanguage:
			err = readOnceString(r, sz, &seen, teSeenLanguage, &track.Language)
		case idLanguageBCP:
			err = readOnceString(r, sz, &seen, teSeenLanguageBCP, &track.LanguageBCP47)
		case idCodecID:
			err = readOnceString(r, sz, &seen, teSeenCodecID, &codecID)
		case idCodecName:
			err = readOnceString(r, sz, &seen, teSeenCodecName, &track.CodecName)
		case idMinCache:
			err = readOnceUInt(r, sz, &seen, teSeenMinCache, &track.MinCache, &track.MinCacheSet)
		case idMaxCache:
			err = readOnceUInt(r, sz, &seen, teSeenMaxCache, &track.MaxCache, &track.MaxCacheSet)
		case idMaxBlockAdditionID:
			err = readOnceUInt(r, sz, &seen, teSeenMaxBlockAdditionID, &track.MaxBlockAdditionID, nil)
		case idDefaultDur:
			if err = markSeenBit(&seen, teSeenDefaultDuration); err == nil {
				track.DefaultDurationNS, err = readNonZeroInt64Payload(r, sz)
			}
		case idDefaultDecodedDur:
			if err = markSeenBit(&seen, teSeenDefaultDecodedDuration); err == nil {
				track.DefaultDecodedFieldDurationNS, err = readNonZeroInt64Payload(r, sz)
			}
		case idTrackTimestampScale:
			if err = markSeenBit(&seen, teSeenTrackTimestampScale); err == nil {
				var value float64
				if value, err = readFloatPayload(r, sz); err == nil {
					if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
						err = ErrInvalidData
					} else {
						track.TrackTimestampScale = value
						track.TrackTimestampScaleSet = true
					}
				}
			}
		case idTrackOffset:
			if err = markSeenBit(&seen, teSeenTrackOffset); err == nil {
				if track.TrackOffsetNS, err = readIntPayload(r, sz); err == nil {
					track.TrackOffsetSet = true
				}
			}
		case idBlockAdditionMapping:
			var mapping BlockAdditionMapping
			if mapping, err = d.parseBlockAdditionMapping(r, child); err == nil {
				track.BlockAdditionMappings = append(track.BlockAdditionMappings, mapping)
			}
		case idTrackOverlay:
			var value uint64
			if value, err = readUIntPayload(r, sz); err == nil {
				if value == 0 {
					err = ErrInvalidData
				} else {
					track.TrackOverlays = append(track.TrackOverlays, value)
				}
			}
		case idCodecDelay:
			err = readOnceNonNegativeInt64(r, sz, &seen, teSeenCodecDelay, &track.CodecDelayNS)
		case idSeekPreRoll:
			err = readOnceNonNegativeInt64(r, sz, &seen, teSeenSeekPreRoll, &track.SeekPreRollNS)
		case idTrackTranslate:
			var translate TrackTranslate
			if translate, err = d.parseTrackTranslate(r, child); err == nil {
				track.TrackTranslates = append(track.TrackTranslates, translate)
			}
		case idContentEncodings:
			if err = markSeenBit(&seen, teSeenContentEncodings); err == nil {
				track.ContentEncodings, err = d.parseContentEncodings(r, child)
			}
		case idCodecPrivate:
			if err = markSeenBit(&seen, teSeenCodecPrivate); err == nil {
				track.CodecPrivate, err = readBinaryPayload(r, sz)
			}
		case idVideo:
			if err = markSeenBit(&seen, teSeenVideo); err == nil {
				track.Video, err = d.parseVideo(r, child)
			}
		case idAudio:
			if err = markSeenBit(&seen, teSeenAudio); err == nil {
				track.Audio, err = d.parseAudio(r, child)
			}
		default:
			err = appendUnknownChildElement(r, child, isKnownTrackEntryElement, &track.UnknownElements)
		}
		if err != nil {
			return Track{}, err
		}
	}
	if err := master.Validate(); err != nil {
		return Track{}, err
	}
	const requiredMask = (uint64(1) << teSeenTrackNumber) | (uint64(1) << teSeenTrackType) | (uint64(1) << teSeenCodecID)
	if seen&requiredMask != requiredMask {
		return Track{}, ErrInvalidData
	}
	if (track.Type == TrackVideo && seen&(uint64(1)<<teSeenVideo) == 0) ||
		(track.Type == TrackAudio && seen&(uint64(1)<<teSeenAudio) == 0) {
		return Track{}, ErrInvalidData
	}
	if track.UID == 0 {
		track.UID = uint64(track.ID)
	}
	if err := applyTrackCodecPrivate(codecID, &track); err != nil {
		return Track{}, err
	}
	if err := validateTrackBlockAdditionMetadata(track); err != nil {
		return Track{}, err
	}
	if err := validateContentEncodings(track.ContentEncodings); err != nil {
		return Track{}, ErrInvalidData
	}
	if err := validateUnknownElementsFor(track.UnknownElements, isKnownTrackEntryElement); err != nil {
		return Track{}, ErrInvalidData
	}
	return track, nil
}

func applyTrackCodecPrivate(codecID string, track *Track) error {
	track.Codec = codecFromMatroskaID(codecID, track.CodecPrivate)
	if codecID == codecIDMS {
		format, err := parseMSACMWaveFormat(track.CodecPrivate)
		if err != nil {
			return err
		}
		track.Codec = codecFromMSACMTag(format.FormatTag)
		track.Audio.SampleRate = format.SampleRate
		track.Audio.Channels = format.Channels
		track.Audio.BitDepth = format.BitsPerSample
		if track.Codec == CodecPCMU || track.Codec == CodecPCMA {
			return validateG711MSACMWaveFormat(format, track.Codec)
		}
		return nil
	}
	switch track.Codec {
	case CodecOpus:
		if len(track.CodecPrivate) != 0 {
			head, err := parseOpusHead(track.CodecPrivate)
			if err != nil {
				return err
			}
			track.Audio.Channels = head.Channels
			if track.CodecDelayNS == 0 {
				track.CodecDelayNS = opusCodecDelayNS(head.PreSkip)
			}
			if track.SeekPreRollNS == 0 {
				track.SeekPreRollNS = opusDefaultSeekPreRollNS
			}
		}
	case CodecVorbis:
		private, err := parseVorbisCodecPrivate(track.CodecPrivate)
		if err != nil {
			return err
		}
		track.Audio.Channels = private.Channels
		track.Audio.SampleRate = private.SampleRate
	case CodecFLAC:
		private, err := parseFLACCodecPrivate(track.CodecPrivate)
		if err != nil {
			return err
		}
		track.Audio.Channels = private.Channels
		track.Audio.SampleRate = private.SampleRate
		track.Audio.BitDepth = private.BitsPerSample
	case CodecAAC:
		private, err := parseAACAudioSpecificConfig(track.CodecPrivate)
		if err != nil {
			return err
		}
		track.Audio.Channels = private.Channels
		track.Audio.SampleRate = private.SampleRate
	case CodecAV1:
		if len(track.CodecPrivate) != 0 {
			if _, err := parseAV1CodecConfigurationRecord(track.CodecPrivate); err != nil {
				return err
			}
		}
	case CodecH264:
		if len(track.CodecPrivate) != 0 {
			if _, err := parseAVCDecoderConfigurationRecord(track.CodecPrivate); err != nil {
				return err
			}
		}
	case CodecH265:
		if len(track.CodecPrivate) != 0 {
			if _, err := parseHEVCDecoderConfigurationRecord(track.CodecPrivate); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Demuxer) parseTrackTranslate(parent io.Reader, header ebml.Header) (TrackTranslate, error) {
	if header.Size.Unknown {
		return TrackTranslate{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return TrackTranslate{}, err
	}
	var translate TrackTranslate
	trackIDSeen := false
	codecSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return TrackTranslate{}, err
		}
		switch child.ID {
		case idTrackTranslateTrack:
			if err := markElementSeen(&trackIDSeen); err != nil {
				return TrackTranslate{}, err
			}
			translate.TrackID, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return TrackTranslate{}, err
			}
		case idTrackTranslateCodec:
			if err := markElementSeen(&codecSeen); err != nil {
				return TrackTranslate{}, err
			}
			translate.Codec, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return TrackTranslate{}, err
			}
		case idTrackTranslateEdit:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return TrackTranslate{}, err
			}
			translate.EditionUIDs = append(translate.EditionUIDs, value)
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return TrackTranslate{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return TrackTranslate{}, err
	}
	if !trackIDSeen || !codecSeen {
		return TrackTranslate{}, ErrInvalidData
	}
	return translate, nil
}

func (d *Demuxer) parseBlockAdditionMapping(parent io.Reader, header ebml.Header) (BlockAdditionMapping, error) {
	if header.Size.Unknown {
		return BlockAdditionMapping{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return BlockAdditionMapping{}, err
	}
	var mapping BlockAdditionMapping
	idSeen := false
	nameSeen := false
	typeSeen := false
	extraDataSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return BlockAdditionMapping{}, err
		}
		switch child.ID {
		case idBlockAddIDValue:
			if err := markElementSeen(&idSeen); err != nil {
				return BlockAdditionMapping{}, err
			}
			mapping.IDValue, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return BlockAdditionMapping{}, err
			}
		case idBlockAddIDName:
			if err := markElementSeen(&nameSeen); err != nil {
				return BlockAdditionMapping{}, err
			}
			mapping.Name, err = readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return BlockAdditionMapping{}, err
			}
		case idBlockAddIDType:
			if err := markElementSeen(&typeSeen); err != nil {
				return BlockAdditionMapping{}, err
			}
			mapping.Type, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return BlockAdditionMapping{}, err
			}
		case idBlockAddIDExtraData:
			if err := markElementSeen(&extraDataSeen); err != nil {
				return BlockAdditionMapping{}, err
			}
			mapping.ExtraData, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return BlockAdditionMapping{}, err
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return BlockAdditionMapping{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return BlockAdditionMapping{}, err
	}
	if !idSeen || mapping.IDValue < 2 {
		return BlockAdditionMapping{}, ErrInvalidData
	}
	return mapping, nil
}

func (d *Demuxer) parseContentEncodings(parent io.Reader, header ebml.Header) ([]ContentEncoding, error) {
	if header.Size.Unknown {
		return nil, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return nil, err
	}
	var encodings []ContentEncoding
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return nil, err
		}
		switch child.ID {
		case idContentEncoding:
			encoding, err := d.parseContentEncoding(master.Reader(), child)
			if err != nil {
				return nil, err
			}
			encodings = append(encodings, encoding)
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return nil, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return nil, err
	}
	if len(encodings) == 0 {
		return nil, ErrInvalidData
	}
	if err := validateContentEncodings(encodings); err != nil {
		return nil, ErrInvalidData
	}
	return encodings, nil
}

func (d *Demuxer) parseContentEncoding(parent io.Reader, header ebml.Header) (ContentEncoding, error) {
	if header.Size.Unknown {
		return ContentEncoding{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return ContentEncoding{}, err
	}
	encoding := ContentEncoding{Scope: ContentEncodingScopeBlock}
	orderSeen := false
	scopeSeen := false
	typeSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return ContentEncoding{}, err
		}
		switch child.ID {
		case idContentEncodingOrd:
			if err := markElementSeen(&orderSeen); err != nil {
				return ContentEncoding{}, err
			}
			encoding.Order, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncoding{}, err
			}
		case idContentEncodingScope:
			if err := markElementSeen(&scopeSeen); err != nil {
				return ContentEncoding{}, err
			}
			encoding.Scope, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncoding{}, err
			}
		case idContentEncodingType:
			if err := markElementSeen(&typeSeen); err != nil {
				return ContentEncoding{}, err
			}
			encoding.Type, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncoding{}, err
			}
		case idContentCompression:
			if encoding.CompressionSet {
				return ContentEncoding{}, ErrInvalidData
			}
			encoding.Compression, err = d.parseContentCompression(master.Reader(), child)
			if err != nil {
				return ContentEncoding{}, err
			}
			encoding.CompressionSet = true
		case idContentEncryption:
			if encoding.EncryptionSet {
				return ContentEncoding{}, ErrInvalidData
			}
			encoding.Encryption, err = d.parseContentEncryption(master.Reader(), child)
			if err != nil {
				return ContentEncoding{}, err
			}
			encoding.EncryptionSet = true
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return ContentEncoding{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return ContentEncoding{}, err
	}
	return encoding, nil
}

func (d *Demuxer) parseContentCompression(parent io.Reader, header ebml.Header) (ContentCompression, error) {
	if header.Size.Unknown {
		return ContentCompression{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return ContentCompression{}, err
	}
	var compression ContentCompression
	algorithmSeen := false
	settingsSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return ContentCompression{}, err
		}
		switch child.ID {
		case idContentCompAlgo:
			if err := markElementSeen(&algorithmSeen); err != nil {
				return ContentCompression{}, err
			}
			compression.Algorithm, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentCompression{}, err
			}
		case idContentCompSettings:
			if err := markElementSeen(&settingsSeen); err != nil {
				return ContentCompression{}, err
			}
			compression.Settings, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentCompression{}, err
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return ContentCompression{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return ContentCompression{}, err
	}
	return compression, nil
}

func (d *Demuxer) parseContentEncryption(parent io.Reader, header ebml.Header) (ContentEncryption, error) {
	if header.Size.Unknown {
		return ContentEncryption{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return ContentEncryption{}, err
	}
	var encryption ContentEncryption
	algorithmSeen := false
	keyIDSeen := false
	signatureSeen := false
	signatureKeyIDSeen := false
	signatureAlgorithmSeen := false
	signatureHashAlgorithmSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return ContentEncryption{}, err
		}
		switch child.ID {
		case idContentEncAlgo:
			if err := markElementSeen(&algorithmSeen); err != nil {
				return ContentEncryption{}, err
			}
			encryption.Algorithm, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncryption{}, err
			}
		case idContentEncKeyID:
			if err := markElementSeen(&keyIDSeen); err != nil {
				return ContentEncryption{}, err
			}
			encryption.KeyID, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncryption{}, err
			}
		case idContentEncAES:
			if encryption.AESSettingsSet {
				return ContentEncryption{}, ErrInvalidData
			}
			encryption.AESSettings, err = d.parseContentEncAESSettings(master.Reader(), child)
			if err != nil {
				return ContentEncryption{}, err
			}
			encryption.AESSettingsSet = true
		case idContentSignature:
			if err := markElementSeen(&signatureSeen); err != nil {
				return ContentEncryption{}, err
			}
			encryption.Signature, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncryption{}, err
			}
		case idContentSigKeyID:
			if err := markElementSeen(&signatureKeyIDSeen); err != nil {
				return ContentEncryption{}, err
			}
			encryption.SignatureKeyID, err = readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncryption{}, err
			}
		case idContentSigAlgo:
			if err := markElementSeen(&signatureAlgorithmSeen); err != nil {
				return ContentEncryption{}, err
			}
			encryption.SignatureAlgorithm, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncryption{}, err
			}
		case idContentSigHashAlgo:
			if err := markElementSeen(&signatureHashAlgorithmSeen); err != nil {
				return ContentEncryption{}, err
			}
			encryption.SignatureHashAlgorithm, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncryption{}, err
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return ContentEncryption{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return ContentEncryption{}, err
	}
	return encryption, nil
}

func (d *Demuxer) parseContentEncAESSettings(parent io.Reader, header ebml.Header) (ContentEncAESSettings, error) {
	if header.Size.Unknown {
		return ContentEncAESSettings{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return ContentEncAESSettings{}, err
	}
	var settings ContentEncAESSettings
	cipherSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return ContentEncAESSettings{}, err
		}
		switch child.ID {
		case idContentEncAESCipher:
			if err := markElementSeen(&cipherSeen); err != nil {
				return ContentEncAESSettings{}, err
			}
			settings.CipherMode, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return ContentEncAESSettings{}, err
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return ContentEncAESSettings{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return ContentEncAESSettings{}, err
	}
	if !cipherSeen {
		return ContentEncAESSettings{}, ErrInvalidData
	}
	return settings, nil
}

func (d *Demuxer) parseVideo(parent io.Reader, header ebml.Header) (VideoConfig, error) {
	if header.Size.Unknown {
		return VideoConfig{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return VideoConfig{}, err
	}
	var video VideoConfig
	var seen uint64
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return VideoConfig{}, err
		}
		r := master.Reader()
		sz := child.Size.Value
		switch child.ID {
		case idFlagInterlaced:
			err = readOnceIntFromUInt(r, sz, &seen, 0, &video.FlagInterlaced, &video.FlagInterlacedSet)
		case idFieldOrder:
			err = readOnceIntFromUInt(r, sz, &seen, 1, &video.FieldOrder, &video.FieldOrderSet)
		case idStereoMode:
			err = readOnceIntFromUInt(r, sz, &seen, 2, &video.StereoMode, &video.StereoModeSet)
		case idAlphaMode:
			err = readOnceIntFromUInt(r, sz, &seen, 3, &video.AlphaMode, &video.AlphaModeSet)
		case idPixelWidth:
			err = readOnceNonZeroInt(r, sz, &seen, 4, &video.Width)
		case idPixelHeight:
			err = readOnceNonZeroInt(r, sz, &seen, 5, &video.Height)
		case idPixelCropBottom:
			err = readOnceIntFromUInt(r, sz, &seen, 6, &video.PixelCropBottom, nil)
		case idPixelCropTop:
			err = readOnceIntFromUInt(r, sz, &seen, 7, &video.PixelCropTop, nil)
		case idPixelCropLeft:
			err = readOnceIntFromUInt(r, sz, &seen, 8, &video.PixelCropLeft, nil)
		case idPixelCropRight:
			err = readOnceIntFromUInt(r, sz, &seen, 9, &video.PixelCropRight, nil)
		case idDisplayWidth:
			err = readOnceNonZeroInt(r, sz, &seen, 10, &video.DisplayWidth)
		case idDisplayHeight:
			err = readOnceNonZeroInt(r, sz, &seen, 11, &video.DisplayHeight)
		case idDisplayUnit:
			err = readOnceIntFromUInt(r, sz, &seen, 12, &video.DisplayUnit, nil)
		case idAspectRatioType:
			err = readOnceIntFromUInt(r, sz, &seen, 13, &video.AspectRatioType, &video.AspectRatioTypeSet)
		case idColour:
			if err = markSeenBit(&seen, 14); err == nil {
				video.Colour, err = d.parseColour(r, child)
			}
		case idProjection:
			if err = markSeenBit(&seen, 15); err == nil {
				video.Projection, err = d.parseProjection(r, child)
			}
		default:
			err = skipElement(r, child)
		}
		if err != nil {
			return VideoConfig{}, err
		}
	}
	if err := master.Validate(); err != nil {
		return VideoConfig{}, err
	}
	return video, nil
}

func (d *Demuxer) parseProjection(parent io.Reader, header ebml.Header) (VideoProjectionConfig, error) {
	if header.Size.Unknown {
		return VideoProjectionConfig{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return VideoProjectionConfig{}, err
	}
	projection := VideoProjectionConfig{Set: true}
	typeSeen := false
	privateSeen := false
	poseYawSeen := false
	posePitchSeen := false
	poseRollSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return VideoProjectionConfig{}, err
		}
		switch child.ID {
		case idProjectionType:
			if err := markElementSeen(&typeSeen); err != nil {
				return VideoProjectionConfig{}, err
			}
			projection.Type, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoProjectionConfig{}, err
			}
		case idProjectionPrivate:
			if err := markElementSeen(&privateSeen); err != nil {
				return VideoProjectionConfig{}, err
			}
			value, err := readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoProjectionConfig{}, err
			}
			projection.Private = value
		case idProjectionPoseYaw:
			if err := markElementSeen(&poseYawSeen); err != nil {
				return VideoProjectionConfig{}, err
			}
			projection.PoseYaw, err = readProjectionPosePayload(master.Reader(), child.Size.Value, -180, 180)
			if err != nil {
				return VideoProjectionConfig{}, err
			}
		case idProjectionPosePitch:
			if err := markElementSeen(&posePitchSeen); err != nil {
				return VideoProjectionConfig{}, err
			}
			projection.PosePitch, err = readProjectionPosePayload(master.Reader(), child.Size.Value, -90, 90)
			if err != nil {
				return VideoProjectionConfig{}, err
			}
		case idProjectionPoseRoll:
			if err := markElementSeen(&poseRollSeen); err != nil {
				return VideoProjectionConfig{}, err
			}
			projection.PoseRoll, err = readProjectionPosePayload(master.Reader(), child.Size.Value, -180, 180)
			if err != nil {
				return VideoProjectionConfig{}, err
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return VideoProjectionConfig{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return VideoProjectionConfig{}, err
	}
	if err := validateVideoProjection(projection); err != nil {
		return VideoProjectionConfig{}, ErrInvalidData
	}
	return projection, nil
}

func (d *Demuxer) parseColour(parent io.Reader, header ebml.Header) (VideoColourConfig, error) {
	if header.Size.Unknown {
		return VideoColourConfig{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return VideoColourConfig{}, err
	}
	var colour VideoColourConfig
	var seen uint64
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return VideoColourConfig{}, err
		}
		r := master.Reader()
		sz := child.Size.Value
		switch child.ID {
		case idMatrixCoefficients:
			err = readOnceIntFromUInt(r, sz, &seen, 0, &colour.MatrixCoefficients, &colour.MatrixCoefficientsSet)
		case idBitsPerChannel:
			err = readOnceIntFromUInt(r, sz, &seen, 1, &colour.BitsPerChannel, &colour.BitsPerChannelSet)
		case idChromaSubsampleHorz:
			err = readOnceIntFromUInt(r, sz, &seen, 2, &colour.ChromaSubsamplingHorz, &colour.ChromaSubsamplingHorzSet)
		case idChromaSubsampleVert:
			err = readOnceIntFromUInt(r, sz, &seen, 3, &colour.ChromaSubsamplingVert, &colour.ChromaSubsamplingVertSet)
		case idCbSubsampleHorz:
			err = readOnceIntFromUInt(r, sz, &seen, 4, &colour.CbSubsamplingHorz, &colour.CbSubsamplingHorzSet)
		case idCbSubsampleVert:
			err = readOnceIntFromUInt(r, sz, &seen, 5, &colour.CbSubsamplingVert, &colour.CbSubsamplingVertSet)
		case idChromaSitingHorz:
			err = readOnceIntFromUInt(r, sz, &seen, 6, &colour.ChromaSitingHorz, &colour.ChromaSitingHorzSet)
		case idChromaSitingVert:
			err = readOnceIntFromUInt(r, sz, &seen, 7, &colour.ChromaSitingVert, &colour.ChromaSitingVertSet)
		case idColourRange:
			err = readOnceIntFromUInt(r, sz, &seen, 8, &colour.Range, &colour.RangeSet)
		case idTransferChar:
			err = readOnceIntFromUInt(r, sz, &seen, 9, &colour.TransferCharacteristics, &colour.TransferCharacteristicsSet)
		case idPrimaries:
			err = readOnceIntFromUInt(r, sz, &seen, 10, &colour.Primaries, &colour.PrimariesSet)
		case idMaxCLL:
			err = readOnceIntFromUInt(r, sz, &seen, 11, &colour.MaxCLL, &colour.MaxCLLSet)
		case idMaxFALL:
			err = readOnceIntFromUInt(r, sz, &seen, 12, &colour.MaxFALL, &colour.MaxFALLSet)
		case idMasteringMetadata:
			if err = markSeenBit(&seen, 13); err == nil {
				colour.MasteringMetadata, err = d.parseMasteringMetadata(r, child)
			}
		default:
			err = skipElement(r, child)
		}
		if err != nil {
			return VideoColourConfig{}, err
		}
	}
	if err := master.Validate(); err != nil {
		return VideoColourConfig{}, err
	}
	return colour, nil
}

func (d *Demuxer) parseMasteringMetadata(parent io.Reader, header ebml.Header) (VideoMasteringMetadataConfig, error) {
	if header.Size.Unknown {
		return VideoMasteringMetadataConfig{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return VideoMasteringMetadataConfig{}, err
	}
	var metadata VideoMasteringMetadataConfig
	var seen uint64
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return VideoMasteringMetadataConfig{}, err
		}
		r := master.Reader()
		sz := child.Size.Value
		switch child.ID {
		case idPrimaryRX:
			err = readOnceFloat(r, sz, &seen, 0, &metadata.PrimaryRChromaticityX, &metadata.PrimaryRChromaticityXSet, readChromaticityPayload)
		case idPrimaryRY:
			err = readOnceFloat(r, sz, &seen, 1, &metadata.PrimaryRChromaticityY, &metadata.PrimaryRChromaticityYSet, readChromaticityPayload)
		case idPrimaryGX:
			err = readOnceFloat(r, sz, &seen, 2, &metadata.PrimaryGChromaticityX, &metadata.PrimaryGChromaticityXSet, readChromaticityPayload)
		case idPrimaryGY:
			err = readOnceFloat(r, sz, &seen, 3, &metadata.PrimaryGChromaticityY, &metadata.PrimaryGChromaticityYSet, readChromaticityPayload)
		case idPrimaryBX:
			err = readOnceFloat(r, sz, &seen, 4, &metadata.PrimaryBChromaticityX, &metadata.PrimaryBChromaticityXSet, readChromaticityPayload)
		case idPrimaryBY:
			err = readOnceFloat(r, sz, &seen, 5, &metadata.PrimaryBChromaticityY, &metadata.PrimaryBChromaticityYSet, readChromaticityPayload)
		case idWhitePointX:
			err = readOnceFloat(r, sz, &seen, 6, &metadata.WhitePointChromaticityX, &metadata.WhitePointChromaticityXSet, readChromaticityPayload)
		case idWhitePointY:
			err = readOnceFloat(r, sz, &seen, 7, &metadata.WhitePointChromaticityY, &metadata.WhitePointChromaticityYSet, readChromaticityPayload)
		case idLuminanceMax:
			err = readOnceFloat(r, sz, &seen, 8, &metadata.LuminanceMax, &metadata.LuminanceMaxSet, readNonNegativeFloatPayload)
		case idLuminanceMin:
			err = readOnceFloat(r, sz, &seen, 9, &metadata.LuminanceMin, &metadata.LuminanceMinSet, readNonNegativeFloatPayload)
		default:
			err = skipElement(r, child)
		}
		if err != nil {
			return VideoMasteringMetadataConfig{}, err
		}
	}
	if err := master.Validate(); err != nil {
		return VideoMasteringMetadataConfig{}, err
	}
	return metadata, nil
}

func (d *Demuxer) parseAudio(parent io.Reader, header ebml.Header) (AudioConfig, error) {
	if header.Size.Unknown {
		return AudioConfig{}, ErrInvalidData
	}
	master, err := d.checkedMasterReader(parent, header.Size.Value)
	if err != nil {
		return AudioConfig{}, err
	}
	audio := AudioConfig{SampleRate: 48000, Channels: 2}
	sampleRateSeen := false
	outputSampleRateSeen := false
	channelsSeen := false
	bitDepthSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return AudioConfig{}, err
		}
		switch child.ID {
		case idSamplingFreq:
			if err := markElementSeen(&sampleRateSeen); err != nil {
				return AudioConfig{}, err
			}
			value, err := readFloatPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return AudioConfig{}, err
			}
			if invalidAudioFrequency(value) {
				return AudioConfig{}, ErrInvalidData
			}
			audio.SampleRate = int(value)
		case idOutputFreq:
			if err := markElementSeen(&outputSampleRateSeen); err != nil {
				return AudioConfig{}, err
			}
			value, err := readFloatPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return AudioConfig{}, err
			}
			if invalidAudioFrequency(value) {
				return AudioConfig{}, ErrInvalidData
			}
			audio.OutputSampleRate = int(value)
		case idChannels:
			if err := markElementSeen(&channelsSeen); err != nil {
				return AudioConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return AudioConfig{}, err
			}
			audio.Channels, err = intFromUint(value)
			if err != nil {
				return AudioConfig{}, err
			}
		case idBitDepth:
			if err := markElementSeen(&bitDepthSeen); err != nil {
				return AudioConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return AudioConfig{}, err
			}
			audio.BitDepth, err = intFromUint(value)
			if err != nil {
				return AudioConfig{}, err
			}
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return AudioConfig{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return AudioConfig{}, err
	}
	return audio, nil
}

func invalidAudioFrequency(value float64) bool {
	return math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 || value > float64(maxIntValue)
}
