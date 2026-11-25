package matroska

import (
	"io"
	"math"

	"github.com/thesyncim/goav/container/ebml"
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
	trackNumberSeen := false
	trackUIDSeen := false
	trackTypeSeen := false
	codecIDSeen := false
	flagEnabledSeen := false
	flagDefaultSeen := false
	flagForcedSeen := false
	flagHearingImpairedSeen := false
	flagVisualImpairedSeen := false
	flagTextDescriptionsSeen := false
	flagOriginalSeen := false
	flagCommentarySeen := false
	flagLacingSeen := false
	nameSeen := false
	languageSeen := false
	languageBCPSeen := false
	codecNameSeen := false
	minCacheSeen := false
	maxCacheSeen := false
	defaultDurationSeen := false
	defaultDecodedDurationSeen := false
	trackTimestampScaleSeen := false
	trackOffsetSeen := false
	maxBlockAdditionIDSeen := false
	codecDecodeAllSeen := false
	codecDelaySeen := false
	seekPreRollSeen := false
	contentEncodingsSeen := false
	codecPrivateSeen := false
	videoSeen := false
	audioSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return Track{}, err
		}
		switch child.ID {
		case idTrackNumber:
			if trackNumberSeen {
				return Track{}, ErrInvalidData
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			trackID, err := trackIDFromUint(value)
			if err != nil {
				return Track{}, err
			}
			track.ID = trackID
			trackNumberSeen = true
		case idTrackUID:
			if trackUIDSeen {
				return Track{}, ErrInvalidData
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			if value == 0 {
				return Track{}, ErrInvalidData
			}
			track.UID = value
			trackUIDSeen = true
		case idTrackType:
			if trackTypeSeen {
				return Track{}, ErrInvalidData
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
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
			trackTypeSeen = true
		case idFlagEnabled:
			if err := markElementSeen(&flagEnabledSeen); err != nil {
				return Track{}, err
			}
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagEnabled = value
			track.FlagEnabledSet = true
		case idFlagDefault:
			if err := markElementSeen(&flagDefaultSeen); err != nil {
				return Track{}, err
			}
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagDefault = value
			track.FlagDefaultSet = true
		case idFlagForced:
			if err := markElementSeen(&flagForcedSeen); err != nil {
				return Track{}, err
			}
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagForced = value
			track.FlagForcedSet = true
		case idFlagHearingImpaired:
			if err := markElementSeen(&flagHearingImpairedSeen); err != nil {
				return Track{}, err
			}
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagHearingImpaired = value
			track.FlagHearingImpairedSet = true
		case idFlagVisualImpaired:
			if err := markElementSeen(&flagVisualImpairedSeen); err != nil {
				return Track{}, err
			}
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagVisualImpaired = value
			track.FlagVisualImpairedSet = true
		case idFlagTextDescriptions:
			if err := markElementSeen(&flagTextDescriptionsSeen); err != nil {
				return Track{}, err
			}
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagTextDescriptions = value
			track.FlagTextDescriptionsSet = true
		case idFlagOriginal:
			if err := markElementSeen(&flagOriginalSeen); err != nil {
				return Track{}, err
			}
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagOriginal = value
			track.FlagOriginalSet = true
		case idFlagCommentary:
			if err := markElementSeen(&flagCommentarySeen); err != nil {
				return Track{}, err
			}
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagCommentary = value
			track.FlagCommentarySet = true
		case idFlagLacing:
			if err := markElementSeen(&flagLacingSeen); err != nil {
				return Track{}, err
			}
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.FlagLacing = value
			track.FlagLacingSet = true
		case idName:
			if err := markElementSeen(&nameSeen); err != nil {
				return Track{}, err
			}
			value, err := readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.Name = value
		case idLanguage:
			if err := markElementSeen(&languageSeen); err != nil {
				return Track{}, err
			}
			value, err := readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.Language = value
		case idLanguageBCP:
			if err := markElementSeen(&languageBCPSeen); err != nil {
				return Track{}, err
			}
			value, err := readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.LanguageBCP47 = value
		case idCodecID:
			if codecIDSeen {
				return Track{}, ErrInvalidData
			}
			value, err := readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			codecID = value
			codecIDSeen = true
		case idCodecName:
			if err := markElementSeen(&codecNameSeen); err != nil {
				return Track{}, err
			}
			value, err := readStringPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.CodecName = value
		case idMinCache:
			if err := markElementSeen(&minCacheSeen); err != nil {
				return Track{}, err
			}
			track.MinCache, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.MinCacheSet = true
		case idMaxCache:
			if err := markElementSeen(&maxCacheSeen); err != nil {
				return Track{}, err
			}
			track.MaxCache, err = readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.MaxCacheSet = true
		case idDefaultDur:
			if err := markElementSeen(&defaultDurationSeen); err != nil {
				return Track{}, err
			}
			value, err := readNonZeroInt64Payload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.DefaultDurationNS = value
		case idDefaultDecodedDur:
			if err := markElementSeen(&defaultDecodedDurationSeen); err != nil {
				return Track{}, err
			}
			value, err := readNonZeroInt64Payload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.DefaultDecodedFieldDurationNS = value
		case idTrackTimestampScale:
			if err := markElementSeen(&trackTimestampScaleSeen); err != nil {
				return Track{}, err
			}
			value, err := readFloatPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
				return Track{}, ErrInvalidData
			}
			track.TrackTimestampScale = value
			track.TrackTimestampScaleSet = true
		case idTrackOffset:
			if err := markElementSeen(&trackOffsetSeen); err != nil {
				return Track{}, err
			}
			value, err := readIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.TrackOffsetNS = value
			track.TrackOffsetSet = true
		case idMaxBlockAdditionID:
			if err := markElementSeen(&maxBlockAdditionIDSeen); err != nil {
				return Track{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.MaxBlockAdditionID = value
		case idBlockAdditionMapping:
			mapping, err := d.parseBlockAdditionMapping(master.Reader(), child)
			if err != nil {
				return Track{}, err
			}
			track.BlockAdditionMappings = append(track.BlockAdditionMappings, mapping)
		case idCodecDecodeAll:
			if err := markElementSeen(&codecDecodeAllSeen); err != nil {
				return Track{}, err
			}
			value, err := readBoolFlagPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.CodecDecodeAll = value
			track.CodecDecodeAllSet = true
		case idTrackOverlay:
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			if value == 0 {
				return Track{}, ErrInvalidData
			}
			track.TrackOverlays = append(track.TrackOverlays, value)
		case idCodecDelay:
			if err := markElementSeen(&codecDelaySeen); err != nil {
				return Track{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			if value > uint64(math.MaxInt64) {
				return Track{}, ErrInvalidData
			}
			track.CodecDelayNS = int64(value)
		case idSeekPreRoll:
			if err := markElementSeen(&seekPreRollSeen); err != nil {
				return Track{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			if value > uint64(math.MaxInt64) {
				return Track{}, ErrInvalidData
			}
			track.SeekPreRollNS = int64(value)
		case idTrackTranslate:
			translate, err := d.parseTrackTranslate(master.Reader(), child)
			if err != nil {
				return Track{}, err
			}
			track.TrackTranslates = append(track.TrackTranslates, translate)
		case idContentEncodings:
			if err := markElementSeen(&contentEncodingsSeen); err != nil {
				return Track{}, err
			}
			encodings, err := d.parseContentEncodings(master.Reader(), child)
			if err != nil {
				return Track{}, err
			}
			track.ContentEncodings = encodings
		case idCodecPrivate:
			if err := markElementSeen(&codecPrivateSeen); err != nil {
				return Track{}, err
			}
			value, err := readBinaryPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.CodecPrivate = value
		case idVideo:
			if videoSeen {
				return Track{}, ErrInvalidData
			}
			video, err := d.parseVideo(master.Reader(), child)
			if err != nil {
				return Track{}, err
			}
			track.Video = video
			videoSeen = true
		case idAudio:
			if audioSeen {
				return Track{}, ErrInvalidData
			}
			audio, err := d.parseAudio(master.Reader(), child)
			if err != nil {
				return Track{}, err
			}
			track.Audio = audio
			audioSeen = true
		default:
			if err := appendUnknownChildElement(master.Reader(), child, isKnownTrackEntryElement, &track.UnknownElements); err != nil {
				return Track{}, err
			}
		}
	}
	if err := master.Validate(); err != nil {
		return Track{}, err
	}
	if !trackNumberSeen || !trackTypeSeen || !codecIDSeen {
		return Track{}, ErrInvalidData
	}
	if (track.Type == TrackVideo && !videoSeen) || (track.Type == TrackAudio && !audioSeen) {
		return Track{}, ErrInvalidData
	}
	if track.UID == 0 {
		track.UID = uint64(track.ID)
	}
	track.Codec = codecFromMatroskaID(codecID, track.CodecPrivate)
	if codecID == codecIDMS {
		format, err := parseMSACMWaveFormat(track.CodecPrivate)
		if err != nil {
			return Track{}, err
		}
		track.Codec = codecFromMSACMTag(format.FormatTag)
		track.Audio.SampleRate = format.SampleRate
		track.Audio.Channels = format.Channels
		track.Audio.BitDepth = format.BitsPerSample
		if track.Codec == CodecPCMU || track.Codec == CodecPCMA {
			if err := validateG711MSACMWaveFormat(format, track.Codec); err != nil {
				return Track{}, err
			}
		}
	}
	if track.Codec == CodecOpus && len(track.CodecPrivate) != 0 {
		head, err := parseOpusHead(track.CodecPrivate)
		if err != nil {
			return Track{}, err
		}
		track.Audio.Channels = head.Channels
		if track.CodecDelayNS == 0 {
			track.CodecDelayNS = opusCodecDelayNS(head.PreSkip)
		}
		if track.SeekPreRollNS == 0 {
			track.SeekPreRollNS = opusDefaultSeekPreRollNS
		}
	}
	if track.Codec == CodecVorbis {
		private, err := parseVorbisCodecPrivate(track.CodecPrivate)
		if err != nil {
			return Track{}, err
		}
		track.Audio.Channels = private.Channels
		track.Audio.SampleRate = private.SampleRate
	}
	if track.Codec == CodecFLAC {
		private, err := parseFLACCodecPrivate(track.CodecPrivate)
		if err != nil {
			return Track{}, err
		}
		track.Audio.Channels = private.Channels
		track.Audio.SampleRate = private.SampleRate
		track.Audio.BitDepth = private.BitsPerSample
	}
	if track.Codec == CodecAAC {
		private, err := parseAACAudioSpecificConfig(track.CodecPrivate)
		if err != nil {
			return Track{}, err
		}
		track.Audio.Channels = private.Channels
		track.Audio.SampleRate = private.SampleRate
	}
	if track.Codec == CodecAV1 && len(track.CodecPrivate) != 0 {
		if _, err := parseAV1CodecConfigurationRecord(track.CodecPrivate); err != nil {
			return Track{}, err
		}
	}
	if track.Codec == CodecH264 && len(track.CodecPrivate) != 0 {
		if _, err := parseAVCDecoderConfigurationRecord(track.CodecPrivate); err != nil {
			return Track{}, err
		}
	}
	if track.Codec == CodecH265 && len(track.CodecPrivate) != 0 {
		if _, err := parseHEVCDecoderConfigurationRecord(track.CodecPrivate); err != nil {
			return Track{}, err
		}
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
	flagInterlacedSeen := false
	fieldOrderSeen := false
	stereoModeSeen := false
	alphaModeSeen := false
	pixelWidthSeen := false
	pixelHeightSeen := false
	pixelCropBottomSeen := false
	pixelCropTopSeen := false
	pixelCropLeftSeen := false
	pixelCropRightSeen := false
	displayWidthSeen := false
	displayHeightSeen := false
	displayUnitSeen := false
	aspectRatioTypeSeen := false
	colourSeen := false
	projectionSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return VideoConfig{}, err
		}
		switch child.ID {
		case idFlagInterlaced:
			if err := markElementSeen(&flagInterlacedSeen); err != nil {
				return VideoConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.FlagInterlaced, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.FlagInterlacedSet = true
		case idFieldOrder:
			if err := markElementSeen(&fieldOrderSeen); err != nil {
				return VideoConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.FieldOrder, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.FieldOrderSet = true
		case idStereoMode:
			if err := markElementSeen(&stereoModeSeen); err != nil {
				return VideoConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.StereoMode, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.StereoModeSet = true
		case idAlphaMode:
			if err := markElementSeen(&alphaModeSeen); err != nil {
				return VideoConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.AlphaMode, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.AlphaModeSet = true
		case idPixelWidth:
			if err := markElementSeen(&pixelWidthSeen); err != nil {
				return VideoConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.Width, err = nonZeroIntFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idPixelHeight:
			if err := markElementSeen(&pixelHeightSeen); err != nil {
				return VideoConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.Height, err = nonZeroIntFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idPixelCropBottom:
			if err := markElementSeen(&pixelCropBottomSeen); err != nil {
				return VideoConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.PixelCropBottom, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idPixelCropTop:
			if err := markElementSeen(&pixelCropTopSeen); err != nil {
				return VideoConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.PixelCropTop, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idPixelCropLeft:
			if err := markElementSeen(&pixelCropLeftSeen); err != nil {
				return VideoConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.PixelCropLeft, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idPixelCropRight:
			if err := markElementSeen(&pixelCropRightSeen); err != nil {
				return VideoConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.PixelCropRight, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idDisplayWidth:
			if err := markElementSeen(&displayWidthSeen); err != nil {
				return VideoConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.DisplayWidth, err = nonZeroIntFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idDisplayHeight:
			if err := markElementSeen(&displayHeightSeen); err != nil {
				return VideoConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.DisplayHeight, err = nonZeroIntFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idDisplayUnit:
			if err := markElementSeen(&displayUnitSeen); err != nil {
				return VideoConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.DisplayUnit, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
		case idAspectRatioType:
			if err := markElementSeen(&aspectRatioTypeSeen); err != nil {
				return VideoConfig{}, err
			}
			value, err := readUIntPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.AspectRatioType, err = intFromUint(value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.AspectRatioTypeSet = true
		case idColour:
			if err := markElementSeen(&colourSeen); err != nil {
				return VideoConfig{}, err
			}
			colour, err := d.parseColour(master.Reader(), child)
			if err != nil {
				return VideoConfig{}, err
			}
			video.Colour = colour
		case idProjection:
			if err := markElementSeen(&projectionSeen); err != nil {
				return VideoConfig{}, err
			}
			projection, err := d.parseProjection(master.Reader(), child)
			if err != nil {
				return VideoConfig{}, err
			}
			video.Projection = projection
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return VideoConfig{}, err
			}
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
	matrixSeen := false
	bitsPerChannelSeen := false
	chromaSubsamplingHorzSeen := false
	chromaSubsamplingVertSeen := false
	cbSubsamplingHorzSeen := false
	cbSubsamplingVertSeen := false
	chromaSitingHorzSeen := false
	chromaSitingVertSeen := false
	rangeSeen := false
	transferSeen := false
	primariesSeen := false
	maxCLLSeen := false
	maxFALLSeen := false
	masteringSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return VideoColourConfig{}, err
		}
		switch child.ID {
		case idMatrixCoefficients:
			if err := markElementSeen(&matrixSeen); err != nil {
				return VideoColourConfig{}, err
			}
			colour.MatrixCoefficients, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.MatrixCoefficientsSet = true
		case idBitsPerChannel:
			if err := markElementSeen(&bitsPerChannelSeen); err != nil {
				return VideoColourConfig{}, err
			}
			colour.BitsPerChannel, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.BitsPerChannelSet = true
		case idChromaSubsampleHorz:
			if err := markElementSeen(&chromaSubsamplingHorzSeen); err != nil {
				return VideoColourConfig{}, err
			}
			colour.ChromaSubsamplingHorz, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.ChromaSubsamplingHorzSet = true
		case idChromaSubsampleVert:
			if err := markElementSeen(&chromaSubsamplingVertSeen); err != nil {
				return VideoColourConfig{}, err
			}
			colour.ChromaSubsamplingVert, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.ChromaSubsamplingVertSet = true
		case idCbSubsampleHorz:
			if err := markElementSeen(&cbSubsamplingHorzSeen); err != nil {
				return VideoColourConfig{}, err
			}
			colour.CbSubsamplingHorz, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.CbSubsamplingHorzSet = true
		case idCbSubsampleVert:
			if err := markElementSeen(&cbSubsamplingVertSeen); err != nil {
				return VideoColourConfig{}, err
			}
			colour.CbSubsamplingVert, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.CbSubsamplingVertSet = true
		case idChromaSitingHorz:
			if err := markElementSeen(&chromaSitingHorzSeen); err != nil {
				return VideoColourConfig{}, err
			}
			colour.ChromaSitingHorz, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.ChromaSitingHorzSet = true
		case idChromaSitingVert:
			if err := markElementSeen(&chromaSitingVertSeen); err != nil {
				return VideoColourConfig{}, err
			}
			colour.ChromaSitingVert, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.ChromaSitingVertSet = true
		case idColourRange:
			if err := markElementSeen(&rangeSeen); err != nil {
				return VideoColourConfig{}, err
			}
			colour.Range, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.RangeSet = true
		case idTransferChar:
			if err := markElementSeen(&transferSeen); err != nil {
				return VideoColourConfig{}, err
			}
			colour.TransferCharacteristics, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.TransferCharacteristicsSet = true
		case idPrimaries:
			if err := markElementSeen(&primariesSeen); err != nil {
				return VideoColourConfig{}, err
			}
			colour.Primaries, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.PrimariesSet = true
		case idMaxCLL:
			if err := markElementSeen(&maxCLLSeen); err != nil {
				return VideoColourConfig{}, err
			}
			colour.MaxCLL, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.MaxCLLSet = true
		case idMaxFALL:
			if err := markElementSeen(&maxFALLSeen); err != nil {
				return VideoColourConfig{}, err
			}
			colour.MaxFALL, err = readIntPayloadFromUInt(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.MaxFALLSet = true
		case idMasteringMetadata:
			if err := markElementSeen(&masteringSeen); err != nil {
				return VideoColourConfig{}, err
			}
			metadata, err := d.parseMasteringMetadata(master.Reader(), child)
			if err != nil {
				return VideoColourConfig{}, err
			}
			colour.MasteringMetadata = metadata
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return VideoColourConfig{}, err
			}
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
	primaryRXSeen := false
	primaryRYSeen := false
	primaryGXSeen := false
	primaryGYSeen := false
	primaryBXSeen := false
	primaryBYSeen := false
	whitePointXSeen := false
	whitePointYSeen := false
	luminanceMaxSeen := false
	luminanceMinSeen := false
	for !master.Done() {
		child, err := master.ReadHeader()
		if err != nil {
			return VideoMasteringMetadataConfig{}, err
		}
		switch child.ID {
		case idPrimaryRX:
			if err := markElementSeen(&primaryRXSeen); err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryRChromaticityX, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryRChromaticityXSet = true
		case idPrimaryRY:
			if err := markElementSeen(&primaryRYSeen); err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryRChromaticityY, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryRChromaticityYSet = true
		case idPrimaryGX:
			if err := markElementSeen(&primaryGXSeen); err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryGChromaticityX, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryGChromaticityXSet = true
		case idPrimaryGY:
			if err := markElementSeen(&primaryGYSeen); err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryGChromaticityY, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryGChromaticityYSet = true
		case idPrimaryBX:
			if err := markElementSeen(&primaryBXSeen); err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryBChromaticityX, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryBChromaticityXSet = true
		case idPrimaryBY:
			if err := markElementSeen(&primaryBYSeen); err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryBChromaticityY, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.PrimaryBChromaticityYSet = true
		case idWhitePointX:
			if err := markElementSeen(&whitePointXSeen); err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.WhitePointChromaticityX, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.WhitePointChromaticityXSet = true
		case idWhitePointY:
			if err := markElementSeen(&whitePointYSeen); err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.WhitePointChromaticityY, err = readChromaticityPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.WhitePointChromaticityYSet = true
		case idLuminanceMax:
			if err := markElementSeen(&luminanceMaxSeen); err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.LuminanceMax, err = readNonNegativeFloatPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.LuminanceMaxSet = true
		case idLuminanceMin:
			if err := markElementSeen(&luminanceMinSeen); err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.LuminanceMin, err = readNonNegativeFloatPayload(master.Reader(), child.Size.Value)
			if err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
			metadata.LuminanceMinSet = true
		default:
			if err := skipElement(master.Reader(), child); err != nil {
				return VideoMasteringMetadataConfig{}, err
			}
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
