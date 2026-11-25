package matroska

import (
	"bytes"
	"math"
	"slices"

	"github.com/thesyncim/goav/container/ebml"
)

func writeTrackEntry(w *ebml.Writer, track Track, scratch *[codecPrivateScratchSize]byte) error {
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	if err := tw.WriteUInt(idTrackNumber, uint64(track.ID)); err != nil {
		return err
	}
	if err := tw.WriteUInt(idTrackUID, track.UID); err != nil {
		return err
	}
	switch track.Type {
	case TrackVideo:
		if err := tw.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
			return err
		}
	case TrackAudio:
		if err := tw.WriteUInt(idTrackType, matroskaTrackAudio); err != nil {
			return err
		}
	case TrackSubtitle:
		if err := tw.WriteUInt(idTrackType, matroskaTrackSubtitle); err != nil {
			return err
		}
	default:
		return ErrInvalidTrack
	}
	if err := tw.WriteUInt(idFlagEnabled, boolFlagUInt(track.FlagEnabled)); err != nil {
		return err
	}
	if err := tw.WriteUInt(idFlagDefault, boolFlagUInt(track.FlagDefault)); err != nil {
		return err
	}
	if err := tw.WriteUInt(idFlagForced, boolFlagUInt(track.FlagForced)); err != nil {
		return err
	}
	if err := writeOptionalBoolFlag(tw, idFlagHearingImpaired, track.FlagHearingImpaired, track.FlagHearingImpairedSet); err != nil {
		return err
	}
	if err := writeOptionalBoolFlag(tw, idFlagVisualImpaired, track.FlagVisualImpaired, track.FlagVisualImpairedSet); err != nil {
		return err
	}
	if err := writeOptionalBoolFlag(tw, idFlagTextDescriptions, track.FlagTextDescriptions, track.FlagTextDescriptionsSet); err != nil {
		return err
	}
	if err := writeOptionalBoolFlag(tw, idFlagOriginal, track.FlagOriginal, track.FlagOriginalSet); err != nil {
		return err
	}
	if err := writeOptionalBoolFlag(tw, idFlagCommentary, track.FlagCommentary, track.FlagCommentarySet); err != nil {
		return err
	}
	if err := tw.WriteUInt(idFlagLacing, boolFlagUInt(track.FlagLacing)); err != nil {
		return err
	}
	if track.Name != "" {
		if err := tw.WriteString(idName, track.Name); err != nil {
			return err
		}
	}
	if track.Language != "" {
		if err := tw.WriteString(idLanguage, track.Language); err != nil {
			return err
		}
	}
	if track.LanguageBCP47 != "" {
		if err := tw.WriteString(idLanguageBCP, track.LanguageBCP47); err != nil {
			return err
		}
	}
	codecID, err := matroskaCodecID(track.Codec)
	if err != nil {
		return err
	}
	if err := tw.WriteString(idCodecID, codecID); err != nil {
		return err
	}
	if track.CodecName != "" {
		if err := tw.WriteString(idCodecName, track.CodecName); err != nil {
			return err
		}
	}
	if track.MinCacheSet || track.MinCache != 0 {
		if err := tw.WriteUInt(idMinCache, track.MinCache); err != nil {
			return err
		}
	}
	if track.MaxCacheSet {
		if err := tw.WriteUInt(idMaxCache, track.MaxCache); err != nil {
			return err
		}
	}
	if track.DefaultDurationNS > 0 {
		if err := tw.WriteUInt(idDefaultDur, uint64(track.DefaultDurationNS)); err != nil {
			return err
		}
	}
	if track.DefaultDecodedFieldDurationNS > 0 {
		if err := tw.WriteUInt(idDefaultDecodedDur, uint64(track.DefaultDecodedFieldDurationNS)); err != nil {
			return err
		}
	}
	if track.TrackTimestampScaleSet || track.TrackTimestampScale != 0 {
		if err := tw.WriteFloat64(idTrackTimestampScale, trackTimestampScale(track)); err != nil {
			return err
		}
	}
	if track.TrackOffsetSet || track.TrackOffsetNS != 0 {
		if err := tw.WriteInt(idTrackOffset, track.TrackOffsetNS); err != nil {
			return err
		}
	}
	if track.MaxBlockAdditionID != 0 {
		if err := tw.WriteUInt(idMaxBlockAdditionID, track.MaxBlockAdditionID); err != nil {
			return err
		}
	}
	for i := range track.BlockAdditionMappings {
		if err := writeBlockAdditionMapping(tw, track.BlockAdditionMappings[i]); err != nil {
			return err
		}
	}
	if track.CodecDecodeAllSet {
		if err := tw.WriteUInt(idCodecDecodeAll, boolFlagUInt(track.CodecDecodeAll)); err != nil {
			return err
		}
	}
	for i := range track.TrackOverlays {
		if err := tw.WriteUInt(idTrackOverlay, track.TrackOverlays[i]); err != nil {
			return err
		}
	}
	private := track.CodecPrivate
	if track.Type == TrackAudio {
		normalizeAudioDefaults(track.Codec, &track.Audio)
	}
	if len(private) == 0 {
		private = defaultCodecPrivate(track, scratch)
	}
	codecDelayNS, seekPreRollNS, err := trackCodecTiming(track, private)
	if err != nil {
		return err
	}
	if codecDelayNS > 0 || track.Codec == CodecOpus {
		if err := tw.WriteUInt(idCodecDelay, uint64(codecDelayNS)); err != nil {
			return err
		}
	}
	if seekPreRollNS > 0 || track.Codec == CodecOpus {
		if err := tw.WriteUInt(idSeekPreRoll, uint64(seekPreRollNS)); err != nil {
			return err
		}
	}
	for i := range track.TrackTranslates {
		if err := writeTrackTranslate(tw, track.TrackTranslates[i]); err != nil {
			return err
		}
	}
	if len(track.ContentEncodings) != 0 {
		if err := writeContentEncodings(tw, track.ContentEncodings); err != nil {
			return err
		}
	}
	if len(private) != 0 {
		if err := writeBinary(tw, idCodecPrivate, private); err != nil {
			return err
		}
	}
	switch track.Type {
	case TrackVideo:
		if err := writeVideo(tw, track.Video); err != nil {
			return err
		}
	case TrackAudio:
		if err := writeAudio(tw, track.Audio); err != nil {
			return err
		}
	}
	if err := writeUnknownElements(tw, track.UnknownElements); err != nil {
		return err
	}
	return w.WriteElement(idTrackEntry, payload.Bytes())
}

func writeTrackTranslate(w *ebml.Writer, translate TrackTranslate) error {
	if err := validateTrackTranslate(translate); err != nil {
		return err
	}
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	if err := writeBinary(tw, idTrackTranslateTrack, translate.TrackID); err != nil {
		return err
	}
	if err := tw.WriteUInt(idTrackTranslateCodec, translate.Codec); err != nil {
		return err
	}
	for i := range translate.EditionUIDs {
		if err := tw.WriteUInt(idTrackTranslateEdit, translate.EditionUIDs[i]); err != nil {
			return err
		}
	}
	return w.WriteElement(idTrackTranslate, payload.Bytes())
}

func writeBlockAdditionMapping(w *ebml.Writer, mapping BlockAdditionMapping) error {
	var payload bytes.Buffer
	mw := ebml.NewWriter(&payload)
	if err := mw.WriteUInt(idBlockAddIDValue, mapping.IDValue); err != nil {
		return err
	}
	if mapping.Name != "" {
		if err := mw.WriteString(idBlockAddIDName, mapping.Name); err != nil {
			return err
		}
	}
	if err := mw.WriteUInt(idBlockAddIDType, mapping.Type); err != nil {
		return err
	}
	if mapping.ExtraData != nil {
		if err := writeBinary(mw, idBlockAddIDExtraData, mapping.ExtraData); err != nil {
			return err
		}
	}
	return w.WriteElement(idBlockAdditionMapping, payload.Bytes())
}

func writeContentEncodings(w *ebml.Writer, encodings []ContentEncoding) error {
	if err := validateContentEncodings(encodings); err != nil {
		return err
	}
	var payload bytes.Buffer
	cw := ebml.NewWriter(&payload)
	for i := range encodings {
		if err := writeContentEncoding(cw, encodings[i]); err != nil {
			return err
		}
	}
	return w.WriteElement(idContentEncodings, payload.Bytes())
}

func writeContentEncoding(w *ebml.Writer, encoding ContentEncoding) error {
	var payload bytes.Buffer
	cw := ebml.NewWriter(&payload)
	if err := cw.WriteUInt(idContentEncodingOrd, encoding.Order); err != nil {
		return err
	}
	if err := cw.WriteUInt(idContentEncodingScope, contentEncodingScope(encoding.Scope)); err != nil {
		return err
	}
	if err := cw.WriteUInt(idContentEncodingType, encoding.Type); err != nil {
		return err
	}
	switch encoding.Type {
	case ContentEncodingTypeCompression:
		if err := writeContentCompression(cw, encoding.Compression); err != nil {
			return err
		}
	case ContentEncodingTypeEncryption:
		if err := writeContentEncryption(cw, encoding.Encryption); err != nil {
			return err
		}
	default:
		return ErrInvalidTrack
	}
	return w.WriteElement(idContentEncoding, payload.Bytes())
}

func writeContentCompression(w *ebml.Writer, compression ContentCompression) error {
	var payload bytes.Buffer
	cw := ebml.NewWriter(&payload)
	if err := cw.WriteUInt(idContentCompAlgo, compression.Algorithm); err != nil {
		return err
	}
	if compression.Settings != nil {
		if err := writeBinary(cw, idContentCompSettings, compression.Settings); err != nil {
			return err
		}
	}
	return w.WriteElement(idContentCompression, payload.Bytes())
}

func writeContentEncryption(w *ebml.Writer, encryption ContentEncryption) error {
	var payload bytes.Buffer
	ew := ebml.NewWriter(&payload)
	if err := ew.WriteUInt(idContentEncAlgo, encryption.Algorithm); err != nil {
		return err
	}
	if encryption.KeyID != nil {
		if err := writeBinary(ew, idContentEncKeyID, encryption.KeyID); err != nil {
			return err
		}
	}
	if encryption.AESSettingsSet {
		if err := writeContentEncAESSettings(ew, encryption.AESSettings); err != nil {
			return err
		}
	}
	if encryption.Signature != nil {
		if err := writeBinary(ew, idContentSignature, encryption.Signature); err != nil {
			return err
		}
	}
	if encryption.SignatureKeyID != nil {
		if err := writeBinary(ew, idContentSigKeyID, encryption.SignatureKeyID); err != nil {
			return err
		}
	}
	if encryption.SignatureAlgorithm != 0 {
		if err := ew.WriteUInt(idContentSigAlgo, encryption.SignatureAlgorithm); err != nil {
			return err
		}
	}
	if encryption.SignatureHashAlgorithm != 0 {
		if err := ew.WriteUInt(idContentSigHashAlgo, encryption.SignatureHashAlgorithm); err != nil {
			return err
		}
	}
	return w.WriteElement(idContentEncryption, payload.Bytes())
}

func writeContentEncAESSettings(w *ebml.Writer, settings ContentEncAESSettings) error {
	var payload bytes.Buffer
	aw := ebml.NewWriter(&payload)
	if err := aw.WriteUInt(idContentEncAESCipher, settings.CipherMode); err != nil {
		return err
	}
	return w.WriteElement(idContentEncAES, payload.Bytes())
}

func trackCodecTiming(track Track, private []byte) (int64, int64, error) {
	codecDelayNS := track.CodecDelayNS
	seekPreRollNS := track.SeekPreRollNS
	if track.Codec != CodecOpus {
		return codecDelayNS, seekPreRollNS, nil
	}
	if codecDelayNS == 0 && len(private) != 0 {
		head, err := parseOpusHead(private)
		if err != nil {
			return 0, 0, err
		}
		codecDelayNS = opusCodecDelayNS(head.PreSkip)
	}
	if seekPreRollNS == 0 {
		seekPreRollNS = opusDefaultSeekPreRollNS
	}
	return codecDelayNS, seekPreRollNS, nil
}

func boolFlagUInt(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}

func writeOptionalBoolFlag(w *ebml.Writer, id ebml.ID, value bool, set bool) error {
	if !set {
		return nil
	}
	return w.WriteUInt(id, boolFlagUInt(value))
}

func writeVideo(w *ebml.Writer, video VideoConfig) error {
	var payload bytes.Buffer
	vw := ebml.NewWriter(&payload)
	if err := writeOptionalUInt(vw, idFlagInterlaced, video.FlagInterlaced, video.FlagInterlacedSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(vw, idFieldOrder, video.FieldOrder, video.FieldOrderSet); err != nil {
		return err
	}
	if video.StereoModeSet {
		if err := vw.WriteUInt(idStereoMode, uint64(video.StereoMode)); err != nil {
			return err
		}
	}
	if video.AlphaModeSet {
		if err := vw.WriteUInt(idAlphaMode, uint64(video.AlphaMode)); err != nil {
			return err
		}
	}
	if err := vw.WriteUInt(idPixelWidth, uint64(video.Width)); err != nil {
		return err
	}
	if err := vw.WriteUInt(idPixelHeight, uint64(video.Height)); err != nil {
		return err
	}
	if video.PixelCropBottom > 0 {
		if err := vw.WriteUInt(idPixelCropBottom, uint64(video.PixelCropBottom)); err != nil {
			return err
		}
	}
	if video.PixelCropTop > 0 {
		if err := vw.WriteUInt(idPixelCropTop, uint64(video.PixelCropTop)); err != nil {
			return err
		}
	}
	if video.PixelCropLeft > 0 {
		if err := vw.WriteUInt(idPixelCropLeft, uint64(video.PixelCropLeft)); err != nil {
			return err
		}
	}
	if video.PixelCropRight > 0 {
		if err := vw.WriteUInt(idPixelCropRight, uint64(video.PixelCropRight)); err != nil {
			return err
		}
	}
	if video.DisplayWidth > 0 {
		if err := vw.WriteUInt(idDisplayWidth, uint64(video.DisplayWidth)); err != nil {
			return err
		}
	}
	if video.DisplayHeight > 0 {
		if err := vw.WriteUInt(idDisplayHeight, uint64(video.DisplayHeight)); err != nil {
			return err
		}
	}
	if video.DisplayUnit > 0 {
		if err := vw.WriteUInt(idDisplayUnit, uint64(video.DisplayUnit)); err != nil {
			return err
		}
	}
	if err := writeOptionalUInt(vw, idAspectRatioType, video.AspectRatioType, video.AspectRatioTypeSet); err != nil {
		return err
	}
	if videoColourHasMetadata(video.Colour) {
		if err := writeColour(vw, video.Colour); err != nil {
			return err
		}
	}
	if videoProjectionHasMetadata(video.Projection) {
		if err := writeProjection(vw, video.Projection); err != nil {
			return err
		}
	}
	return w.WriteElement(idVideo, payload.Bytes())
}

func videoProjectionHasMetadata(projection VideoProjectionConfig) bool {
	return projection.Set ||
		projection.Type != 0 ||
		len(projection.Private) != 0 ||
		projection.PoseYaw != 0 ||
		projection.PosePitch != 0 ||
		projection.PoseRoll != 0
}

func writeProjection(w *ebml.Writer, projection VideoProjectionConfig) error {
	var payload bytes.Buffer
	pw := ebml.NewWriter(&payload)
	if err := pw.WriteUInt(idProjectionType, uint64(projection.Type)); err != nil {
		return err
	}
	if len(projection.Private) != 0 {
		if err := writeBinary(pw, idProjectionPrivate, projection.Private); err != nil {
			return err
		}
	}
	if err := pw.WriteFloat64(idProjectionPoseYaw, projection.PoseYaw); err != nil {
		return err
	}
	if err := pw.WriteFloat64(idProjectionPosePitch, projection.PosePitch); err != nil {
		return err
	}
	if err := pw.WriteFloat64(idProjectionPoseRoll, projection.PoseRoll); err != nil {
		return err
	}
	return w.WriteElement(idProjection, payload.Bytes())
}

func videoColourHasMetadata(colour VideoColourConfig) bool {
	return colour.MatrixCoefficientsSet ||
		colour.BitsPerChannelSet ||
		colour.ChromaSubsamplingHorzSet ||
		colour.ChromaSubsamplingVertSet ||
		colour.CbSubsamplingHorzSet ||
		colour.CbSubsamplingVertSet ||
		colour.ChromaSitingHorzSet ||
		colour.ChromaSitingVertSet ||
		colour.RangeSet ||
		colour.TransferCharacteristicsSet ||
		colour.PrimariesSet ||
		colour.MaxCLLSet ||
		colour.MaxFALLSet ||
		masteringMetadataHasValues(colour.MasteringMetadata)
}

func masteringMetadataHasValues(metadata VideoMasteringMetadataConfig) bool {
	return metadata.PrimaryRChromaticityXSet ||
		metadata.PrimaryRChromaticityYSet ||
		metadata.PrimaryGChromaticityXSet ||
		metadata.PrimaryGChromaticityYSet ||
		metadata.PrimaryBChromaticityXSet ||
		metadata.PrimaryBChromaticityYSet ||
		metadata.WhitePointChromaticityXSet ||
		metadata.WhitePointChromaticityYSet ||
		metadata.LuminanceMaxSet ||
		metadata.LuminanceMinSet
}

func writeColour(w *ebml.Writer, colour VideoColourConfig) error {
	var payload bytes.Buffer
	cw := ebml.NewWriter(&payload)
	if err := writeOptionalUInt(cw, idMatrixCoefficients, colour.MatrixCoefficients, colour.MatrixCoefficientsSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idBitsPerChannel, colour.BitsPerChannel, colour.BitsPerChannelSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idChromaSubsampleHorz, colour.ChromaSubsamplingHorz, colour.ChromaSubsamplingHorzSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idChromaSubsampleVert, colour.ChromaSubsamplingVert, colour.ChromaSubsamplingVertSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idCbSubsampleHorz, colour.CbSubsamplingHorz, colour.CbSubsamplingHorzSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idCbSubsampleVert, colour.CbSubsamplingVert, colour.CbSubsamplingVertSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idChromaSitingHorz, colour.ChromaSitingHorz, colour.ChromaSitingHorzSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idChromaSitingVert, colour.ChromaSitingVert, colour.ChromaSitingVertSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idColourRange, colour.Range, colour.RangeSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idTransferChar, colour.TransferCharacteristics, colour.TransferCharacteristicsSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idPrimaries, colour.Primaries, colour.PrimariesSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idMaxCLL, colour.MaxCLL, colour.MaxCLLSet); err != nil {
		return err
	}
	if err := writeOptionalUInt(cw, idMaxFALL, colour.MaxFALL, colour.MaxFALLSet); err != nil {
		return err
	}
	if masteringMetadataHasValues(colour.MasteringMetadata) {
		if err := writeMasteringMetadata(cw, colour.MasteringMetadata); err != nil {
			return err
		}
	}
	return w.WriteElement(idColour, payload.Bytes())
}

func writeMasteringMetadata(w *ebml.Writer, metadata VideoMasteringMetadataConfig) error {
	var payload bytes.Buffer
	mw := ebml.NewWriter(&payload)
	if err := writeOptionalFloat64(mw, idPrimaryRX, metadata.PrimaryRChromaticityX, metadata.PrimaryRChromaticityXSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idPrimaryRY, metadata.PrimaryRChromaticityY, metadata.PrimaryRChromaticityYSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idPrimaryGX, metadata.PrimaryGChromaticityX, metadata.PrimaryGChromaticityXSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idPrimaryGY, metadata.PrimaryGChromaticityY, metadata.PrimaryGChromaticityYSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idPrimaryBX, metadata.PrimaryBChromaticityX, metadata.PrimaryBChromaticityXSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idPrimaryBY, metadata.PrimaryBChromaticityY, metadata.PrimaryBChromaticityYSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idWhitePointX, metadata.WhitePointChromaticityX, metadata.WhitePointChromaticityXSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idWhitePointY, metadata.WhitePointChromaticityY, metadata.WhitePointChromaticityYSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idLuminanceMax, metadata.LuminanceMax, metadata.LuminanceMaxSet); err != nil {
		return err
	}
	if err := writeOptionalFloat64(mw, idLuminanceMin, metadata.LuminanceMin, metadata.LuminanceMinSet); err != nil {
		return err
	}
	return w.WriteElement(idMasteringMetadata, payload.Bytes())
}

func writeOptionalUInt(w *ebml.Writer, id ebml.ID, value int, set bool) error {
	if !set {
		return nil
	}
	if value < 0 {
		return ErrInvalidTrack
	}
	return w.WriteUInt(id, uint64(value))
}

func writeOptionalFloat64(w *ebml.Writer, id ebml.ID, value float64, set bool) error {
	if !set {
		return nil
	}
	return w.WriteFloat64(id, value)
}

func writeAudio(w *ebml.Writer, audio AudioConfig) error {
	var payload bytes.Buffer
	aw := ebml.NewWriter(&payload)
	sampleRate := audio.SampleRate
	if sampleRate == 0 {
		sampleRate = 48000
	}
	channels := audio.Channels
	if channels == 0 {
		channels = 2
	}
	if err := aw.WriteFloat64(idSamplingFreq, float64(sampleRate)); err != nil {
		return err
	}
	if audio.OutputSampleRate > 0 {
		if err := aw.WriteFloat64(idOutputFreq, float64(audio.OutputSampleRate)); err != nil {
			return err
		}
	}
	if err := aw.WriteUInt(idChannels, uint64(channels)); err != nil {
		return err
	}
	if audio.BitDepth > 0 {
		if err := aw.WriteUInt(idBitDepth, uint64(audio.BitDepth)); err != nil {
			return err
		}
	}
	return w.WriteElement(idAudio, payload.Bytes())
}

func validateTrack(track Track) error {
	if track.Type != TrackAudio && track.Type != TrackVideo && track.Type != TrackSubtitle {
		return ErrInvalidTrack
	}
	if track.DefaultDurationNS < 0 || track.DefaultDecodedFieldDurationNS < 0 || track.CodecDelayNS < 0 || track.SeekPreRollNS < 0 {
		return ErrInvalidTrack
	}
	if err := validateTrackTiming(track); err != nil {
		return err
	}
	if slices.Contains(track.TrackOverlays, 0) {
		return ErrInvalidTrack
	}
	for i := range track.TrackTranslates {
		if err := validateTrackTranslate(track.TrackTranslates[i]); err != nil {
			return err
		}
	}
	if err := validateContentEncodings(track.ContentEncodings); err != nil {
		return err
	}
	if err := validateWritableBlockContentEncodings(track); err != nil {
		return err
	}
	maxMappingID, err := maxBlockAdditionMappingID(track.BlockAdditionMappings)
	if err != nil || maxMappingID > track.MaxBlockAdditionID {
		return ErrInvalidTrack
	}
	if _, err := matroskaCodecID(track.Codec); err != nil {
		return err
	}
	switch track.Type {
	case TrackAudio:
		if track.Audio.SampleRate < 0 || track.Audio.OutputSampleRate < 0 || track.Audio.Channels < 0 || track.Audio.BitDepth < 0 {
			return ErrInvalidTrack
		}
		switch track.Codec {
		case CodecOpus:
			if len(track.CodecPrivate) == 0 && !canWriteDefaultOpusHead(track) {
				return ErrInvalidTrack
			}
		case CodecVorbis:
			if len(track.CodecPrivate) == 0 {
				return ErrInvalidTrack
			}
			private, err := parseVorbisCodecPrivate(track.CodecPrivate)
			if err != nil {
				return ErrInvalidTrack
			}
			if track.Audio.SampleRate != 0 && track.Audio.SampleRate != private.SampleRate {
				return ErrInvalidTrack
			}
			if track.Audio.Channels != 0 && track.Audio.Channels != private.Channels {
				return ErrInvalidTrack
			}
		case CodecFLAC:
			if len(track.CodecPrivate) == 0 {
				return ErrInvalidTrack
			}
			private, err := parseFLACCodecPrivate(track.CodecPrivate)
			if err != nil {
				return ErrInvalidTrack
			}
			if track.Audio.SampleRate != 0 && track.Audio.SampleRate != private.SampleRate {
				return ErrInvalidTrack
			}
			if track.Audio.Channels != 0 && track.Audio.Channels != private.Channels {
				return ErrInvalidTrack
			}
			if track.Audio.BitDepth != 0 && track.Audio.BitDepth != private.BitsPerSample {
				return ErrInvalidTrack
			}
		case CodecAAC:
			if len(track.CodecPrivate) == 0 {
				return ErrInvalidTrack
			}
			private, err := parseAACAudioSpecificConfig(track.CodecPrivate)
			if err != nil {
				return ErrInvalidTrack
			}
			if track.Audio.SampleRate != 0 && track.Audio.SampleRate != private.SampleRate {
				return ErrInvalidTrack
			}
			if track.Audio.Channels != 0 && track.Audio.Channels != private.Channels {
				return ErrInvalidTrack
			}
		case CodecPCMU, CodecPCMA:
			if len(track.CodecPrivate) != 0 {
				format, err := parseMSACMWaveFormat(track.CodecPrivate)
				if err != nil {
					return ErrInvalidTrack
				}
				if err := validateG711MSACMWaveFormat(format, track.Codec); err != nil {
					return ErrInvalidTrack
				}
				if track.Audio.SampleRate != 0 && track.Audio.SampleRate != format.SampleRate {
					return ErrInvalidTrack
				}
				if track.Audio.Channels != 0 && track.Audio.Channels != format.Channels {
					return ErrInvalidTrack
				}
				if track.Audio.BitDepth != 0 && track.Audio.BitDepth != format.BitsPerSample {
					return ErrInvalidTrack
				}
			} else if !canWriteDefaultMSACMWaveFormat(track) {
				return ErrInvalidTrack
			}
		default:
			return ErrInvalidTrack
		}
	case TrackVideo:
		if track.Video.Width < 0 || track.Video.Height < 0 ||
			track.Video.FlagInterlaced < 0 || track.Video.FieldOrder < 0 ||
			track.Video.StereoMode < 0 || track.Video.AlphaMode < 0 ||
			track.Video.PixelCropBottom < 0 || track.Video.PixelCropTop < 0 ||
			track.Video.PixelCropLeft < 0 || track.Video.PixelCropRight < 0 ||
			track.Video.DisplayWidth < 0 || track.Video.DisplayHeight < 0 ||
			track.Video.DisplayUnit < 0 || track.Video.AspectRatioType < 0 {
			return ErrInvalidTrack
		}
		if err := validateVideoColour(track.Video.Colour); err != nil {
			return err
		}
		if err := validateVideoProjection(track.Video.Projection); err != nil {
			return err
		}
		switch track.Codec {
		case CodecAV1:
			if len(track.CodecPrivate) != 0 {
				if _, err := parseAV1CodecConfigurationRecord(track.CodecPrivate); err != nil {
					return ErrInvalidTrack
				}
			}
		case CodecH264:
			if len(track.CodecPrivate) != 0 {
				if _, err := parseAVCDecoderConfigurationRecord(track.CodecPrivate); err != nil {
					return ErrInvalidTrack
				}
			}
		case CodecH265:
			if len(track.CodecPrivate) != 0 {
				if _, err := parseHEVCDecoderConfigurationRecord(track.CodecPrivate); err != nil {
					return ErrInvalidTrack
				}
			}
		case CodecVP8, CodecVP9:
		default:
			return ErrInvalidTrack
		}
	case TrackSubtitle:
		if track.Codec != CodecTextUTF8 || len(track.CodecPrivate) != 0 {
			return ErrInvalidTrack
		}
	}
	if (track.TimebaseNum == 0) != (track.TimebaseDen == 0) ||
		track.TimebaseNum < 0 || track.TimebaseDen < 0 {
		return ErrInvalidTrack
	}
	return nil
}

func validateTrackTiming(track Track) error {
	scale := trackTimestampScale(track)
	if scale <= 0 || math.IsNaN(scale) || math.IsInf(scale, 0) {
		return ErrInvalidTrack
	}
	return nil
}

func trackTimestampScale(track Track) float64 {
	if track.TrackTimestampScaleSet || track.TrackTimestampScale != 0 {
		return track.TrackTimestampScale
	}
	return 1
}

func validateTrackTranslate(translate TrackTranslate) error {
	if translate.TrackID == nil {
		return ErrInvalidTrack
	}
	return nil
}

func validateContentEncodings(encodings []ContentEncoding) error {
	if len(encodings) == 0 {
		return nil
	}
	orders := make(map[uint64]struct{}, len(encodings))
	for i := range encodings {
		order := encodings[i].Order
		if _, ok := orders[order]; ok {
			return ErrInvalidTrack
		}
		orders[order] = struct{}{}
		if err := validateContentEncoding(encodings[i]); err != nil {
			return err
		}
	}
	return nil
}

func validateWritableBlockContentEncodings(track Track) error {
	encoding, err := blockContentEncoding(track)
	if err != nil {
		return err
	}
	if encoding.compression == blockContentTransformBzlib {
		return ErrBzip2ContentEncodingWrite
	}
	return nil
}

type blockContentTransform uint8

const (
	blockContentTransformNone blockContentTransform = iota
	blockContentTransformZlib
	blockContentTransformBzlib
)

type blockContentEncodingInfo struct {
	headerSettings   []byte
	headerOrder      uint64
	headerSet        bool
	compression      blockContentTransform
	compressionOrder uint64
	encryption       ContentEncryption
	encryptionOrder  uint64
	encryptionSet    bool
}

func blockContentEncoding(track Track) (blockContentEncodingInfo, error) {
	var out blockContentEncodingInfo
	for i := range track.ContentEncodings {
		encoding := track.ContentEncodings[i]
		if contentEncodingScope(encoding.Scope)&ContentEncodingScopeBlock == 0 {
			continue
		}
		switch encoding.Type {
		case ContentEncodingTypeCompression:
			if !encoding.CompressionSet {
				return blockContentEncodingInfo{}, ErrUnsupportedContentEncoding
			}
			switch encoding.Compression.Algorithm {
			case ContentCompAlgoHeaderStripping:
				if out.headerSet {
					return blockContentEncodingInfo{}, ErrUnsupportedContentEncoding
				}
				out.headerSet = true
				out.headerOrder = encoding.Order
				out.headerSettings = encoding.Compression.Settings
			case ContentCompAlgoZlib:
				if len(encoding.Compression.Settings) != 0 {
					return blockContentEncodingInfo{}, ErrUnsupportedContentEncoding
				}
				if out.compression != blockContentTransformNone {
					return blockContentEncodingInfo{}, ErrUnsupportedContentEncoding
				}
				out.compression = blockContentTransformZlib
				out.compressionOrder = encoding.Order
			case ContentCompAlgoBzlib:
				if len(encoding.Compression.Settings) != 0 {
					return blockContentEncodingInfo{}, ErrUnsupportedContentEncoding
				}
				if out.compression != blockContentTransformNone {
					return blockContentEncodingInfo{}, ErrUnsupportedContentEncoding
				}
				out.compression = blockContentTransformBzlib
				out.compressionOrder = encoding.Order
			case ContentCompAlgoLZO1X:
				return blockContentEncodingInfo{}, ErrLZOContentEncoding
			default:
				return blockContentEncodingInfo{}, ErrUnsupportedContentEncoding
			}
		case ContentEncodingTypeEncryption:
			if !encoding.EncryptionSet {
				return blockContentEncodingInfo{}, ErrUnsupportedContentEncoding
			}
			if encoding.Encryption.Algorithm == ContentEncAlgoNotEncrypted {
				continue
			}
			if out.encryptionSet ||
				encoding.Encryption.Algorithm != ContentEncAlgoAES ||
				!encoding.Encryption.AESSettingsSet ||
				encoding.Encryption.AESSettings.CipherMode != ContentEncAESCipherModeCTR {
				return blockContentEncodingInfo{}, ErrUnsupportedContentEncoding
			}
			out.encryption = encoding.Encryption
			out.encryptionOrder = encoding.Order
			out.encryptionSet = true
		default:
			return blockContentEncodingInfo{}, ErrUnsupportedContentEncoding
		}
	}
	if out.headerSet && out.compression != blockContentTransformNone && out.headerOrder >= out.compressionOrder {
		return blockContentEncodingInfo{}, ErrUnsupportedContentEncoding
	}
	if out.headerSet && out.encryptionSet && out.headerOrder >= out.encryptionOrder {
		return blockContentEncodingInfo{}, ErrUnsupportedContentEncoding
	}
	if out.compression != blockContentTransformNone && out.encryptionSet && out.compressionOrder >= out.encryptionOrder {
		return blockContentEncodingInfo{}, ErrUnsupportedContentEncoding
	}
	if out.headerSet && codecUsesLengthPrefixedSamples(track) {
		return blockContentEncodingInfo{}, ErrUnsupportedContentEncoding
	}
	return out, nil
}

func trackHasBlockEncryption(track Track) (bool, error) {
	if len(track.ContentEncodings) == 0 {
		return false, nil
	}
	encoding, err := blockContentEncoding(track)
	if err != nil {
		return false, err
	}
	return encoding.encryptionSet, nil
}

func blockHeaderStripping(track Track) ([]byte, bool, error) {
	encoding, err := blockContentEncoding(track)
	if err != nil {
		return nil, false, err
	}
	if !encoding.headerSet {
		return nil, false, nil
	}
	return encoding.headerSettings, true, nil
}

func validateContentEncoding(encoding ContentEncoding) error {
	scope := contentEncodingScope(encoding.Scope)
	if scope&^contentEncodingScopeAll() != 0 {
		return ErrInvalidTrack
	}
	switch encoding.Type {
	case ContentEncodingTypeCompression:
		if !encoding.CompressionSet || encoding.EncryptionSet {
			return ErrInvalidTrack
		}
		return validateContentCompression(encoding.Compression)
	case ContentEncodingTypeEncryption:
		if !encoding.EncryptionSet || encoding.CompressionSet {
			return ErrInvalidTrack
		}
		return validateContentEncryption(encoding.Encryption)
	default:
		return ErrInvalidTrack
	}
}

func contentEncodingScope(scope uint64) uint64 {
	if scope == 0 {
		return ContentEncodingScopeBlock
	}
	return scope
}

func contentEncodingScopeAll() uint64 {
	return ContentEncodingScopeBlock | ContentEncodingScopePrivate | ContentEncodingScopeNext
}

func validateContentCompression(compression ContentCompression) error {
	if compression.Algorithm > ContentCompAlgoHeaderStripping {
		return ErrInvalidTrack
	}
	return nil
}

func validateContentEncryption(encryption ContentEncryption) error {
	if encryption.Algorithm > ContentEncAlgoAES ||
		encryption.SignatureAlgorithm > ContentSigAlgoRSA ||
		encryption.SignatureHashAlgorithm > ContentSigHashAlgoMD5 {
		return ErrInvalidTrack
	}
	if encryption.AESSettingsSet {
		if encryption.Algorithm != ContentEncAlgoAES ||
			(encryption.AESSettings.CipherMode != ContentEncAESCipherModeCTR &&
				encryption.AESSettings.CipherMode != ContentEncAESCipherModeCBC) {
			return ErrInvalidTrack
		}
	}
	return nil
}

func validateVideoColour(colour VideoColourConfig) error {
	if colour.MatrixCoefficients < 0 ||
		colour.BitsPerChannel < 0 ||
		colour.ChromaSubsamplingHorz < 0 ||
		colour.ChromaSubsamplingVert < 0 ||
		colour.CbSubsamplingHorz < 0 ||
		colour.CbSubsamplingVert < 0 ||
		colour.ChromaSitingHorz < 0 ||
		colour.ChromaSitingVert < 0 ||
		colour.Range < 0 ||
		colour.TransferCharacteristics < 0 ||
		colour.Primaries < 0 ||
		colour.MaxCLL < 0 ||
		colour.MaxFALL < 0 {
		return ErrInvalidTrack
	}
	return validateMasteringMetadata(colour.MasteringMetadata)
}

func validateVideoProjection(projection VideoProjectionConfig) error {
	if projection.Type < 0 {
		return ErrInvalidTrack
	}
	if len(projection.Private) != 0 && projection.Type == 0 {
		return ErrInvalidTrack
	}
	if projection.Type >= 1 && projection.Type <= 3 && len(projection.Private) == 0 {
		return ErrInvalidTrack
	}
	if err := validateProjectionPose(projection.PoseYaw, -180, 180); err != nil {
		return err
	}
	if err := validateProjectionPose(projection.PosePitch, -90, 90); err != nil {
		return err
	}
	return validateProjectionPose(projection.PoseRoll, -180, 180)
}

func validateProjectionPose(value float64, min float64, max float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < min || value > max {
		return ErrInvalidTrack
	}
	return nil
}

func validateMasteringMetadata(metadata VideoMasteringMetadataConfig) error {
	if err := validateChromaticity(metadata.PrimaryRChromaticityX, metadata.PrimaryRChromaticityXSet); err != nil {
		return err
	}
	if err := validateChromaticity(metadata.PrimaryRChromaticityY, metadata.PrimaryRChromaticityYSet); err != nil {
		return err
	}
	if err := validateChromaticity(metadata.PrimaryGChromaticityX, metadata.PrimaryGChromaticityXSet); err != nil {
		return err
	}
	if err := validateChromaticity(metadata.PrimaryGChromaticityY, metadata.PrimaryGChromaticityYSet); err != nil {
		return err
	}
	if err := validateChromaticity(metadata.PrimaryBChromaticityX, metadata.PrimaryBChromaticityXSet); err != nil {
		return err
	}
	if err := validateChromaticity(metadata.PrimaryBChromaticityY, metadata.PrimaryBChromaticityYSet); err != nil {
		return err
	}
	if err := validateChromaticity(metadata.WhitePointChromaticityX, metadata.WhitePointChromaticityXSet); err != nil {
		return err
	}
	if err := validateChromaticity(metadata.WhitePointChromaticityY, metadata.WhitePointChromaticityYSet); err != nil {
		return err
	}
	if err := validateNonNegativeFloat(metadata.LuminanceMax, metadata.LuminanceMaxSet); err != nil {
		return err
	}
	return validateNonNegativeFloat(metadata.LuminanceMin, metadata.LuminanceMinSet)
}

func validateChromaticity(value float64, set bool) error {
	if !set {
		return nil
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
		return ErrInvalidTrack
	}
	return nil
}

func validateNonNegativeFloat(value float64, set bool) error {
	if !set {
		return nil
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return ErrInvalidTrack
	}
	return nil
}

func canWriteDefaultOpusHead(track Track) bool {
	channels := track.Audio.Channels
	if channels == 0 {
		channels = 2
	}
	sampleRate := track.Audio.SampleRate
	if sampleRate == 0 {
		sampleRate = 48000
	}
	return channels >= 1 && channels <= 2 && uint64(sampleRate) <= uint64(^uint32(0))
}

func normalizeAudioDefaults(codec Codec, audio *AudioConfig) {
	if codec == CodecPCMU || codec == CodecPCMA {
		if audio.SampleRate == 0 {
			audio.SampleRate = 8000
		}
		if audio.Channels == 0 {
			audio.Channels = 1
		}
		if audio.BitDepth == 0 {
			audio.BitDepth = 8
		}
	}
}

func canWriteDefaultMSACMWaveFormat(track Track) bool {
	audio := track.Audio
	normalizeAudioDefaults(track.Codec, &audio)
	if audio.SampleRate <= 0 || uint64(audio.SampleRate) > uint64(^uint32(0)) {
		return false
	}
	if audio.Channels <= 0 || uint64(audio.Channels) > uint64(^uint16(0)) {
		return false
	}
	if audio.BitDepth != 8 {
		return false
	}
	avgBytesPerSec := uint64(audio.SampleRate) * uint64(audio.Channels)
	return avgBytesPerSec <= uint64(^uint32(0))
}

const timeNS = 1000000000
