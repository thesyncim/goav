package matroska

import (
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/thesyncim/goav/container/ebml"
)

type Demuxer struct {
	reader          *ebml.Reader
	seeker          io.ReadSeeker
	options         DemuxerOptions
	docType         string
	segmentData     int64
	timecodeScaleNS int64
	tracks          []Track
	cues            []CuePoint
	seekEntries     []SeekEntry
	inSegment       bool
	inCluster       bool
	clusterUnknown  bool
	clusterEnd      int64
	clusterTimecode int64
	blockLimit      io.LimitedReader
	groupLimit      io.LimitedReader
	groupReader     *ebml.Reader
	blockHeader     [3]byte
	scratch         [ebml.MaxSizeWidth]byte
	uintScratch     [8]byte
}

func NewDemuxer(r io.Reader, opts DemuxerOptions) (*Demuxer, error) {
	if r == nil {
		return nil, ErrNilReader
	}
	d := &Demuxer{}
	if err := d.init(r, opts); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Demuxer) init(r io.Reader, opts DemuxerOptions) error {
	d.reader = ebml.NewReader(r, ebml.ReaderOptions{MaxElementSize: opts.MaxElementSize})
	d.seeker, _ = r.(io.ReadSeeker)
	if d.groupReader == nil {
		d.groupReader = ebml.NewReader(&d.groupLimit, ebml.ReaderOptions{MaxElementSize: opts.MaxElementSize})
	}
	d.options = opts
	d.docType = ""
	d.segmentData = 0
	d.timecodeScaleNS = defaultTimecodeScaleNS
	d.tracks = d.tracks[:0]
	d.cues = d.cues[:0]
	d.seekEntries = d.seekEntries[:0]
	d.inSegment = false
	d.inCluster = false
	d.clusterUnknown = false
	d.clusterEnd = 0
	d.clusterTimecode = 0
	return d.readPreamble()
}

func (d *Demuxer) SeekToTime(timeNS int64) error {
	if d == nil || d.reader == nil {
		return ErrNilReader
	}
	if d.seeker == nil {
		return ErrNonSeekableReader
	}
	if timeNS < 0 {
		return ErrInvalidData
	}
	if len(d.cues) == 0 {
		if err := d.loadCuesFromSeekHead(); err != nil {
			return err
		}
	}
	if len(d.cues) == 0 {
		return ErrInvalidData
	}
	cue := d.cues[0]
	for i := range d.cues {
		if d.cues[i].TimeNS > timeNS {
			break
		}
		cue = d.cues[i]
	}
	offset := d.segmentData + int64(cue.ClusterPosition)
	if _, err := d.seeker.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	d.reader.Reset(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	header, err := d.reader.ReadHeader()
	if err != nil {
		return err
	}
	if header.ID != idCluster {
		return ErrInvalidData
	}
	if err := d.enterCluster(header); err != nil {
		return err
	}
	return nil
}

func (d *Demuxer) Tracks() []Track {
	if d == nil || len(d.tracks) == 0 {
		return nil
	}
	tracks := make([]Track, len(d.tracks))
	copy(tracks, d.tracks)
	return tracks
}

func (d *Demuxer) Cues() []CuePoint {
	if d == nil || len(d.cues) == 0 {
		return nil
	}
	cues := make([]CuePoint, len(d.cues))
	copy(cues, d.cues)
	return cues
}

func (d *Demuxer) SeekEntries() []SeekEntry {
	if d == nil || len(d.seekEntries) == 0 {
		return nil
	}
	entries := make([]SeekEntry, len(d.seekEntries))
	copy(entries, d.seekEntries)
	return entries
}

func (d *Demuxer) ReadPacket(dst *Packet) error {
	if d == nil || d.reader == nil {
		return ErrNilReader
	}
	if dst == nil {
		return ErrNilPacket
	}
	dst.Reset()
	for {
		if d.inCluster && !d.clusterUnknown && d.reader.Offset() >= d.clusterEnd {
			d.inCluster = false
		}
		header, err := d.reader.ReadHeader()
		if err != nil {
			return err
		}
		if d.inCluster {
			if header.ID == idCluster {
				if err := d.enterCluster(header); err != nil {
					return err
				}
				continue
			}
			switch header.ID {
			case idTimestamp:
				value, err := readUIntPayloadScratch(d.reader, header.Size.Value, &d.uintScratch)
				if err != nil {
					return err
				}
				if value > uint64(math.MaxInt64) {
					return ErrInvalidData
				}
				d.clusterTimecode = int64(value)
			case idSimpleBlock:
				return d.readSimpleBlock(header, dst)
			case idBlockGroup:
				return d.readBlockGroup(header, dst)
			case idVoid, idCRC32:
				if err := skipElement(d.reader, header); err != nil {
					return err
				}
			default:
				if err := skipElement(d.reader, header); err != nil {
					return err
				}
			}
			continue
		}
		switch header.ID {
		case idSeekHead:
			if err := d.parseSeekHead(header); err != nil {
				return err
			}
		case idCluster:
			if err := d.enterCluster(header); err != nil {
				return err
			}
		case idInfo:
			if err := d.parseInfo(header); err != nil {
				return err
			}
		case idTracks:
			if err := d.parseTracks(header); err != nil {
				return err
			}
		case idCues:
			if err := d.parseCues(header); err != nil {
				return err
			}
		case idVoid, idCRC32:
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		default:
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		}
	}
}

func (d *Demuxer) readPreamble() error {
	header, err := d.reader.ReadHeader()
	if err != nil {
		return err
	}
	if header.ID != idEBML {
		return ErrInvalidData
	}
	if err := d.parseEBMLHeader(header); err != nil {
		return err
	}
	for {
		header, err := d.reader.ReadHeader()
		if err != nil {
			return err
		}
		switch header.ID {
		case idSegment:
			d.inSegment = true
			d.segmentData = header.DataOffset
			return d.readSegmentHeaders()
		case idVoid, idCRC32:
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		default:
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		}
	}
}

func (d *Demuxer) loadCuesFromSeekHead() error {
	var cuesPosition uint64
	for i := range d.seekEntries {
		if d.seekEntries[i].ID == uint64(idCues) {
			cuesPosition = d.seekEntries[i].Position
			break
		}
	}
	if cuesPosition == 0 {
		return ErrInvalidData
	}
	if _, err := d.seeker.Seek(d.segmentData+int64(cuesPosition), io.SeekStart); err != nil {
		return err
	}
	d.reader.Reset(d.seeker, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	header, err := d.reader.ReadHeader()
	if err != nil {
		return err
	}
	if header.ID != idCues {
		return ErrInvalidData
	}
	return d.parseCues(header)
}

func (d *Demuxer) readSegmentHeaders() error {
	for {
		header, err := d.reader.ReadHeader()
		if err != nil {
			return err
		}
		switch header.ID {
		case idSeekHead:
			if err := d.parseSeekHead(header); err != nil {
				return err
			}
		case idInfo:
			if err := d.parseInfo(header); err != nil {
				return err
			}
		case idTracks:
			if err := d.parseTracks(header); err != nil {
				return err
			}
		case idCues:
			if err := d.parseCues(header); err != nil {
				return err
			}
		case idCluster:
			return d.enterCluster(header)
		case idVoid, idCRC32:
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		default:
			if err := skipElement(d.reader, header); err != nil {
				return err
			}
		}
	}
}

func (d *Demuxer) parseSeekHead(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	limited := d.reader.Limited(header.Size.Value)
	reader := ebml.NewReader(limited, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	for limited.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idSeek:
			entry, err := d.parseSeekEntry(reader, child)
			if err != nil {
				return err
			}
			if entry.ID != 0 {
				d.seekEntries = append(d.seekEntries, entry)
			}
		default:
			if err := skipElement(reader, child); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Demuxer) parseSeekEntry(parent io.Reader, header ebml.Header) (SeekEntry, error) {
	if header.Size.Unknown {
		return SeekEntry{}, ErrInvalidData
	}
	limited := &io.LimitedReader{R: parent, N: int64(header.Size.Value)}
	reader := ebml.NewReader(limited, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	var entry SeekEntry
	for limited.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return SeekEntry{}, err
		}
		switch child.ID {
		case idSeekID:
			value, err := readElementIDPayload(reader, child.Size.Value)
			if err != nil {
				return SeekEntry{}, err
			}
			entry.ID = uint64(value)
		case idSeekPosition:
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return SeekEntry{}, err
			}
			entry.Position = value
		default:
			if err := skipElement(reader, child); err != nil {
				return SeekEntry{}, err
			}
		}
	}
	return entry, nil
}

func (d *Demuxer) parseEBMLHeader(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	limited := d.reader.Limited(header.Size.Value)
	reader := ebml.NewReader(limited, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	for limited.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idDocType:
			value, err := readStringPayload(reader, child.Size.Value)
			if err != nil {
				return err
			}
			d.docType = value
		default:
			if err := skipElement(reader, child); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Demuxer) parseInfo(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	limited := d.reader.Limited(header.Size.Value)
	reader := ebml.NewReader(limited, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	for limited.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idTimestampScale:
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return err
			}
			if value == 0 || value > uint64(math.MaxInt64) {
				return ErrInvalidData
			}
			d.timecodeScaleNS = int64(value)
		default:
			if err := skipElement(reader, child); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Demuxer) parseTracks(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	limited := d.reader.Limited(header.Size.Value)
	reader := ebml.NewReader(limited, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	for limited.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idTrackEntry:
			track, err := d.parseTrackEntry(reader, child)
			if err != nil {
				return err
			}
			if track.ID != 0 {
				d.upsertTrack(track)
			}
		default:
			if err := skipElement(reader, child); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Demuxer) parseCues(header ebml.Header) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	limited := d.reader.Limited(header.Size.Value)
	reader := ebml.NewReader(limited, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	for limited.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idCuePoint:
			cue, err := d.parseCuePoint(reader, child)
			if err != nil {
				return err
			}
			if cue.TrackID != 0 {
				d.cues = append(d.cues, cue)
			}
		default:
			if err := skipElement(reader, child); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Demuxer) parseCuePoint(parent io.Reader, header ebml.Header) (CuePoint, error) {
	if header.Size.Unknown {
		return CuePoint{}, ErrInvalidData
	}
	limited := &io.LimitedReader{R: parent, N: int64(header.Size.Value)}
	reader := ebml.NewReader(limited, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	var cue CuePoint
	for limited.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return CuePoint{}, err
		}
		switch child.ID {
		case idCueTime:
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return CuePoint{}, err
			}
			if value > uint64(math.MaxInt64)/uint64(d.timecodeScaleNS) {
				return CuePoint{}, ErrInvalidData
			}
			cue.TimeNS = int64(value) * d.timecodeScaleNS
		case idCueTrackPositions:
			trackID, position, err := d.parseCueTrackPositions(reader, child)
			if err != nil {
				return CuePoint{}, err
			}
			cue.TrackID = trackID
			cue.ClusterPosition = position
		default:
			if err := skipElement(reader, child); err != nil {
				return CuePoint{}, err
			}
		}
	}
	return cue, nil
}

func (d *Demuxer) parseCueTrackPositions(parent io.Reader, header ebml.Header) (uint32, uint64, error) {
	if header.Size.Unknown {
		return 0, 0, ErrInvalidData
	}
	limited := &io.LimitedReader{R: parent, N: int64(header.Size.Value)}
	reader := ebml.NewReader(limited, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	var trackID uint32
	var clusterPosition uint64
	for limited.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return 0, 0, err
		}
		switch child.ID {
		case idCueTrack:
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return 0, 0, err
			}
			trackID = uint32(value)
		case idCueClusterPosition:
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return 0, 0, err
			}
			clusterPosition = value
		default:
			if err := skipElement(reader, child); err != nil {
				return 0, 0, err
			}
		}
	}
	return trackID, clusterPosition, nil
}

func (d *Demuxer) parseTrackEntry(parent io.Reader, header ebml.Header) (Track, error) {
	if header.Size.Unknown {
		return Track{}, ErrInvalidData
	}
	limited := &io.LimitedReader{R: parent, N: int64(header.Size.Value)}
	reader := ebml.NewReader(limited, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	track := Track{Language: "und", TimebaseNum: 1, TimebaseDen: timeNS}
	var codecID string
	for limited.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return Track{}, err
		}
		switch child.ID {
		case idTrackNumber:
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.ID = uint32(value)
		case idTrackType:
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			switch value {
			case matroskaTrackAudio:
				track.Type = TrackAudio
			case matroskaTrackVideo:
				track.Type = TrackVideo
			default:
				track.Type = TrackUnknown
			}
		case idName:
			value, err := readStringPayload(reader, child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.Name = value
		case idLanguage:
			value, err := readStringPayload(reader, child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.Language = value
		case idCodecID:
			value, err := readStringPayload(reader, child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			codecID = value
		case idCodecPrivate:
			value, err := readBinaryPayload(reader, child.Size.Value)
			if err != nil {
				return Track{}, err
			}
			track.CodecPrivate = value
		case idVideo:
			video, err := d.parseVideo(reader, child)
			if err != nil {
				return Track{}, err
			}
			track.Video = video
		case idAudio:
			audio, err := d.parseAudio(reader, child)
			if err != nil {
				return Track{}, err
			}
			track.Audio = audio
		default:
			if err := skipElement(reader, child); err != nil {
				return Track{}, err
			}
		}
	}
	track.Codec = codecFromMatroskaID(codecID, track.CodecPrivate)
	return track, nil
}

func (d *Demuxer) parseVideo(parent io.Reader, header ebml.Header) (VideoConfig, error) {
	if header.Size.Unknown {
		return VideoConfig{}, ErrInvalidData
	}
	limited := &io.LimitedReader{R: parent, N: int64(header.Size.Value)}
	reader := ebml.NewReader(limited, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	var video VideoConfig
	for limited.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return VideoConfig{}, err
		}
		switch child.ID {
		case idPixelWidth:
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.Width = int(value)
		case idPixelHeight:
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return VideoConfig{}, err
			}
			video.Height = int(value)
		default:
			if err := skipElement(reader, child); err != nil {
				return VideoConfig{}, err
			}
		}
	}
	return video, nil
}

func (d *Demuxer) parseAudio(parent io.Reader, header ebml.Header) (AudioConfig, error) {
	if header.Size.Unknown {
		return AudioConfig{}, ErrInvalidData
	}
	limited := &io.LimitedReader{R: parent, N: int64(header.Size.Value)}
	reader := ebml.NewReader(limited, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	audio := AudioConfig{SampleRate: 48000, Channels: 2}
	for limited.N > 0 {
		child, err := reader.ReadHeader()
		if err != nil {
			return AudioConfig{}, err
		}
		switch child.ID {
		case idSamplingFreq:
			value, err := readFloatPayload(reader, child.Size.Value)
			if err != nil {
				return AudioConfig{}, err
			}
			audio.SampleRate = int(value)
		case idChannels:
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return AudioConfig{}, err
			}
			audio.Channels = int(value)
		case idBitDepth:
			value, err := readUIntPayload(reader, child.Size.Value)
			if err != nil {
				return AudioConfig{}, err
			}
			audio.BitDepth = int(value)
		default:
			if err := skipElement(reader, child); err != nil {
				return AudioConfig{}, err
			}
		}
	}
	return audio, nil
}

func (d *Demuxer) enterCluster(header ebml.Header) error {
	d.inCluster = true
	d.clusterUnknown = header.Size.Unknown
	if header.Size.Unknown {
		d.clusterEnd = 0
	} else {
		d.clusterEnd = header.DataOffset + int64(header.Size.Value)
	}
	d.clusterTimecode = 0
	return nil
}

func (d *Demuxer) readSimpleBlock(header ebml.Header, dst *Packet) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	return d.readBlockPayload(d.reader, header.Size.Value, dst, true)
}

func (d *Demuxer) readBlockGroup(header ebml.Header, dst *Packet) error {
	if header.Size.Unknown {
		return ErrInvalidData
	}
	dst.Reset()
	d.groupLimit.R = d.reader
	d.groupLimit.N = int64(header.Size.Value)
	d.groupReader.Reset(&d.groupLimit, ebml.ReaderOptions{MaxElementSize: d.options.MaxElementSize})
	haveBlock := false
	referenceSeen := false
	durationTicks := uint64(0)
	for d.groupLimit.N > 0 {
		child, err := d.groupReader.ReadHeader()
		if err != nil {
			return err
		}
		switch child.ID {
		case idBlock:
			if err := d.readBlockPayload(d.groupReader, child.Size.Value, dst, false); err != nil {
				return err
			}
			haveBlock = true
		case idBlockDuration:
			value, err := readUIntPayloadScratch(d.groupReader, child.Size.Value, &d.uintScratch)
			if err != nil {
				return err
			}
			durationTicks = value
		case idReferenceBlk:
			if _, err := readIntPayload(d.groupReader, child.Size.Value); err != nil {
				return err
			}
			referenceSeen = true
		default:
			if err := skipElement(d.groupReader, child); err != nil {
				return err
			}
		}
	}
	if !haveBlock {
		return ErrInvalidData
	}
	if durationTicks != 0 {
		if durationTicks > uint64(math.MaxInt64)/uint64(d.timecodeScaleNS) {
			return ErrInvalidData
		}
		dst.DurationNS = int64(durationTicks) * d.timecodeScaleNS
	}
	dst.Keyframe = !referenceSeen
	return nil
}

func (d *Demuxer) readBlockPayload(r io.Reader, size uint64, dst *Packet, simple bool) error {
	d.blockLimit.R = d.reader
	if r != nil {
		d.blockLimit.R = r
	}
	d.blockLimit.N = int64(size)
	trackID, _, err := ebml.ReadUnsignedVINT(&d.blockLimit, &d.scratch)
	if err != nil {
		return err
	}
	if _, err := io.ReadFull(&d.blockLimit, d.blockHeader[:]); err != nil {
		return err
	}
	flags := d.blockHeader[2]
	if flags&simpleBlockLacingMask != 0 {
		_, _ = io.Copy(io.Discard, &d.blockLimit)
		return ErrUnsupportedLacing
	}
	if d.blockLimit.N < 0 || d.blockLimit.N > int64(^uint(0)>>1) {
		return ErrInvalidData
	}
	frameSize := int(d.blockLimit.N)
	if cap(dst.Data) < frameSize {
		return ErrPayloadTooSmall
	}
	dst.Data = dst.Data[:frameSize]
	if _, err := io.ReadFull(&d.blockLimit, dst.Data); err != nil {
		return err
	}
	blockTimecode := int16(binary.BigEndian.Uint16(d.blockHeader[:2]))
	timecode := d.clusterTimecode + int64(blockTimecode)
	if timecode < 0 {
		return ErrInvalidData
	}
	dst.TrackID = uint32(trackID)
	dst.TimeNS = timecode * d.timecodeScaleNS
	if simple {
		dst.Keyframe = flags&simpleBlockKeyframe != 0
	}
	dst.Invisible = flags&simpleBlockInvisible != 0
	if simple {
		dst.Discardable = flags&simpleBlockDiscardable != 0
	}
	return nil
}

func (d *Demuxer) upsertTrack(track Track) {
	for i := range d.tracks {
		if d.tracks[i].ID == track.ID {
			d.tracks[i] = track
			return
		}
	}
	d.tracks = append(d.tracks, track)
}

func isEOF(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
