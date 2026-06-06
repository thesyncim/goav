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
	options         DemuxerOptions
	docType         string
	timecodeScaleNS int64
	tracks          []Track
	inSegment       bool
	inCluster       bool
	clusterUnknown  bool
	clusterEnd      int64
	clusterTimecode int64
	blockLimit      io.LimitedReader
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
	d.options = opts
	d.docType = ""
	d.timecodeScaleNS = defaultTimecodeScaleNS
	d.tracks = d.tracks[:0]
	d.inSegment = false
	d.inCluster = false
	d.clusterUnknown = false
	d.clusterEnd = 0
	d.clusterTimecode = 0
	return d.readPreamble()
}

func (d *Demuxer) Tracks() []Track {
	if d == nil || len(d.tracks) == 0 {
		return nil
	}
	tracks := make([]Track, len(d.tracks))
	copy(tracks, d.tracks)
	return tracks
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

func (d *Demuxer) readSegmentHeaders() error {
	for {
		header, err := d.reader.ReadHeader()
		if err != nil {
			return err
		}
		switch header.ID {
		case idInfo:
			if err := d.parseInfo(header); err != nil {
				return err
			}
		case idTracks:
			if err := d.parseTracks(header); err != nil {
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
	d.blockLimit.R = d.reader
	d.blockLimit.N = int64(header.Size.Value)
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
	dst.Keyframe = flags&simpleBlockKeyframe != 0
	dst.Invisible = flags&simpleBlockInvisible != 0
	dst.Discardable = flags&simpleBlockDiscardable != 0
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
