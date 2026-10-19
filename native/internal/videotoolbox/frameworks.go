package videotoolbox

import (
	"unsafe"

	"github.com/thesyncim/goav/native/kit"
)

// The frameworks' install names; macOS serves them from the dyld shared
// cache.
const (
	coreFoundationPath = "/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation"
	coreMediaPath      = "/System/Library/Frameworks/CoreMedia.framework/CoreMedia"
	coreVideoPath      = "/System/Library/Frameworks/CoreVideo.framework/CoreVideo"
	videoToolboxPath   = "/System/Library/Frameworks/VideoToolbox.framework/VideoToolbox"
)

// CMTime is CoreMedia's rational time, passed by value.
type cmTime struct {
	Value     int64
	Timescale int32
	Flags     uint32
	Epoch     int64
}

const timeValid = 1

func timeAt(value int64, timescale int64) cmTime {
	return cmTime{Value: value, Timescale: int32(timescale), Flags: timeValid}
}

// cmTimingInfo is CMSampleTimingInfo.
type cmTimingInfo struct {
	Duration, PTS, DTS cmTime
}

type coreFoundation struct {
	release          func(ref uintptr)                                                          `sym:"CFRelease"`
	retain           func(ref uintptr) uintptr                                                  `sym:"CFRetain"`
	dictionaryCreate func(alloc uintptr, capacity int, keys, values uintptr) uintptr            `sym:"CFDictionaryCreateMutable"`
	dictionarySet    func(dict, key, value uintptr)                                             `sym:"CFDictionarySetValue"`
	dictionaryGet    func(dict, key uintptr) uintptr                                            `sym:"CFDictionaryGetValue"`
	numberCreate     func(alloc uintptr, kind int, value unsafe.Pointer) uintptr                `sym:"CFNumberCreate"`
	stringCreate     func(alloc uintptr, text *byte, encoding uint32) uintptr                   `sym:"CFStringCreateWithCString"`
	arrayCreate      func(alloc uintptr, values *uintptr, count int, callbacks uintptr) uintptr `sym:"CFArrayCreate"`
	arrayCount       func(array uintptr) int                                                    `sym:"CFArrayGetCount"`
	arrayAt          func(array uintptr, index int) uintptr                                     `sym:"CFArrayGetValueAtIndex"`
	dataLength       func(data uintptr) int                                                     `sym:"CFDataGetLength"`
	dataBytes        func(data uintptr) unsafe.Pointer                                          `sym:"CFDataGetBytePtr"`
	booleanValue     func(boolean uintptr) bool                                                 `sym:"CFBooleanGetValue"`
	booleanTrue      uintptr                                                                    `sym:"kCFBooleanTrue,value"`
	booleanFalse     uintptr                                                                    `sym:"kCFBooleanFalse,value"`
	dictionaryKeys   uintptr                                                                    `sym:"kCFTypeDictionaryKeyCallBacks"`
	dictionaryValues uintptr                                                                    `sym:"kCFTypeDictionaryValueCallBacks"`
	arrayCallbacks   uintptr                                                                    `sym:"kCFTypeArrayCallBacks"`
}

type coreMedia struct {
	dataBuffer     func(sample uintptr) uintptr                                                                                                                  `sym:"CMSampleBufferGetDataBuffer"`
	blockLength    func(block uintptr) int                                                                                                                       `sym:"CMBlockBufferGetDataLength"`
	blockCopy      func(block uintptr, offset, length int, dst unsafe.Pointer) int32                                                                             `sym:"CMBlockBufferCopyDataBytes"`
	formatOf       func(sample uintptr) uintptr                                                                                                                  `sym:"CMSampleBufferGetFormatDescription"`
	extension      func(format, key uintptr) uintptr                                                                                                             `sym:"CMFormatDescriptionGetExtension"`
	attachments    func(sample uintptr, create bool) uintptr                                                                                                     `sym:"CMSampleBufferGetSampleAttachmentsArray"`
	timing         func(sample uintptr, index int, out *cmTimingInfo) int32                                                                                      `sym:"CMSampleBufferGetSampleTimingInfo"`
	formatFromH264 func(alloc uintptr, count int, sets *unsafe.Pointer, sizes *int, nalLength int32, out *uintptr) int32                                         `sym:"CMVideoFormatDescriptionCreateFromH264ParameterSets"`
	formatFromHEVC func(alloc uintptr, count int, sets *unsafe.Pointer, sizes *int, nalLength int32, extensions uintptr, out *uintptr) int32                     `sym:"CMVideoFormatDescriptionCreateFromHEVCParameterSets,optional"`
	blockCreate    func(alloc uintptr, memory uintptr, length int, blockAlloc uintptr, source uintptr, offset, dataLength int, flags uint32, out *uintptr) int32 `sym:"CMBlockBufferCreateWithMemoryBlock"`
	blockReplace   func(src unsafe.Pointer, block uintptr, offset, length int) int32                                                                             `sym:"CMBlockBufferReplaceDataBytes"`
	sampleCreate   func(alloc, block, format uintptr, samples, timings int, timing *cmTimingInfo, sizes int, size *int, out *uintptr) int32                      `sym:"CMSampleBufferCreateReady"`
	atomsKey       uintptr                                                                                                                                       `sym:"kCMFormatDescriptionExtension_SampleDescriptionExtensionAtoms,value"`
	notSyncKey     uintptr                                                                                                                                       `sym:"kCMSampleAttachmentKey_NotSync,value"`
}

type coreVideo struct {
	poolCreate  func(alloc, pool uintptr, out *uintptr) int32                                                 `sym:"CVPixelBufferPoolCreatePixelBuffer"`
	create      func(alloc uintptr, width, height int, format uint32, attributes uintptr, out *uintptr) int32 `sym:"CVPixelBufferCreate"`
	lock        func(buffer uintptr, flags uint64) int32                                                      `sym:"CVPixelBufferLockBaseAddress"`
	unlock      func(buffer uintptr, flags uint64) int32                                                      `sym:"CVPixelBufferUnlockBaseAddress"`
	planeBase   func(buffer uintptr, plane int) unsafe.Pointer                                                `sym:"CVPixelBufferGetBaseAddressOfPlane"`
	planeStride func(buffer uintptr, plane int) int                                                           `sym:"CVPixelBufferGetBytesPerRowOfPlane"`
	planeHeight func(buffer uintptr, plane int) int                                                           `sym:"CVPixelBufferGetHeightOfPlane"`
	planeCount  func(buffer uintptr) int                                                                      `sym:"CVPixelBufferGetPlaneCount"`
	width       func(buffer uintptr) int                                                                      `sym:"CVPixelBufferGetWidth"`
	height      func(buffer uintptr) int                                                                      `sym:"CVPixelBufferGetHeight"`
	pixelFormat func(buffer uintptr) uint32                                                                   `sym:"CVPixelBufferGetPixelFormatType"`
	formatKey   uintptr                                                                                       `sym:"kCVPixelBufferPixelFormatTypeKey,value"`
	surfaceKey  uintptr                                                                                       `sym:"kCVPixelBufferIOSurfacePropertiesKey,value"`
}

type videoToolbox struct {
	compressionCreate   func(alloc uintptr, width, height int32, codec uint32, spec, source, dataAlloc uintptr, callback, refcon uintptr, out *uintptr) int32 `sym:"VTCompressionSessionCreate"`
	setProperty         func(session, key, value uintptr) int32                                                                                               `sym:"VTSessionSetProperty"`
	prepare             func(session uintptr) int32                                                                                                           `sym:"VTCompressionSessionPrepareToEncodeFrames"`
	pool                func(session uintptr) uintptr                                                                                                         `sym:"VTCompressionSessionGetPixelBufferPool"`
	encodeFrame         func(session, image uintptr, pts, duration cmTime, properties, refcon uintptr, flags *uint32) int32                                   `sym:"VTCompressionSessionEncodeFrame"`
	complete            func(session uintptr, until cmTime) int32                                                                                             `sym:"VTCompressionSessionCompleteFrames"`
	compressionEnd      func(session uintptr)                                                                                                                 `sym:"VTCompressionSessionInvalidate"`
	decompressionCreate func(alloc, format, spec, attributes uintptr, callback *callbackRecord, out *uintptr) int32                                           `sym:"VTDecompressionSessionCreate"`
	decodeFrame         func(session, sample uintptr, flags uint32, refcon uintptr, info *uint32) int32                                                       `sym:"VTDecompressionSessionDecodeFrame"`
	finishDelayed       func(session uintptr) int32                                                                                                           `sym:"VTDecompressionSessionFinishDelayedFrames"`
	waitAsync           func(session uintptr) int32                                                                                                           `sym:"VTDecompressionSessionWaitForAsynchronousFrames"`
	decompressionEnd    func(session uintptr)                                                                                                                 `sym:"VTDecompressionSessionInvalidate"`
	hardwareDecode      func(codec uint32) bool                                                                                                               `sym:"VTIsHardwareDecodeSupported,optional"`

	requireHardwareEncoder uintptr `sym:"kVTVideoEncoderSpecification_RequireHardwareAcceleratedVideoEncoder,value"`
	lowLatencyRateControl  uintptr `sym:"kVTVideoEncoderSpecification_EnableLowLatencyRateControl,value,optional"`
	requireHardwareDecoder uintptr `sym:"kVTVideoDecoderSpecification_RequireHardwareAcceleratedVideoDecoder,value"`
	keyRealTime            uintptr `sym:"kVTCompressionPropertyKey_RealTime,value"`
	keyReordering          uintptr `sym:"kVTCompressionPropertyKey_AllowFrameReordering,value"`
	keyAverageBitRate      uintptr `sym:"kVTCompressionPropertyKey_AverageBitRate,value"`
	keyConstantBitRate     uintptr `sym:"kVTCompressionPropertyKey_ConstantBitRate,value,optional"`
	keyDataRateLimits      uintptr `sym:"kVTCompressionPropertyKey_DataRateLimits,value"`
	keyQuality             uintptr `sym:"kVTCompressionPropertyKey_Quality,value"`
	keyMaxKeyFrameInterval uintptr `sym:"kVTCompressionPropertyKey_MaxKeyFrameInterval,value"`
	keyExpectedFrameRate   uintptr `sym:"kVTCompressionPropertyKey_ExpectedFrameRate,value"`
	keyProfileLevel        uintptr `sym:"kVTCompressionPropertyKey_ProfileLevel,value"`
	keyEntropyMode         uintptr `sym:"kVTCompressionPropertyKey_H264EntropyMode,value"`
	keySpeedOverQuality    uintptr `sym:"kVTCompressionPropertyKey_PrioritizeEncodingSpeedOverQuality,value,optional"`
	keyMaxFrameDelay       uintptr `sym:"kVTCompressionPropertyKey_MaxFrameDelayCount,value"`
	entropyCAVLC           uintptr `sym:"kVTH264EntropyMode_CAVLC,value"`
	entropyCABAC           uintptr `sym:"kVTH264EntropyMode_CABAC,value"`
	forceKeyFrame          uintptr `sym:"kVTEncodeFrameOptionKey_ForceKeyFrame,value"`
}

// callbackRecord is VTDecompressionOutputCallbackRecord.
type callbackRecord struct {
	Callback uintptr
	Refcon   uintptr
}

// frameworks are the four frameworks a session uses, bound together.
type frameworks struct {
	cf coreFoundation
	cm coreMedia
	cv coreVideo
	vt videoToolbox

	vtLibrary kit.Library
}

func (f *frameworks) libraries() []kit.Library {
	const fix = "VideoToolbox ships with macOS"
	f.vtLibrary = kit.Library{Name: "VideoToolbox.framework", Path: videoToolboxPath, Fix: fix, Symbols: &f.vt}
	return []kit.Library{
		{Name: "CoreFoundation.framework", Path: coreFoundationPath, Fix: fix, Symbols: &f.cf},
		{Name: "CoreMedia.framework", Path: coreMediaPath, Fix: fix, Symbols: &f.cm},
		{Name: "CoreVideo.framework", Path: coreVideoPath, Fix: fix, Symbols: &f.cv},
		f.vtLibrary,
	}
}

func fourCC(s string) uint32 {
	return uint32(s[0])<<24 | uint32(s[1])<<16 | uint32(s[2])<<8 | uint32(s[3])
}

// CoreFoundation helpers. Every created object is the caller's to release.

const utf8 = 0x08000100

func (f *frameworks) dictionary() uintptr {
	return f.cf.dictionaryCreate(0, 0, f.cf.dictionaryKeys, f.cf.dictionaryValues)
}

func (f *frameworks) int32Number(v int32) uintptr {
	return f.cf.numberCreate(0, 3, unsafe.Pointer(&v)) // kCFNumberSInt32Type
}

func (f *frameworks) int64Number(v int64) uintptr {
	return f.cf.numberCreate(0, 4, unsafe.Pointer(&v)) // kCFNumberSInt64Type
}

func (f *frameworks) float64Number(v float64) uintptr {
	return f.cf.numberCreate(0, 6, unsafe.Pointer(&v)) // kCFNumberFloat64Type
}

func (f *frameworks) boolean(v bool) uintptr {
	if v {
		return f.cf.booleanTrue
	}
	return f.cf.booleanFalse
}

func (f *frameworks) cfString(s string) uintptr {
	b := append([]byte(s), 0)
	return f.cf.stringCreate(0, &b[0], utf8)
}

// setOwned sets a dictionary value and releases the caller's reference.
func (f *frameworks) setOwned(dict, key, value uintptr) {
	f.cf.dictionarySet(dict, key, value)
	f.cf.release(value)
}

// bytesOf copies a CFData's bytes.
func (f *frameworks) bytesOf(data uintptr) []byte {
	n := f.cf.dataLength(data)
	if n <= 0 {
		return nil
	}
	return append([]byte(nil), unsafe.Slice((*byte)(f.cf.dataBytes(data)), n)...)
}
