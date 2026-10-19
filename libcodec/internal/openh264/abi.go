package openh264

import "unsafe"

// The C structures of OpenH264's codec_app_def.h and codec_def.h, mirrored
// field for field. OpenH264 2.6 appended PSNR fields to three of them (its
// soname 8); 2.3 through 2.5 share the earlier layout (soname 7). Go lays
// these out like C on every supported target; abi_test.go pins each size
// and offset against values measured with the C compiler.

const (
	maxSpatialLayers = 4
	maxSlices        = 35  // MAX_SLICES_NUM_TMP
	maxLayers        = 128 // MAX_LAYER_NUM_OF_FRAME
)

type version struct {
	Major, Minor, Revision, Reserved uint32
}

type sliceArgument struct {
	SliceMode       int32  `c:"uiSliceMode"`
	SliceNum        uint32 `c:"uiSliceNum"`
	SliceMbNum      [maxSlices]uint32
	SliceSizeConstr uint32 `c:"uiSliceSizeConstraint"`
}

type spatialLayer struct {
	VideoWidth             int32         `c:"iVideoWidth"`
	VideoHeight            int32         `c:"iVideoHeight"`
	FrameRate              float32       `c:"fFrameRate"`
	SpatialBitrate         int32         `c:"iSpatialBitrate"`
	MaxSpatialBitrate      int32         `c:"iMaxSpatialBitrate"`
	ProfileIdc             int32         `c:"uiProfileIdc"`
	LevelIdc               int32         `c:"uiLevelIdc"`
	DLayerQp               int32         `c:"iDLayerQp"`
	Slices                 sliceArgument `c:",inline"`
	VideoSignalTypePresent bool          `c:"bVideoSignalTypePresent"`
	VideoFormat            uint8         `c:"uiVideoFormat"`
	FullRange              bool          `c:"bFullRange"`
	ColorDescPresent       bool          `c:"bColorDescriptionPresent"`
	ColorPrimaries         uint8         `c:"uiColorPrimaries"`
	TransferChar           uint8         `c:"uiTransferCharacteristics"`
	ColorMatrix            uint8         `c:"uiColorMatrix"`
	AspectRatioPresent     bool          `c:"bAspectRatioPresent"`
	AspectRatio            int32         `c:"eAspectRatio"`
	AspectRatioExtWidth    uint16        `c:"sAspectRatioExtWidth"`
	AspectRatioExtHeight   uint16        `c:"sAspectRatioExtHeight"`
}

// encParamExt is SEncParamExt in the 2.6 layout: earlier versions read and
// write all but the trailing PSNR flags.
type encParamExt struct {
	UsageType            int32                          `c:"iUsageType"`
	PicWidth             int32                          `c:"iPicWidth"`
	PicHeight            int32                          `c:"iPicHeight"`
	TargetBitrate        int32                          `c:"iTargetBitrate"`
	RCMode               int32                          `c:"iRCMode"`
	MaxFrameRate         float32                        `c:"fMaxFrameRate"`
	TemporalLayerNum     int32                          `c:"iTemporalLayerNum"`
	SpatialLayerNum      int32                          `c:"iSpatialLayerNum"`
	SpatialLayers        [maxSpatialLayers]spatialLayer `c:",first"`
	ComplexityMode       int32                          `c:"iComplexityMode"`
	IntraPeriod          uint32                         `c:"uiIntraPeriod"`
	NumRefFrame          int32                          `c:"iNumRefFrame"`
	SpsPpsIDStrategy     int32                          `c:"eSpsPpsIdStrategy"`
	PrefixNalAddingCtrl  bool                           `c:"bPrefixNalAddingCtrl"`
	EnableSSEI           bool                           `c:"bEnableSSEI"`
	SimulcastAVC         bool                           `c:"bSimulcastAVC"`
	PaddingFlag          int32                          `c:"iPaddingFlag"`
	EntropyCodingMode    int32                          `c:"iEntropyCodingModeFlag"`
	EnableFrameSkip      bool                           `c:"bEnableFrameSkip"`
	MaxBitrate           int32                          `c:"iMaxBitrate"`
	MaxQp                int32                          `c:"iMaxQp"`
	MinQp                int32                          `c:"iMinQp"`
	MaxNalSize           uint32                         `c:"uiMaxNalSize"`
	EnableLongTermRef    bool                           `c:"bEnableLongTermReference"`
	LTRRefNum            int32                          `c:"iLTRRefNum"`
	LtrMarkPeriod        uint32                         `c:"iLtrMarkPeriod"`
	MultipleThreadIdc    uint16                         `c:"iMultipleThreadIdc"`
	UseLoadBalancing     bool                           `c:"bUseLoadBalancing"`
	LoopFilterDisableIdc int32                          `c:"iLoopFilterDisableIdc"`
	LoopFilterAlphaC0    int32                          `c:"iLoopFilterAlphaC0Offset"`
	LoopFilterBeta       int32                          `c:"iLoopFilterBetaOffset"`
	EnableDenoise        bool                           `c:"bEnableDenoise"`
	EnableBackground     bool                           `c:"bEnableBackgroundDetection"`
	EnableAdaptiveQuant  bool                           `c:"bEnableAdaptiveQuant"`
	EnableFrameCropping  bool                           `c:"bEnableFrameCroppingFlag"`
	EnableSceneChange    bool                           `c:"bEnableSceneChangeDetect"`
	IsLosslessLink       bool                           `c:"bIsLosslessLink"`
	FixRCOverShoot       bool                           `c:"bFixRCOverShoot"`
	IdrBitrateRatio      int32                          `c:"iIdrBitrateRatio"`
	PsnrY, PsnrU, PsnrV  bool
}

// sourcePicture is SSourcePicture in the 2.6 layout.
type sourcePicture struct {
	ColorFormat         int32
	Stride              [4]int32
	Data                [4]unsafe.Pointer
	PicWidth, PicHeight int32
	TimeStamp           int64
	PsnrY, PsnrU, PsnrV bool
}

// layerInfo is SLayerBSInfo before 2.6; layerInfo26 adds the PSNR floats.
type layerInfo struct {
	TemporalID, SpatialID, QualityID uint8
	FrameType                        int32
	LayerType                        uint8
	SubSeqID                         int32
	NalCount                         int32
	NalLengthInByte                  *int32
	BsBuf                            *byte
}

type layerInfo26 struct {
	layerInfo
	Psnr [3]float32
}

type frameBSInfo struct {
	LayerNum       int32
	Layers         [maxLayers]layerInfo
	FrameType      int32
	FrameSizeBytes int32
	TimeStamp      int64
}

type frameBSInfo26 struct {
	LayerNum       int32
	Layers         [maxLayers]layerInfo26
	FrameType      int32
	FrameSizeBytes int32
	TimeStamp      int64
}

type bitrateInfo struct {
	Layer   int32
	Bitrate int32
}

type sysMemBuffer struct {
	Width, Height, Format int32
	Stride                [2]int32
}

type bufferInfo struct {
	BufferStatus int32
	InTimeStamp  uint64
	OutTimeStamp uint64
	System       sysMemBuffer
	Dst          [3]*byte
}

type decodingParam struct {
	FileNameRestructed *byte
	CPULoad            uint32
	TargetDqLayer      uint8
	EcActiveIdc        int32
	ParseOnly          bool
	VideoPropertySize  uint32
	VideoBsType        int32
}

// encoderVtable is ISVCEncoderVtbl: slot order is ABI.
type encoderVtable struct {
	Initialize          func(this uintptr, param unsafe.Pointer) int32
	InitializeExt       func(this uintptr, param *encParamExt) int32
	GetDefaultParams    func(this uintptr, param *encParamExt) int32
	Uninitialize        func(this uintptr) int32
	EncodeFrame         func(this uintptr, src *sourcePicture, info unsafe.Pointer) int32
	EncodeParameterSets func(this uintptr, info unsafe.Pointer) int32
	ForceIntraFrame     func(this uintptr, idr bool, layer int32) int32
	SetOption           func(this uintptr, option int32, value unsafe.Pointer) int32
	GetOption           func(this uintptr, option int32, value unsafe.Pointer) int32
}

// decoderVtable is ISVCDecoderVtbl.
type decoderVtable struct {
	Initialize         func(this uintptr, param *decodingParam) int32
	Uninitialize       func(this uintptr) int32
	DecodeFrame        func(this uintptr, src *byte, size int32, dst unsafe.Pointer, stride unsafe.Pointer, width, height unsafe.Pointer) int32
	DecodeFrameNoDelay func(this uintptr, src *byte, size int32, dst *[3]*byte, info *bufferInfo) int32
	DecodeFrame2       func(this uintptr, src *byte, size int32, dst *[3]*byte, info *bufferInfo) int32
	FlushFrame         func(this uintptr, dst *[3]*byte, info *bufferInfo) int32
	DecodeParser       func(this uintptr, src *byte, size int32, info unsafe.Pointer) int32
	DecodeFrameEx      func(this uintptr, src *byte, size int32, dst *byte, stride int32, length, width, height, format unsafe.Pointer) int32
	SetOption          func(this uintptr, option int32, value unsafe.Pointer) int32
	GetOption          func(this uintptr, option int32, value unsafe.Pointer) int32
}

// Constants from codec_app_def.h and codec_def.h.
const (
	videoFormatI420 = 23

	frameTypeInvalid = 0
	frameTypeIDR     = 1
	frameTypeSkip    = 4

	optionDataFormat  = 0
	optionIDRInterval = 1
	optionBitrate     = 5
	optionMaxBitrate  = 6
	optionTraceLevel  = 25

	decoderOptionTraceLevel = 9
	decoderOptionThreads    = 19

	spatialLayerAll = 4
	constantID      = 0
	sliceSingle     = 0
	sliceFixedNum   = 1
)
