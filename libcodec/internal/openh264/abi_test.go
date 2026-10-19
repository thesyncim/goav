package openh264

import (
	"testing"
	"unsafe"
)

// TestABIMatchesTheCHeaders pins every mirrored structure to the sizes and
// offsets the C compiler gives codec_app_def.h of OpenH264 2.5 (layout 25)
// and 2.6 (layout 26) on LP64 targets.
func TestABIMatchesTheCHeaders(t *testing.T) {
	var p encParamExt
	var l spatialLayer
	var s sourcePicture
	var b bufferInfo
	var d decodingParam
	var f25 frameBSInfo
	var f26 frameBSInfo26
	for _, c := range []struct {
		name      string
		got, want uintptr
	}{
		{"SEncParamExt (2.6)", unsafe.Sizeof(p), 924},
		{"SEncParamExt.sSpatialLayers", unsafe.Offsetof(p.SpatialLayers), 32},
		{"SEncParamExt.iComplexityMode", unsafe.Offsetof(p.ComplexityMode), 832},
		{"SEncParamExt.bPrefixNalAddingCtrl", unsafe.Offsetof(p.PrefixNalAddingCtrl), 848},
		{"SEncParamExt.iPaddingFlag", unsafe.Offsetof(p.PaddingFlag), 852},
		{"SEncParamExt.iMaxBitrate", unsafe.Offsetof(p.MaxBitrate), 864},
		{"SEncParamExt.uiMaxNalSize", unsafe.Offsetof(p.MaxNalSize), 876},
		{"SEncParamExt.iLtrMarkPeriod", unsafe.Offsetof(p.LtrMarkPeriod), 888},
		{"SEncParamExt.iMultipleThreadIdc", unsafe.Offsetof(p.MultipleThreadIdc), 892},
		{"SEncParamExt.bUseLoadBalancing", unsafe.Offsetof(p.UseLoadBalancing), 894},
		{"SEncParamExt.iLoopFilterDisableIdc", unsafe.Offsetof(p.LoopFilterDisableIdc), 896},
		{"SEncParamExt.bEnableDenoise", unsafe.Offsetof(p.EnableDenoise), 908},
		{"SEncParamExt.iIdrBitrateRatio", unsafe.Offsetof(p.IdrBitrateRatio), 916},
		{"SSpatialLayerConfig", unsafe.Sizeof(l), 200},
		{"SSpatialLayerConfig.sSliceArgument", unsafe.Offsetof(l.Slices), 32},
		{"SSpatialLayerConfig.bVideoSignalTypePresent", unsafe.Offsetof(l.VideoSignalTypePresent), 184},
		{"SSpatialLayerConfig.eAspectRatio", unsafe.Offsetof(l.AspectRatio), 192},
		{"SSpatialLayerConfig.sAspectRatioExtHeight", unsafe.Offsetof(l.AspectRatioExtHeight), 198},
		{"SSliceArgument", unsafe.Sizeof(l.Slices), 152},
		{"SSourcePicture (2.6)", unsafe.Sizeof(s), 80},
		{"SSourcePicture.pData", unsafe.Offsetof(s.Data), 24},
		{"SSourcePicture.iPicWidth", unsafe.Offsetof(s.PicWidth), 56},
		{"SSourcePicture.uiTimeStamp", unsafe.Offsetof(s.TimeStamp), 64},
		{"SLayerBSInfo (2.5)", unsafe.Sizeof(f25.Layers[0]), 40},
		{"SLayerBSInfo (2.6)", unsafe.Sizeof(f26.Layers[0]), 56},
		{"SLayerBSInfo.iNalCount", unsafe.Offsetof(f25.Layers[0].NalCount), 16},
		{"SLayerBSInfo.pBsBuf", unsafe.Offsetof(f25.Layers[0].BsBuf), 32},
		{"SFrameBSInfo (2.5)", unsafe.Sizeof(f25), 5144},
		{"SFrameBSInfo.eFrameType (2.5)", unsafe.Offsetof(f25.FrameType), 5128},
		{"SFrameBSInfo (2.6)", unsafe.Sizeof(f26), 7192},
		{"SFrameBSInfo.eFrameType (2.6)", unsafe.Offsetof(f26.FrameType), 7176},
		{"SBufferInfo", unsafe.Sizeof(b), 72},
		{"SBufferInfo.uiOutYuvTimeStamp", unsafe.Offsetof(b.OutTimeStamp), 16},
		{"SBufferInfo.UsrData", unsafe.Offsetof(b.System), 24},
		{"SBufferInfo.pDst", unsafe.Offsetof(b.Dst), 48},
		{"SDecodingParam", unsafe.Sizeof(d), 32},
		{"SDecodingParam.eEcActiveIdc", unsafe.Offsetof(d.EcActiveIdc), 16},
		{"SDecodingParam.sVideoProperty", unsafe.Offsetof(d.VideoPropertySize), 24},
	} {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
}
