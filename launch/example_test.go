package launch_test

import (
	"fmt"

	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/launch"
)

// ExampleCompile compiles a program and prints the graph it builds, without
// running it.
func ExampleCompile() {
	outs, err := launch.Compile(`
		v := testsrc video size=640x360 duration=1s
		v ! resize 320x180 ! encode vp8 ! as small ! null
		v ! encode vp8 bitrate=1M ! as full ! null
	`)
	if err != nil {
		fmt.Println(err)
		return
	}
	spec, err := bundle.Describe(outs...)
	if err != nil {
		fmt.Println(err)
		return
	}
	text, _ := graphrender.RenderURI(spec, "goav:graph")
	fmt.Print(text)
	// Output:
	// pipeline goav
	//   source video [generated test video]
	//   stage select-video [select, type=video]
	//   stage resize-small [resize, 320x180, exact]
	//   stage encode-small [frames -> packets, codec=vp8, backend=govpx (go)]
	//   stage encode-full [frames -> packets, codec=vp8, backend=govpx (go)]
	//   sink null
	//   video -> select-video
	//   select-video -> resize-small
	//   select-video -> encode-full
	//   resize-small -> encode-small
	//   encode-small -> null
	//   encode-full -> null
}
