package cameraav

import (
	"fmt"
	"github.com/thesyncim/goav/provider"
	"net/url"
	"strconv"
)

// Scheme binds camera://default?width=1280&height=720&fps=30. Select a specific
// opaque device ID with the percent-encoded device query parameter. Binding is
// pure, so Describe and Explain neither enumerate nor activate cameras.
type Scheme struct{}

func (Scheme) Input(u *url.URL) (provider.Input, error) {
	if u == nil || u.Scheme != "camera" || (u.Host != "default" && u.Host != "") || u.Path != "" || u.User != nil || u.Fragment != "" {
		return nil, fmt.Errorf("cameraav: expected camera://default?device=ID&width=W&height=H&fps=N")
	}
	values, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, err
	}
	var c Config
	for key, v := range values {
		if len(v) != 1 {
			return nil, fmt.Errorf("cameraav: repeated option %q", key)
		}
		if key == "device" {
			c.Device = v[0]
			continue
		}
		n, e := strconv.Atoi(v[0])
		if e != nil || n <= 0 {
			return nil, fmt.Errorf("cameraav: invalid %s", key)
		}
		switch key {
		case "width":
			c.Width = n
		case "height":
			c.Height = n
		case "fps":
			c.FPS = n
		case "queue":
			c.Queue = n
		default:
			return nil, fmt.Errorf("cameraav: unknown option %q", key)
		}
	}
	c, err = normalize(c)
	if err != nil {
		return nil, err
	}
	return New(c), nil
}
