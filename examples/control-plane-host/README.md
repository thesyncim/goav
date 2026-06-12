# Control-plane host example

This example starts a deterministic live task and exposes it through a Unix
control socket. It registers:

- `vendor.rate`, a custom control command.
- `meter`, a custom branch-pipeline step.
- `memorysink`, a custom destination step backed by an in-process sink.
- `acmeenc`, a custom encoder spelling that maps CLI settings into a codec spec.

Run the host:

```sh
go run ./examples/control-plane-host --control unix:///tmp/goav-control-plane-host.sock
```

In another shell:

```sh
goav ctl --control unix:///tmp/goav-control-plane-host.sock help attach
goav ctl --control unix:///tmp/goav-control-plane-host.sock help control vendor.rate
goav ctl --control unix:///tmp/goav-control-plane-host.sock taps
goav ctl --control unix:///tmp/goav-control-plane-host.sock control vendor.rate value=0.5 source=fixture
goav ctl --control unix:///tmp/goav-control-plane-host.sock attach frames as archive \
  'meter label="left ! right" ! acmeenc bitrate=128000 quality=voice lookahead=deep ! filesink location="/tmp/goav archive.ogg" format=ogg'
goav ctl --control unix:///tmp/goav-control-plane-host.sock attach frames as memory \
  'meter ! acmeenc bitrate=64000 quality=preview lookahead=shallow ! memorysink name=preview'
goav ctl --control unix:///tmp/goav-control-plane-host.sock graph
goav ctl --control unix:///tmp/goav-control-plane-host.sock graph format=text
goav ctl --control unix:///tmp/goav-control-plane-host.sock rebranch archive \
  'meter ! acmeenc bitrate=96000 quality=voice lookahead=shallow ! filesink location="/tmp/goav archive-low.ogg" format=ogg'
goav ctl --control unix:///tmp/goav-control-plane-host.sock detach archive
goav ctl --control unix:///tmp/goav-control-plane-host.sock detach memory
```

`help attach` is server-aware, so it lists the custom steps and encoder
registered by this host, including aliases, summaries, and usage strings. The
`graph` commands render the live task snapshot after attachments, including
branch lifecycle annotations.
