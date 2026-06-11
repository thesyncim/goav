# Control-plane host example

This example starts a deterministic live task and exposes it through a Unix
control socket. It registers:

- `vendor.rate`, a custom control command.
- `meter`, a custom branch-pipeline step.
- `acmeenc`, a custom encoder spelling that maps CLI settings into a codec spec.

Run the host:

```sh
go run ./examples/control-plane-host --control unix:///tmp/goav-control-plane-host.sock
```

In another shell:

```sh
goav ctl --control unix:///tmp/goav-control-plane-host.sock help attach
goav ctl --control unix:///tmp/goav-control-plane-host.sock control vendor.rate value=0.5 source=fixture
goav ctl --control unix:///tmp/goav-control-plane-host.sock attach frames as archive \
  'meter label="left ! right" ! acmeenc bitrate=128000 quality=voice lookahead=deep ! filesink location="/tmp/goav archive.ogg" format=ogg'
goav ctl --control unix:///tmp/goav-control-plane-host.sock graph
goav ctl --control unix:///tmp/goav-control-plane-host.sock detach archive
```

`help attach` is server-aware, so it lists the custom step and encoder registered
by this host, including aliases, summaries, and usage strings.
