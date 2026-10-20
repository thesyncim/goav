- Add optional cameraav live video capture for macOS, Linux, and Windows without
  cgo, with exact mode selection, bounded native queues, and fixed Go frame storage.
  Register camera:// inputs in the CLI, add camera device listing, and provide a
  macOS permission-bundle helper. See cameraav/README.md for supported formats,
  allocation guarantees, access requirements, and hardware-validation limits.
