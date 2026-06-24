# API Inventory

This started as the baseline taken before the API reduction work. It should move
downward as symbols leave the root package, standard adapters move behind
`goav/std`, and implementation-specific error codes collapse into stable error
families.

## Approved Symbol Counts

Source: `testdata/api_surface.txt`.

| Package | Initial identifiers | Current identifiers |
| --- | ---: | ---: |
| `goav` | 134 | 116 |
| `control` | 0 | 19 |
| `errcode` | 147 | 146 |
| `graphrender` | 9 | 9 |
| `inspect` | 0 | 3 |
| `lifecycle` | 13 | 13 |
| `plan` | 28 | 28 |
| `snapshot` | 4 | 4 |

## Root Dependency Baseline

Initial command:

```sh
go list -deps github.com/thesyncim/goav
```

Initial result: the root package dependency graph included the standard adapter
packages and backend codec modules.

Standard packages currently pulled by the root include:

- `github.com/thesyncim/goav/adapters/annexb`
- `github.com/thesyncim/goav/adapters/goaac`
- `github.com/thesyncim/goav/adapters/goav1`
- `github.com/thesyncim/goav/adapters/goh264`
- `github.com/thesyncim/goav/adapters/gopus`
- `github.com/thesyncim/goav/adapters/govpx`
- `github.com/thesyncim/goav/adapters/ivf`
- `github.com/thesyncim/goav/adapters/resample`
- `github.com/thesyncim/goav/adapters/resize`
- `github.com/thesyncim/goav/container/matroska`
- `github.com/thesyncim/goav/container/mp4`
- `github.com/thesyncim/goav/container/webm`
- `github.com/thesyncim/goaac`
- `github.com/thesyncim/goav1`
- `github.com/thesyncim/gopus`
- `github.com/thesyncim/govpx`

The target check after the standard package split is:

```sh
go list -deps github.com/thesyncim/goav |
  rg 'goaac|goav1|goh264|gopus|govpx|adapters/(annexb|ivf|resample|resize)|container/(matroska|mp4|webm)'
```

Expected result after the standard package split: no matches for the root
package. Importing `goav/std` should pull the standard bundle intentionally.

Current result after the `goav/std` split: no matches for the root package.

## Documentation Baseline

Current line counts:

| File | Lines |
| --- | ---: |
| `README.md` | 198 |
| `docs/API_SURFACE.md` | 355 |
| `docs/ROADMAP.md` | 224 |
| `docs/PROGRESS.md` | 137 |

The README target is under 200 lines after the advanced vocabulary moves into
focused docs.
