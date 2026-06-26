# API Inventory

This started as the baseline taken before the API reduction work. It should move
downward as symbols leave the root package, bundled adapters move behind
`goav/bundle`, and implementation-specific error codes collapse into stable error
families.

## Approved Symbol Counts

Source: `testdata/api_surface.txt`.

| Package | Initial identifiers | Current identifiers |
| --- | ---: | ---: |
| `goav` | 134 | 55 |
| `control` | 0 | 22 |
| `errcode` | 147 | 164 |
| `graphrender` | 9 | 9 |
| `inspect` | 0 | 8 |
| `lifecycle` | 13 | 24 |
| `plan` | 28 | 28 |
| `snapshot` | 4 | 4 |

## Root Dependency Baseline

Initial command:

```sh
go list -deps github.com/thesyncim/goav
```

Initial result: the root package dependency graph included the bundled adapter
packages and backend codec modules.

Bundled packages included in the initial root graph:

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

The target check after the bundle package split is:

```sh
go list -deps github.com/thesyncim/goav |
  rg 'goaac|goav1|goh264|gopus|govpx|adapters/(annexb|ivf|resample|resize)|container/(matroska|mp4|webm)'
```

Expected result after the bundle package split: no matches for the root
package. Importing `goav/bundle` should pull the bundled adapter packages intentionally.

Current result after the `goav/bundle` split: no matches for the root package.

`goav/bundle` is a package in the root module, not a nested module. Importing
`github.com/thesyncim/goav` does not pull bundled adapter packages into the
root package dependency graph. The root module still carries bundled backend
requirements until/unless `goav/bundle` becomes a nested module.

## Documentation Baseline

Current line counts:

| File | Lines |
| --- | ---: |
| `README.md` | 74 |
| `docs/API_SURFACE.md` | 378 |
| `docs/ROADMAP.md` | 244 |
| `docs/PROGRESS.md` | 138 |
| `docs/API_REDUCTION_PLAN.md` | 211 |
| `docs/SIMPLIFICATION_TARGET.md` | 201 |

The README meets the <=120 line target after the advanced vocabulary moved into
focused docs.
