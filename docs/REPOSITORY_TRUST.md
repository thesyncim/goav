# Repository trust surface

This is the public trust checklist for the repository page and release posture.
It keeps the GitHub-facing metadata, no-release-yet stance, and local trust
files explicit so reviewers do not have to infer them from scattered settings.
The values are intentionally small and adoption-focused.

## GitHub metadata

Expected repository metadata:

- Description: `Pure-Go media workflow runtime for validated, inspectable, in-process audio/video pipelines`
- Homepage: `https://pkg.go.dev/github.com/thesyncim/goav`
- Topics: `audio`, `codecs`, `go`, `media`, `pure-go`, `realtime`, `rtp`,
  `streaming`, `video`, `webrtc`

Verify with:

```sh
gh repo view thesyncim/goav \
  --json description,homepageUrl,repositoryTopics,latestRelease \
  --jq '{description, homepageUrl, topics: [.repositoryTopics[].name], latestRelease}'
```

## Release posture

No release should be published until the maintainer intentionally cuts a signed
v0.x or v1 tag. An empty release list is acceptable while the README says
pre-v1 and `docs/ROADMAP.md` keeps the release decision open.

Before a release appears, the release evidence must include:

- `CHANGELOG.md` version notes;
- `docs/COMPATIBILITY.md` filled for the tag;
- `.github/workflows/release.yml` signed-tag validation;
- release artifacts or source release notes generated from the tag;
- benchmark artifacts only when performance claims depend on them.

Verify current releases with:

```sh
gh release list --repo thesyncim/goav --limit 10
```

## Local trust files

The repository trust files are part of the adoption surface:

- `README.md` front-door status, badges, and doc links;
- `CHANGELOG.md` release-note history;
- `CONTRIBUTING.md` contributor workflow and changelog rules;
- `SECURITY.md` private vulnerability-reporting process;
- `SUPPORT.md` support expectations while pre-v1;
- `.github/PULL_REQUEST_TEMPLATE.md` reviewer evidence checklist;
- `docs/RELEASING.md` and `docs/COMPATIBILITY.md` release decision evidence.
