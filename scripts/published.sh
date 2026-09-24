#!/usr/bin/env bash
# published.sh builds and tests each sub-module the way a consumer gets it:
# copied out of the repository, with its replace of the core dropped, so the
# core is whatever version its go.mod requires, fetched like any dependency.
#
# Go ignores replace directives in a dependency, so a sub-module whose
# require names a core that was never tagged - or a tag without what the
# sub-module uses - builds here and fails for everyone who imports it. This
# is the check that it does not.
#
#   scripts/published.sh            every sub-module
#   scripts/published.sh zstd       one
set -euo pipefail

CORE=github.com/LukasSelin/zarr
ZERO=v0.0.0-00010101000000-000000000000
root=$(cd "$(dirname "$0")/.." && pwd)
modules=("$@")
if [ ${#modules[@]} -eq 0 ]; then
	modules=(s3 zstd)
fi

status=0
for m in "${modules[@]}"; do
	echo "==> published $m"
	want=$(cd "$root/$m" && go list -m -f '{{.Version}}' "$CORE")
	if [ "$want" = "$ZERO" ] || [ -z "$want" ]; then
		echo "$m/go.mod requires $CORE at $want, which is no version: require a tag of the core" >&2
		status=1
		continue
	fi
	tmp=$(mktemp -d)
	cp -R "$root/$m/." "$tmp/"
	if ! (
		cd "$tmp" &&
			go mod edit -dropreplace="$CORE" &&
			GOWORK=off GOFLAGS=-mod=mod go build ./... &&
			GOWORK=off GOFLAGS=-mod=mod go vet ./... &&
			GOWORK=off GOFLAGS=-mod=mod go test -count=1 ./...
	); then
		echo "$m does not build and pass its tests against $CORE $want, the core it requires: tag the core it needs and require that" >&2
		status=1
	fi
	rm -rf "$tmp"
done
exit $status
