#!/usr/bin/env bash
# vuln.sh runs govulncheck over each module on the Go that module builds
# with: the one its go.mod's toolchain line names. govulncheck reports the
# standard library as well as the modules, so which Go it runs on is part of
# what it checks.
#
# The tool is built first, on whatever Go it asks for, and run after. Under
# `go run`, a module on an older Go than the tool needs has the go command
# switch to a newer one to build it, and the scan then reads the newer Go's
# standard library: a module on go1.25.13 was reported clean against
# go1.26.10. govulncheck says which Go it read, and if that is not the
# toolchain line this fails.
#
#   scripts/vuln.sh            every module
#   scripts/vuln.sh s3         one
#
# GOOS picks the platform the scan reads as: CI runs on Linux.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
modules=("$@")
if [ ${#modules[@]} -eq 0 ]; then
	modules=(. s3 gcs azblob zstd geodb pycompare)
fi

bin=$(mktemp -d)
trap 'rm -rf "$bin"' EXIT
# Built for this machine, whatever GOOS the scan is to read as.
GOBIN=$bin GOOS=$(go env GOHOSTOS) GOARCH=$(go env GOHOSTARCH) GOTOOLCHAIN=auto \
	go install golang.org/x/vuln/cmd/govulncheck@latest

status=0
for m in "${modules[@]}"; do
	want=$(awk '$1 == "toolchain" { print $2 }' "$root/$m/go.mod")
	if [ -z "$want" ]; then
		echo "$m/go.mod has no toolchain line: name the Go it builds with" >&2
		status=1
		continue
	fi
	echo "==> govulncheck $m on $want"
	out=$(cd "$root/$m" && GOTOOLCHAIN=$want "$bin/govulncheck" -show version ./...) || status=1
	echo "$out"
	got=$(awk '$1 == "Go:" { print $2; exit }' <<<"$out")
	if [ -z "$got" ]; then
		echo "govulncheck did not say which Go it read for $m" >&2
		status=1
	elif [ "$got" != "$want" ]; then
		echo "govulncheck read the standard library of $got, but $m builds with $want" >&2
		status=1
	fi
done
exit $status
