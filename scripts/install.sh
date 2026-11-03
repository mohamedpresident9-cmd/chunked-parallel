#!/bin/sh
# Install the latest `cpx` release for Linux.
#
#   curl -fsSL https://raw.githubusercontent.com/mohamedpresident9-cmd/chunked-parallel/main/scripts/install.sh | sh
#
# Works on any glibc or non-glibc distribution because the published binary is
# statically linked against musl: Arch, Manjaro, EndeavourOS, CachyOS, Bazzite,
# Fedora atomic images, Alpine, and so on.
set -eu

REPO="mohamedpresident9-cmd/chunked-parallel"
BIN="cpx"

die() {
    printf 'error: %s\n' "$1" >&2
    exit 1
}

command -v curl >/dev/null 2>&1 || die "curl is required"
command -v tar >/dev/null 2>&1 || die "tar is required"

os="$(uname -s)"
[ "$os" = "Linux" ] || die "unsupported OS: $os (this script handles Linux only)"

arch="$(uname -m)"
case "$arch" in
    x86_64 | amd64) target="x86_64-unknown-linux-musl" ;;
    aarch64 | arm64) target="aarch64-unknown-linux-musl" ;;
    *) die "unsupported architecture: $arch" ;;
esac

# Resolve the newest tag without requiring jq.
tag="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
    | head -n 1)"
[ -n "$tag" ] || die "could not resolve the latest release tag"

url="https://github.com/${REPO}/releases/download/${tag}/${BIN}-${target}.tar.gz"
printf 'downloading %s (%s)\n' "$url" "$tag"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM

curl -fsSL "$url" | tar -xz -C "$tmp" || die "download or extraction failed"
[ -f "$tmp/$BIN" ] || die "archive did not contain $BIN"

# Prefer a system-wide install, fall back to the user directory.
if [ -w /usr/local/bin ] 2>/dev/null; then
    dest="/usr/local/bin"
else
    dest="${HOME}/.local/bin"
    mkdir -p "$dest"
fi

install -m 0755 "$tmp/$BIN" "$dest/$BIN" 2>/dev/null \
    || { cp "$tmp/$BIN" "$dest/$BIN" && chmod 0755 "$dest/$BIN"; }

printf 'installed %s -> %s/%s\n' "$tag" "$dest" "$BIN"
case ":$PATH:" in
    *":$dest:"*) ;;
    *) printf 'note: add %s to PATH\n' "$dest" ;;
esac

"$dest/$BIN" --help >/dev/null 2>&1 && printf 'ok: %s runs\n' "$BIN"
