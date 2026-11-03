# Packaging

Two ways to install on Linux, plus the Windows archive.

| Route | Best for | Needs |
|---|---|---|
| GitHub release asset (static musl) | Arch, Manjaro, EndeavourOS, CachyOS, Bazzite, any distro | nothing |
| `packaging/aur/PKGBUILD` | Arch and derivatives, managed by pacman | `base-devel`, `rust` |
| `cargo install --path .` | development machines | a Rust toolchain |

## Arch Linux (AUR)

The PKGBUILD in `aur/` is the `-git` flavour: it builds the current default
branch, so it carries no release-asset checksums and never goes stale.

```sh
git clone https://github.com/mohamedpresident9-cmd/chunked-parallel
cd chunked-parallel/packaging/aur
makepkg -si
```

Or, once the package is on the AUR:

```sh
paru -S chunked-parallel-git    # or: yay -S chunked-parallel-git
```

Publishing it to the AUR needs an AUR account and an SSH key registered there;
the upload itself is free:

```sh
git clone ssh://aur@aur.archlinux.org/chunked-parallel-git.git
cp PKGBUILD chunked-parallel-git/
cd chunked-parallel-git
makepkg --printsrcinfo > .SRCINFO
git add PKGBUILD .SRCINFO && git commit -m "initial" && git push
```

## Bazzite / Fedora atomic

Do not layer a package onto an immutable root with `rpm-ostree` just for this.
Use the static binary, Homebrew, or a container:

```sh
# static binary, works on the immutable root as-is
curl -fsSL https://raw.githubusercontent.com/mohamedpresident9-cmd/chunked-parallel/main/scripts/install.sh | sh

# or inside a toolbox
toolbox run -c rust cargo install --path .
```

## Windows

Unzip `cpx-x86_64-pc-windows-msvc.zip` from the latest release, or run
`cargo install --path .` from a checkout.
