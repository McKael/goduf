# goduf

Goduf is a fast duplicate file finder.

[![license](https://img.shields.io/badge/license-MIT-blue.svg?style=flat)](https://raw.githubusercontent.com/McKael/goduf/master/LICENSE)
[![Build Status](https://travis-ci.org/McKael/goduf.svg?branch=master)](https://travis-ci.org/McKael/goduf)

## Usage

The typical usage is very simple:

```
% goduf DIRS...
```

Examples:

```
% goduf /usr/bin
Group #1 (2 files * 76 bytes):
/usr/bin/vam
/usr/bin/vim-addons

Group #2 (2 files * 292 bytes):
/usr/bin/pip
/usr/bin/pip2

Group #3 (3 files * 1134 bytes):
/usr/bin/gajim
/usr/bin/gajim-history-manager
/usr/bin/gajim-remote

Group #4 (2 files * 1303 bytes):
/usr/bin/pdftexi2dvi
/usr/bin/texi2pdf

Group #5 (7 files * 4791 bytes):
/usr/bin/ansible
/usr/bin/ansible-console
/usr/bin/ansible-doc
/usr/bin/ansible-galaxy
/usr/bin/ansible-playbook
/usr/bin/ansible-pull
/usr/bin/ansible-vault

(...)
```

```
% goduf -summary /usr/share/doc
2018/04/07 21:48:23 Final count: 5970 duplicate files in 1920 sets
2018/04/07 21:48:23 Redundant data size: 107594575 bytes (102 MiB)
```

Use `goduf -h` to get the list of available options.

*Note for Windows users*: goduf does not normalize paths on Windows, so be careful not to specify the same path twice.

On Linux, hard links are automatically excluded.

## Installation:

From the Github mirror:

```
% go get hg.lilotux.net/golang/mikael/goduf
```

From my Mercurial repository (upstream):

```
% go get github.com/McKael/goduf
```
