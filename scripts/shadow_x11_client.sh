#!/bin/sh
# Keep the pinned display and tools, but avoid TCP fallback in the guarded namespace.
export DISPLAY=unix/:99
case "${0##*/}" in
  xdotool) exec /usr/bin/xdotool "$@" ;;
  openbox) exec /usr/bin/openbox "$@" ;;
  *) exit 64 ;;
esac
