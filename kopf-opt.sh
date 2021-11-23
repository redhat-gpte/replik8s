#!/bin/sh
KOPF_OPTIONS="--log-format=json"

# Restrict watch to operator namespace.
KOPF_NAMESPACED=true

# Do not attempt to coordinate with other kopf operators.
KOPF_STANDALONE=true
