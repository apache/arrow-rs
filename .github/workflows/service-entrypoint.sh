#!/usr/bin/env sh
# Workaround lack of support within github actions for overriding service entrypoints
# https://github.com/orgs/community/discussions/26688

exec $ENTRYPOINT
