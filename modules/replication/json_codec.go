// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"encoding/json" //nolint:depguard // manifest digests must use the fixed encoding/json v1 codec, not Gitea's default handler
	"io"
)

// marshalManifestJSON serializes signed manifest values with the fixed v1 codec,
// so digests and signatures do not depend on Gitea's default JSON handler.
func marshalManifestJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}

// newManifestDecoder returns a streaming v1 decoder with exact io.EOF semantics.
func newManifestDecoder(reader io.Reader) *json.Decoder {
	return json.NewDecoder(reader)
}
