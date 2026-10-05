// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package replication

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	mathrand "math/rand/v2"
	"net"
	"net/http"
	"os"
	pathpkg "path"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gitea.dev/modules/json"
	"gitea.dev/modules/log"
	"gitea.dev/modules/setting"

	"golang.org/x/net/http2"
)

const (
	requestRetryLimit            = 5
	requestRetryBaseDelay        = 500 * time.Millisecond
	requestRetryMaxDelay         = 5 * time.Second
	syncBusyRetryJitterMax       = 500 * time.Millisecond
	chunkProgressLogStride       = 128
	chunkChangeWarnBurst         = 8
	manifestPollInterval         = time.Second
	chunkChangeStopStride        = 256
	finalChunkFetchWorkers       = 8
	finalChunkControlConnReserve = 1
	preflightChunkWorkers        = 4
	statusBodyPreviewLimit       = 4 * 1024
	maxEncodedStatusBodySize     = 64 * 1024
	maxJSONResponseSize          = 1024 * 1024
	maxRetryResponseDrain        = 64 * 1024
	maxRetryDrainDuration        = 250 * time.Millisecond
	responseBodyIdleTimeout      = 2 * time.Minute
	maxSessionHeartbeatTimeout   = 15 * time.Second
	maxSessionProgressInterval   = 10 * time.Second
	// Leave room for a timed-out request and a retry before the lease expires.
	sessionHeartbeatLeaseDivisor = 5
)

var syncBusyRetryDelay = time.Second

var (
	errRemoteSnapshotUnavailable  = errors.New("remote snapshot is unavailable")
	errRemoteManifestStateChanged = errors.New("remote manifest state changed between status and fetch")
)

type chunkSourceChangedError struct {
	hash   string
	detail string
}

func (e *chunkSourceChangedError) Error() string {
	if e.detail == "" {
		return fmt.Sprintf("chunk %s changed or became unavailable at the source", e.hash)
	}
	return fmt.Sprintf("chunk %s changed or became unavailable at the source: %s", e.hash, e.detail)
}

func retryDelay(attempt int) time.Duration {
	if attempt <= 1 {
		return requestRetryBaseDelay
	}
	delay := requestRetryBaseDelay << (attempt - 1)
	if delay > requestRetryMaxDelay {
		return requestRetryMaxDelay
	}
	return delay
}

func responseRetryDelay(resp *http.Response, fallback time.Duration) time.Duration {
	if resp == nil {
		return fallback
	}
	return retryAfterDelay(resp.Header.Get("Retry-After"), fallback)
}

func syncBusyRetryDelayForResponse(resp *http.Response) time.Duration {
	delay := responseRetryDelay(resp, syncBusyRetryDelay)
	if delay <= 0 || delay >= requestRetryMaxDelay {
		return delay
	}
	jitterLimit := min(syncBusyRetryJitterMax, requestRetryMaxDelay-delay)
	if jitterLimit <= 0 {
		return delay
	}
	return delay + time.Duration(mathrand.Int64N(int64(jitterLimit)+1))
}

func retryAfterDelay(value string, fallback time.Duration) time.Duration {
	retryAfter := strings.TrimSpace(value)
	if retryAfter == "" {
		return fallback
	}
	// Retry-After is server-directed backpressure; only local fallback delays are capped.
	allDigits := true
	for i := 0; i < len(retryAfter); i++ {
		if retryAfter[i] < '0' || retryAfter[i] > '9' {
			allDigits = false
			break
		}
	}
	if allDigits {
		retryAfter = strings.TrimLeft(retryAfter, "0")
		if retryAfter == "" {
			return fallback
		}
		seconds, err := strconv.ParseUint(retryAfter, 10, 64)
		if err != nil || seconds > uint64((1<<63-1)/int64(time.Second)) {
			return time.Duration(1<<63 - 1)
		}
		return max(fallback, time.Duration(seconds)*time.Second)
	}
	if retryAt, err := http.ParseTime(retryAfter); err == nil {
		return max(fallback, time.Until(retryAt))
	}
	return fallback
}

func shouldRetryHTTPStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusInternalServerError ||
		status == http.StatusTooManyRequests || status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}

type httpResponseStatusError struct {
	statusCode int
	retryAfter string
	errorCode  string
	message    string
}

func (e *httpResponseStatusError) Error() string { return e.message }

func isEndedFinalizeRequestError(err error) bool {
	var statusErr *httpResponseStatusError
	if !errors.As(err, &statusErr) || statusErr.statusCode != http.StatusConflict {
		return false
	}
	if statusErr.errorCode != "" {
		return statusErr.errorCode == errorCodeFinalizeRequestEnded
	}
	return strings.Contains(statusErr.message, "sync job request has already ended in state")
}

func isEndedPreflightRequestError(err error) bool {
	var statusErr *httpResponseStatusError
	if !errors.As(err, &statusErr) || statusErr.statusCode != http.StatusConflict {
		return false
	}
	if statusErr.errorCode != "" {
		return statusErr.errorCode == errorCodePreflightRequestEnded
	}
	return strings.Contains(statusErr.message, "preflight request has already ended in state")
}

func isUnavailablePreflightCheckpointError(err error) bool {
	var statusErr *httpResponseStatusError
	if !errors.As(err, &statusErr) || statusErr.statusCode != http.StatusConflict {
		return false
	}
	if statusErr.errorCode != "" {
		return statusErr.errorCode == errorCodePreflightCheckpoint
	}
	return strings.Contains(statusErr.message, "preflight recovery checkpoint is unavailable or invalid")
}

func isUnavailablePreflightBaseError(err error) bool {
	var statusErr *httpResponseStatusError
	if !errors.As(err, &statusErr) || statusErr.statusCode != http.StatusConflict {
		return false
	}
	if statusErr.errorCode != "" {
		return statusErr.errorCode == errorCodePreflightBase
	}
	return strings.Contains(statusErr.message, "preflight base is unavailable or invalid")
}

type cancelRequestBody struct {
	io.ReadCloser
	cancel       context.CancelFunc
	parent       context.Context
	idleTimer    *time.Timer
	idleTimedOut atomic.Bool
	timerMu      sync.Mutex
	closed       bool
}

func (b *cancelRequestBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.timerMu.Lock()
		if !b.closed {
			b.idleTimer.Reset(responseBodyIdleTimeout)
		}
		b.timerMu.Unlock()
	}
	if err != nil && b.idleTimedOut.Load() && b.parent.Err() == nil {
		return n, fmt.Errorf("replication response body idle timeout after %s: %v", responseBodyIdleTimeout, err)
	}
	return n, err
}

func (b *cancelRequestBody) Close() error {
	b.timerMu.Lock()
	b.closed = true
	b.timerMu.Unlock()
	err := b.ReadCloser.Close()
	b.idleTimer.Stop()
	b.cancel()
	return err
}

func keepRequestContextUntilBodyClose(parent context.Context, resp *http.Response, cancel context.CancelFunc) {
	if resp == nil || resp.Body == nil {
		cancel()
		return
	}
	body := &cancelRequestBody{ReadCloser: resp.Body, cancel: cancel, parent: parent}
	body.idleTimer = time.AfterFunc(responseBodyIdleTimeout, func() {
		body.idleTimedOut.Store(true)
		cancel()
	})
	resp.Body = body
}

func closeRetryResponse(resp *http.Response, cancel context.CancelFunc) {
	if resp == nil || resp.Body == nil {
		cancel()
		return
	}
	timer := time.AfterFunc(maxRetryDrainDuration, cancel)
	n, drainErr := io.Copy(io.Discard, io.LimitReader(resp.Body, maxRetryResponseDrain+1))
	drainTimedOut := !timer.Stop()
	if drainTimedOut || drainErr != nil || n > maxRetryResponseDrain {
		cancel()
	}
	_ = resp.Body.Close()
	cancel()
}

func shouldRetryRequestError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if streamErr, ok := errors.AsType[http2.StreamError](err); ok {
		switch streamErr.Code {
		case http2.ErrCodeInternal, http2.ErrCodeRefusedStream, http2.ErrCodeCancel, http2.ErrCodeEnhanceYourCalm:
			return true
		}
	}
	var goAwayErr http2.GoAwayError //nolint:staticcheck // x/net still returns this deprecated error for GOAWAY shutdowns.
	if errors.As(err, &goAwayErr) && goAwayErr.ErrCode == http2.ErrCodeNo {
		return true
	}
	if dnsErr, ok := errors.AsType[*net.DNSError](err); ok {
		if dnsErr.IsNotFound {
			return false
		}
		if dnsErr.IsTimeout || dnsErr.IsTemporary {
			return true
		}
	}
	var syscallErr syscall.Errno
	if errors.As(err, &syscallErr) && (syscallErr == syscall.EINTR || syscallErr == syscall.EMFILE || syscallErr == syscall.ENFILE) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unexpected eof") ||
		strings.Contains(message, "unexpected end of json") ||
		strings.Contains(message, "gzip:") ||
		(strings.Contains(message, "goaway") && strings.Contains(message, "errcode=no_error")) ||
		strings.Contains(message, "connection reset") ||
		strings.Contains(message, "broken pipe") ||
		strings.Contains(message, "refused") ||
		strings.Contains(message, "timeout") ||
		strings.Contains(message, "reset by peer")
}

func waitForRetry(ctx context.Context, attempt int, operation string, reason error) error {
	return waitForRetryDelay(ctx, attempt, operation, reason, retryDelay(attempt))
}

func waitForRetryDelay(ctx context.Context, attempt int, operation string, reason error, delay time.Duration) error {
	log.Warn("%s failed (%v); retrying in %s (%d/%d)", operation, reason, delay, attempt, requestRetryLimit)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func doRetryableJSONRequestWithAcceptEncoding(ctx context.Context, client *http.Client, method, url, token, operation string, payload []byte, acceptEncoding string) (*http.Response, error) {
	var lastErr error
	for attempt := 1; attempt <= requestRetryLimit; attempt++ {
		reqCtx, cancel := context.WithCancel(ctx)
		req, err := http.NewRequestWithContext(reqCtx, method, url, bytes.NewReader(payload))
		if err != nil {
			cancel()
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		if acceptEncoding != "" {
			req.Header.Set("Accept-Encoding", acceptEncoding)
		}
		resp, err := client.Do(req)
		if err == nil {
			if shouldRetryHTTPStatus(resp.StatusCode) && attempt < requestRetryLimit {
				delay := responseRetryDelay(resp, retryDelay(attempt))
				if delay > requestRetryMaxDelay {
					log.Warn("Deferring %s after server requested a long retry delay: status=%s retry_in=%s", operation, resp.Status, delay)
					keepRequestContextUntilBodyClose(ctx, resp, cancel)
					return resp, nil
				}
				retryErr := fmt.Errorf("%s returned %s", operation, resp.Status)
				closeRetryResponse(resp, cancel)
				if err := waitForRetryDelay(ctx, attempt, operation, retryErr, delay); err != nil {
					return nil, err
				}
				lastErr = retryErr
				continue
			}
			keepRequestContextUntilBodyClose(ctx, resp, cancel)
			return resp, nil
		}
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		cancel()
		if attempt == requestRetryLimit || !shouldRetryRequestError(err) {
			return nil, err
		}
		if err := waitForRetry(ctx, attempt, operation, err); err != nil {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func doRetryableRequest(ctx context.Context, client *http.Client, method, url, token, operation string) (*http.Response, error) {
	return doRetryableRequestWithAcceptEncoding(ctx, client, method, url, token, operation, "gzip")
}

func doRetryableRequestWithAcceptEncoding(ctx context.Context, client *http.Client, method, url, token, operation, acceptEncoding string) (*http.Response, error) {
	var lastErr error
	for attempt := 1; attempt <= requestRetryLimit; attempt++ {
		reqCtx, cancel := context.WithCancel(ctx)
		req, err := http.NewRequestWithContext(reqCtx, method, url, nil)
		if err != nil {
			cancel()
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		if method == http.MethodGet {
			req.Header.Set("Cache-Control", "no-cache")
		}
		if acceptEncoding != "" {
			req.Header.Set("Accept-Encoding", acceptEncoding)
		}
		resp, err := client.Do(req)
		if err != nil {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			cancel()
			if attempt == requestRetryLimit || !shouldRetryRequestError(err) {
				return nil, err
			}
			if retryErr := waitForRetry(ctx, attempt, operation, err); retryErr != nil {
				return nil, retryErr
			}
			lastErr = err
			continue
		}
		if shouldRetryHTTPStatus(resp.StatusCode) && attempt < requestRetryLimit {
			delay := responseRetryDelay(resp, retryDelay(attempt))
			if delay > requestRetryMaxDelay {
				log.Warn("Deferring %s after server requested a long retry delay: status=%s retry_in=%s", operation, resp.Status, delay)
				keepRequestContextUntilBodyClose(ctx, resp, cancel)
				return resp, nil
			}
			retryErr := fmt.Errorf("%s returned %s", operation, resp.Status)
			closeRetryResponse(resp, cancel)
			if err := waitForRetryDelay(ctx, attempt, operation, retryErr, delay); err != nil {
				return nil, err
			}
			lastErr = retryErr
			continue
		}
		keepRequestContextUntilBodyClose(ctx, resp, cancel)
		return resp, nil
	}
	if lastErr == nil {
		lastErr = errors.New("request retries exhausted")
	}
	return nil, lastErr
}

type boundedEncodedResponse struct {
	encoded    *io.LimitedReader
	reader     io.Reader
	gzipReader *gzip.Reader
	maxEncoded int64
	encoding   string
}

func newBoundedEncodedResponse(resp *http.Response, maxEncoded int64) (*boundedEncodedResponse, error) {
	if resp.ContentLength > maxEncoded {
		return nil, fmt.Errorf("encoded response exceeds maximum size of %d bytes", maxEncoded)
	}
	encoded := &io.LimitedReader{R: resp.Body, N: maxEncoded + 1}
	switch encoding := strings.TrimSpace(resp.Header.Get("Content-Encoding")); {
	case encoding == "" || strings.EqualFold(encoding, "identity"):
		return &boundedEncodedResponse{encoded: encoded, reader: encoded, maxEncoded: maxEncoded, encoding: encoding}, nil
	case strings.EqualFold(encoding, "gzip"):
		reader, err := gzip.NewReader(encoded)
		if err != nil {
			return nil, fmt.Errorf("decode gzip response: %w", err)
		}
		return &boundedEncodedResponse{encoded: encoded, reader: reader, gzipReader: reader, maxEncoded: maxEncoded, encoding: encoding}, nil
	default:
		return nil, fmt.Errorf("response uses unsupported content encoding %q", encoding)
	}
}

func (response *boundedEncodedResponse) encodedBytes() int64 {
	return response.maxEncoded + 1 - response.encoded.N
}

func (response *boundedEncodedResponse) exceedsLimit() bool {
	return response.encodedBytes() > response.maxEncoded
}

func (response *boundedEncodedResponse) Close() error {
	if response.gzipReader != nil {
		return response.gzipReader.Close()
	}
	return nil
}

func responseStatusError(prefix string, resp *http.Response) error {
	statusErr := &httpResponseStatusError{
		statusCode: resp.StatusCode,
		retryAfter: resp.Header.Get("Retry-After"),
		errorCode:  resp.Header.Get(replicationErrorCodeHeader),
	}
	response, err := newBoundedEncodedResponse(resp, maxEncodedStatusBodySize)
	if err != nil {
		statusErr.message = fmt.Sprintf("%s returned %s (read body: %v)", prefix, resp.Status, err)
		return statusErr
	}
	body, readErr := io.ReadAll(io.LimitReader(response.reader, statusBodyPreviewLimit+1))
	if response.exceedsLimit() {
		readErr = errors.New("encoded error response exceeds maximum size")
	}
	if closeErr := response.Close(); readErr == nil {
		readErr = closeErr
	}
	if readErr != nil {
		statusErr.message = fmt.Sprintf("%s returned %s (read body: %v)", prefix, resp.Status, readErr)
		return statusErr
	}
	message := strings.TrimSpace(string(body))
	if len(body) > statusBodyPreviewLimit {
		message += "..."
	}
	if message == "" {
		statusErr.message = fmt.Sprintf("%s returned %s", prefix, resp.Status)
		return statusErr
	}
	statusErr.message = fmt.Sprintf("%s returned %s: %s", prefix, resp.Status, message)
	return statusErr
}

func decodeBoundedJSONResponse(resp *http.Response, value any) error {
	response, err := newBoundedEncodedResponse(resp, maxJSONResponseSize+(64<<10))
	if err != nil {
		return err
	}
	data, decodeErr := io.ReadAll(io.LimitReader(response.reader, maxJSONResponseSize+1))
	if response.exceedsLimit() {
		decodeErr = errors.New("encoded JSON response exceeds maximum size")
	}
	if closeErr := response.Close(); decodeErr == nil {
		decodeErr = closeErr
	}
	if decodeErr != nil {
		return decodeErr
	}
	if int64(len(data)) > maxJSONResponseSize {
		return fmt.Errorf("JSON response exceeds maximum size of %d bytes", maxJSONResponseSize)
	}
	return json.Unmarshal(data, value)
}

func decodeBoundedManifestResponse(resp *http.Response) (SnapshotManifest, error) {
	response, err := newBoundedEncodedResponse(resp, maxEncodedManifestSize)
	if err != nil {
		return SnapshotManifest{}, err
	}
	manifest, decodeErr := decodeManifestResponseWithTrailerPolicy(response.reader, !strings.EqualFold(strings.TrimSpace(resp.Header.Get("Content-Encoding")), "gzip"))
	if response.exceedsLimit() {
		decodeErr = errors.New("encoded manifest response exceeds maximum size")
	}
	if closeErr := response.Close(); decodeErr == nil {
		decodeErr = closeErr
	}
	if decodeErr != nil {
		return SnapshotManifest{}, decodeErr
	}
	return manifest, nil
}

func decodeManifestResponse(body io.Reader) (SnapshotManifest, error) {
	return decodeManifestResponseWithTrailerPolicy(body, true)
}

func decodeManifestResponseWithTrailerPolicy(body io.Reader, allowUnexpectedEOF bool) (SnapshotManifest, error) {
	trackedBody := &manifestReadErrorAsEOF{reader: body}
	limited := &io.LimitedReader{R: trackedBody, N: int64(maxManifestSize) + 1}
	decoder := newManifestDecoder(limited)
	var manifest SnapshotManifest
	if decodeErr := decoder.Decode(&manifest); decodeErr != nil {
		if limited.N == 0 {
			return SnapshotManifest{}, errors.New("source manifest exceeds maximum size")
		}
		if trackedBody.readErr != nil {
			return SnapshotManifest{}, fmt.Errorf("read manifest: %w (decode: %v)", trackedBody.readErr, decodeErr)
		}
		return SnapshotManifest{}, decodeErr
	}
	trailingErr := consumeJSONDocumentTail(decoder.Buffered(), limited)
	if limited.N == 0 {
		return SnapshotManifest{}, errors.New("source manifest exceeds maximum size")
	}
	if trailingErr != nil {
		return SnapshotManifest{}, trailingErr
	}
	if trackedBody.readErr != nil {
		if !allowUnexpectedEOF || !errors.Is(trackedBody.readErr, io.ErrUnexpectedEOF) {
			return SnapshotManifest{}, fmt.Errorf("read manifest trailer: %w", trackedBody.readErr)
		}
	}
	// A complete signed JSON document remains usable if a proxy truncates only the
	// transport trailer after all document bytes have arrived.
	return manifest, nil
}

func consumeJSONDocumentTail(buffered, remainder io.Reader) error {
	reader := io.MultiReader(buffered, remainder)
	var buffer [4096]byte
	for {
		n, err := reader.Read(buffer[:])
		for _, b := range buffer[:n] {
			if b != ' ' && b != '\t' && b != '\r' && b != '\n' {
				return errors.New("manifest contains trailing data")
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("read manifest trailer: %w", err)
		}
	}
}

type manifestReadErrorAsEOF struct {
	reader  io.Reader
	readErr error
}

func (r *manifestReadErrorAsEOF) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil && err != io.EOF {
		r.readErr = err
		return n, io.EOF
	}
	return n, err
}

func requestManifest(ctx context.Context, client *http.Client, base, token, endpoint string) (*SnapshotManifest, error) {
	return requestManifestWithRequestID(ctx, client, base, token, endpoint, "", 0)
}

func newReplicationRequestID() (string, error) {
	var generated [16]byte
	if _, err := rand.Read(generated[:]); err != nil {
		return "", fmt.Errorf("generate sync job request ID: %w", err)
	}
	return hex.EncodeToString(generated[:]), nil
}

func requestManifestWithRequestID(ctx context.Context, client *http.Client, base, token, endpoint, requestID string, sessionTimeout time.Duration) (*SnapshotManifest, error) {
	return requestManifestWithRequestIDAndCallback(ctx, client, base, token, endpoint, requestID, sessionTimeout, nil)
}

func requestManifestWithRequestIDAndCallback(ctx context.Context, client *http.Client, base, token, endpoint, requestID string, sessionTimeout time.Duration, persistRequestID func(string) error) (*SnapshotManifest, error) {
	request := syncJobRequest{}
	if strings.Contains(endpoint, "preflight") {
		request.Kind = "preflight"
		if _, value, ok := strings.Cut(endpoint, "resume="); ok {
			request.ResumeJobID = value
		}
	} else {
		request.Kind = "final"
		if _, value, ok := strings.Cut(endpoint, "base="); ok {
			request.BaseJobID = value
		}
	}
	if requestID == "" {
		var err error
		requestID, err = newReplicationRequestID()
		if err != nil {
			return nil, err
		}
		if persistRequestID != nil {
			if err := persistRequestID(requestID); err != nil {
				return nil, fmt.Errorf("persist sync job request ID: %w", err)
			}
		}
	}
	if !validReplicationRequestID(requestID) {
		return nil, errors.New("invalid sync job request ID")
	}
	request.RequestID = requestID
	if request.Kind == "final" {
		log.Info("Submitting final replication sync job: request_id=%s preflight=%s", request.RequestID, request.BaseJobID)
	} else {
		log.Info("Submitting preflight replication sync job: request_id=%s resume=%s", request.RequestID, request.ResumeJobID)
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	operation := "create sync job " + request.Kind
	requestStarted := time.Now()
	busyWaits := 0
	busyStarted := time.Time{}
	preflightRequestRestarted := false
	for attempt := 1; ; attempt++ {
		resp, err := doRetryableJSONRequestWithAcceptEncoding(ctx, client, http.MethodPost, base+syncJobsPath, token, operation, payload, "gzip")
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusAccepted {
			var job Snapshot
			if err := decodeBoundedJSONResponse(resp, &job); err != nil {
				_ = resp.Body.Close()
				if attempt < requestRetryLimit && shouldRetryRequestError(err) {
					if retryErr := waitForRetry(ctx, attempt, operation, err); retryErr != nil {
						return nil, retryErr
					}
					continue
				}
				return nil, err
			}
			_ = resp.Body.Close()
			if !validSnapshotID(job.ID) {
				return nil, errors.New("sync job response contains an invalid snapshot ID")
			}
			log.Info("Replication sync job accepted: kind=%s request_id=%s snapshot=%s state=%s busy_waits=%d request_duration=%s", request.Kind, request.RequestID, job.ID, job.State, busyWaits, time.Since(requestStarted))
			return pollManifestTask(ctx, client, base, token, job.ID, endpoint, sessionTimeout)
		}
		if resp.StatusCode != http.StatusOK {
			statusErr := responseStatusError(endpoint, resp)
			_ = resp.Body.Close()
			if request.Kind == "preflight" && request.ResumeJobID == "" && !preflightRequestRestarted &&
				(isEndedPreflightRequestError(statusErr) || isUnavailablePreflightCheckpointError(statusErr)) {
				newRequestID, err := newReplicationRequestID()
				if err != nil {
					return nil, err
				}
				if persistRequestID != nil {
					if err := persistRequestID(newRequestID); err != nil {
						return nil, fmt.Errorf("persist replacement preflight request ID: %w", err)
					}
				}
				requestID = newRequestID
				request.RequestID = requestID
				payload, err = json.Marshal(request)
				if err != nil {
					return nil, err
				}
				preflightRequestRestarted = true
				log.Warn("Retrying preflight with a new request ID after the previous request became unrecoverable: request_id=%s error=%v", requestID, statusErr)
				attempt = 0
				continue
			}
			if resp.StatusCode == http.StatusConflict && strings.Contains(statusErr.Error(), "sync already in progress") {
				if busyWaits == 0 {
					busyStarted = time.Now()
				}
				busyWaits++
				if busyWaits == 1 || busyWaits%30 == 0 {
					log.Info("Primary replication is busy; waiting to create %s job: waits=%d elapsed=%s reason=%q", request.Kind, busyWaits, time.Since(busyStarted), statusErr)
				}
				// An active remote job cannot outlive its own timeout, so waiting
				// longer than the configured budget only stalls the restore worker.
				if sessionTimeout > 0 && time.Since(busyStarted) > sessionTimeout {
					log.Error("Giving up waiting for the primary replication to become idle: kind=%s waits=%d elapsed=%s timeout=%s reason=%q", request.Kind, busyWaits, time.Since(busyStarted), sessionTimeout, statusErr)
					return nil, fmt.Errorf("primary replication stayed busy longer than %s: %w", sessionTimeout, statusErr)
				}
				delay := syncBusyRetryDelayForResponse(resp)
				if delay > requestRetryMaxDelay {
					log.Warn("Deferring %s job request because primary requested a long retry delay: retry_in=%s reason=%v", request.Kind, delay, statusErr)
					return nil, statusErr
				}
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil, ctx.Err()
				case <-timer.C:
				}
				attempt = 0
				continue
			}
			return nil, statusErr
		}
		manifest, err := decodeBoundedManifestResponse(resp)
		if err != nil {
			_ = resp.Body.Close()
			if attempt < requestRetryLimit && shouldRetryRequestError(err) {
				if retryErr := waitForRetry(ctx, attempt, operation, err); retryErr != nil {
					return nil, retryErr
				}
				continue
			}
			return nil, err
		}
		if request.ResumeJobID != "" && manifest.ID != request.ResumeJobID {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("preflight recovery manifest ID mismatch: got %q want %q", manifest.ID, request.ResumeJobID)
		}
		if err := validateManifestIdentity(&manifest, token); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		expectedState := "transferring"
		if strings.Contains(endpoint, "preflight") {
			expectedState = "preflight"
		}
		if manifest.State != expectedState {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("%s returned manifest state %q", endpoint, manifest.State)
		}
		if err := validateIncrementalManifestContext(ctx, &manifest); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		if err := validateReplicaOAuth2ManifestConfig(&manifest); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		log.Info("Replication sync request returned manifest: kind=%s request_id=%s snapshot=%s state=%s busy_waits=%d request_duration=%s", request.Kind, request.RequestID, manifest.ID, manifest.State, busyWaits, time.Since(requestStarted))
		_ = resp.Body.Close()
		return &manifest, nil
	}
}

func requestSnapshotStatus(ctx context.Context, client *http.Client, base, token, id string) (*Snapshot, error) {
	operation := "poll snapshot " + id
	for attempt := 1; attempt <= requestRetryLimit; attempt++ {
		resp, err := doRetryableRequest(ctx, client, http.MethodGet, base+"/api/v1/replication/sync-jobs/"+id, token, operation)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			if resp.StatusCode == http.StatusNotFound {
				_ = resp.Body.Close()
				return nil, fmt.Errorf("%w: %s", errRemoteSnapshotUnavailable, id)
			}
			err := responseStatusError("snapshot "+id, resp)
			_ = resp.Body.Close()
			return nil, err
		}
		var snapshot Snapshot
		err = decodeBoundedJSONResponse(resp, &snapshot)
		_ = resp.Body.Close()
		if err == nil {
			if snapshot.ID != id {
				return nil, fmt.Errorf("snapshot status ID mismatch: got %q want %q", snapshot.ID, id)
			}
			return &snapshot, nil
		}
		if attempt == requestRetryLimit || !shouldRetryRequestError(err) {
			return nil, err
		}
		if retryErr := waitForRetry(ctx, attempt, operation, err); retryErr != nil {
			return nil, retryErr
		}
	}
	return nil, errors.New("snapshot status retries exhausted")
}

func requestManifestByID(ctx context.Context, client *http.Client, base, token, id, expectedState string) (*SnapshotManifest, error) {
	operation := "fetch manifest " + id
	for attempt := 1; attempt <= requestRetryLimit; attempt++ {
		resp, err := doRetryableRequestWithAcceptEncoding(ctx, client, http.MethodGet, base+"/api/v1/replication/sync-jobs/"+id+"/manifest", token, operation, "gzip")
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			err := responseStatusError("snapshot manifest "+id, resp)
			_ = resp.Body.Close()
			return nil, err
		}
		manifest, err := decodeBoundedManifestResponse(resp)
		if err != nil {
			_ = resp.Body.Close()
			if attempt < requestRetryLimit && shouldRetryRequestError(err) {
				if retryErr := waitForRetry(ctx, attempt, operation, err); retryErr != nil {
					return nil, retryErr
				}
				continue
			}
			return nil, err
		}
		if manifest.ID != id {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("snapshot manifest ID mismatch: got %q want %q", manifest.ID, id)
		}
		if err := validateManifestIdentity(&manifest, token); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		if manifest.State != expectedState {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("%w: snapshot=%s got=%q expected=%q", errRemoteManifestStateChanged, id, manifest.State, expectedState)
		}
		if err := validateIncrementalManifestContext(ctx, &manifest); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		if err := validateReplicaOAuth2ManifestConfig(&manifest); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		_ = resp.Body.Close()
		return &manifest, nil
	}
	return nil, errors.New("manifest fetch retries exhausted")
}

func validateReplicaOAuth2ManifestConfig(manifest *SnapshotManifest) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if !cfg.Enabled || cfg.Mode != modeReplica {
		return nil
	}
	if manifest.OAuth2SigningConfig != "" {
		localIdentity, err := oauth2SigningConfigIdentity()
		if err != nil {
			return err
		}
		if manifest.OAuth2SigningConfig != localIdentity {
			return fmt.Errorf("OAuth2 signing config mismatch: source %q standby %q", manifest.OAuth2SigningConfig, localIdentity)
		}
	}
	if !setting.OAuth2.Enabled || !usesFileBackedOAuth2SigningKey() {
		return nil
	}
	root, err := filepath.Abs(filepath.Clean(setting.AppWorkPath))
	if err != nil {
		return fmt.Errorf("resolve APP_WORK_PATH for OAuth2 signing key validation: %w", err)
	}
	keyPath, err := filepath.Abs(filepath.Clean(setting.OAuth2.JWTSigningPrivateKeyFile))
	if err != nil {
		return fmt.Errorf("resolve OAuth2 JWT signing key path %q: %w", setting.OAuth2.JWTSigningPrivateKeyFile, err)
	}
	relativePath, err := filepath.Rel(root, keyPath)
	if err != nil {
		return fmt.Errorf("resolve OAuth2 JWT signing key path %q relative to APP_WORK_PATH: %w", setting.OAuth2.JWTSigningPrivateKeyFile, err)
	}
	if relativePath == "." || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
		return fmt.Errorf("OAuth2 JWT signing key path %q is outside APP_WORK_PATH %q", setting.OAuth2.JWTSigningPrivateKeyFile, root)
	}
	manifestPath := filepath.ToSlash(relativePath)
	for _, entry := range manifest.Files {
		if entry.Path == manifestPath && entry.Type == "file" {
			return nil
		}
	}
	return fmt.Errorf("replication snapshot %s does not contain configured OAuth2 JWT signing key path %q", manifest.ID, manifestPath)
}

func pollManifestTask(ctx context.Context, client *http.Client, base, token, id, endpoint string, sessionTimeout time.Duration) (*SnapshotManifest, error) {
	expectedState := "transferring"
	phase := "finalize"
	if strings.Contains(endpoint, "preflight") {
		expectedState = "preflight"
		phase = "preflight"
	}
	pollInterval := manifestPollInterval
	if expectedState == "transferring" && sessionTimeout > 0 {
		pollInterval = min(pollInterval, max(sessionTimeout/sessionHeartbeatLeaseDivisor, time.Millisecond))
	}
	log.Info("Submitted %s task %s; polling snapshot status every %s", phase, id, pollInterval)
	pollStarted := time.Now()
	polls := 0
	pollFailures := 0
	var stopProgressHeartbeats func()
	defer func() {
		if stopProgressHeartbeats != nil {
			stopProgressHeartbeats()
		}
	}()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		snapshot, err := requestSnapshotStatus(ctx, client, base, token, id)
		if err != nil {
			pollFailures, err = waitForPollRetry(ctx, phase, id, pollStarted, pollFailures, err)
			if err != nil {
				log.Error("Failed to poll %s task %s after %s: %v", phase, id, time.Since(pollStarted), err)
				return nil, err
			}
			continue
		}
		polls++
		switch snapshot.State {
		case snapshotStateCreating:
			pollFailures = 0
			if polls%60 == 0 {
				log.Info("%s task %s is still creating: polls=%d elapsed=%s", phase, id, polls, time.Since(pollStarted))
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ticker.C:
			}
			continue
		case "failed":
			if snapshot.Error != "" {
				log.Error("%s task %s failed after %s: %s", phase, id, time.Since(pollStarted), snapshot.Error)
				return nil, fmt.Errorf("%s task %s failed: %s", phase, id, snapshot.Error)
			}
			log.Error("%s task %s failed after %s", phase, id, time.Since(pollStarted))
			return nil, fmt.Errorf("%s task %s failed", phase, id)
		case expectedState:
			if expectedState == "transferring" && stopProgressHeartbeats == nil {
				stopProgressHeartbeats = startFinalSessionProgressHeartbeats(ctx, client, base, token, id, sessionTimeout)
			}
			manifest, err := requestManifestByID(ctx, client, base, token, id, expectedState)
			if err != nil {
				pollFailures, err = waitForPollRetry(ctx, phase+" manifest", id, pollStarted, pollFailures, err)
				if err != nil {
					log.Error("Failed to fetch %s task %s manifest after %s: %v", phase, id, time.Since(pollStarted), err)
					return nil, err
				}
				continue
			}
			log.Info("%s task %s reached state %s after %s (%d polls); downloading manifest", phase, id, expectedState, time.Since(pollStarted), polls)
			return manifest, nil
		default:
			return nil, fmt.Errorf("%s task %s entered unexpected state %q", phase, id, snapshot.State)
		}
	}
}

func waitForPollRetry(ctx context.Context, phase, id string, pollStarted time.Time, failures int, err error) (int, error) {
	if ctx.Err() != nil {
		return failures, ctx.Err()
	}
	if errors.Is(err, errRemoteSnapshotUnavailable) || !retryablePollError(err) {
		return failures, err
	}
	failures++
	delay := retryDelay(min(failures, requestRetryLimit))
	if statusErr, ok := errors.AsType[*httpResponseStatusError](err); ok {
		delay = retryAfterDelay(statusErr.retryAfter, delay)
	}
	if delay > requestRetryMaxDelay {
		log.Warn("Deferring %s task polling because source requested a long retry delay: task=%s retry_in=%s elapsed=%s error=%v", phase, id, delay, time.Since(pollStarted), err)
		return failures, err
	}
	if failures <= 8 || failures%30 == 0 {
		log.Warn("Transient %s task request failed; retrying: task=%s failures=%d retry_in=%s elapsed=%s error=%v", phase, id, failures, delay, time.Since(pollStarted), err)
	} else {
		log.Debug("Transient %s task request still failing: task=%s failures=%d retry_in=%s elapsed=%s error=%v", phase, id, failures, delay, time.Since(pollStarted), err)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return failures, ctx.Err()
	case <-timer.C:
		return failures, nil
	}
}

func retryablePollError(err error) bool {
	if errors.Is(err, errRemoteManifestStateChanged) {
		return true
	}
	if statusErr, ok := errors.AsType[*httpResponseStatusError](err); ok {
		return shouldRetryHTTPStatus(statusErr.statusCode)
	}
	return shouldRetryRequestError(err)
}

func readBoundedChunkResponse(resp *http.Response) ([]byte, int64, error) {
	response, err := newBoundedEncodedResponse(resp, maxEncodedChunkSize)
	if err != nil {
		return nil, -1, err
	}
	data, decodeErr := io.ReadAll(io.LimitReader(response.reader, chunkMaxSize+1))
	encodedBytes := response.encodedBytes()
	if response.exceedsLimit() {
		decodeErr = errors.New("source chunk exceeds maximum encoded size")
	}
	if closeErr := response.Close(); decodeErr == nil {
		decodeErr = closeErr
	}
	if response.exceedsLimit() {
		return nil, -1, errors.New("source chunk exceeds maximum encoded size")
	}
	if decodeErr != nil {
		if strings.EqualFold(response.encoding, "gzip") {
			return nil, -1, fmt.Errorf("decode gzip source chunk: %w", decodeErr)
		}
		return nil, -1, fmt.Errorf("read identity source chunk: %w", decodeErr)
	}
	if len(data) > chunkMaxSize {
		return nil, -1, errors.New("source chunk exceeds maximum size")
	}
	return data, encodedBytes, nil
}

func requestChunk(ctx context.Context, client *http.Client, base, token, id, hash string) ([]byte, int64, error) {
	operation := fmt.Sprintf("request chunk: snapshot=%s hash=%s", id, hash)
	for attempt := 1; attempt <= requestRetryLimit; attempt++ {
		resp, err := doRetryableRequestWithAcceptEncoding(ctx, client, http.MethodGet, base+"/api/v1/replication/sync-jobs/"+id+"/chunks/"+hash, token, operation, "gzip")
		if err != nil {
			return nil, -1, err
		}
		if resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusNotFound {
			err := responseStatusError("source chunk", resp)
			_ = resp.Body.Close()
			return nil, -1, &chunkSourceChangedError{hash: hash, detail: err.Error()}
		}
		if resp.StatusCode != http.StatusOK {
			err := responseStatusError("chunk "+hash, resp)
			_ = resp.Body.Close()
			return nil, -1, err
		}
		data, encodedBodyBytes, err := readBoundedChunkResponse(resp)
		_ = resp.Body.Close()
		if err != nil {
			if attempt < requestRetryLimit && shouldRetryRequestError(err) {
				if retryErr := waitForRetry(ctx, attempt, operation, err); retryErr != nil {
					return nil, -1, retryErr
				}
				continue
			}
			return nil, -1, err
		}
		serverEncodedBodyBytes := parseEncodedBodyBytes(resp.Header.Get(replicationEncodedBodyBytesHeader))
		if serverEncodedBodyBytes >= 0 && serverEncodedBodyBytes != encodedBodyBytes {
			log.Debug("Replication chunk encoded body size differs from server report: snapshot=%s hash=%s received=%d server_reported=%d", id, hash, encodedBodyBytes, serverEncodedBodyBytes)
		}
		return data, encodedBodyBytes, nil
	}
	return nil, -1, errors.New("chunk request retries exhausted")
}

func fetchChunkWithIntegrityRetry(ctx context.Context, id, hash string, expectedSize int64, fetch func() ([]byte, int64, error), store func([]byte) error) ([]byte, int64, error) {
	operation := fmt.Sprintf("verify and cache chunk: snapshot=%s hash=%s", id, hash)
	for attempt := 1; attempt <= requestRetryLimit; attempt++ {
		data, encodedBytes, err := fetch()
		if err != nil {
			return nil, -1, err
		}
		if expectedSize <= 0 {
			return nil, -1, fmt.Errorf("chunk %s has size %d, expected %d", hash, len(data), expectedSize)
		}
		if int64(len(data)) != expectedSize {
			err = fmt.Errorf("%w: chunk %s has size %d, expected %d", errChunkSizeMismatch, hash, len(data), expectedSize)
		} else {
			err = store(data)
		}
		if (!errors.Is(err, errChunkHashMismatch) && !errors.Is(err, errChunkSizeMismatch)) || attempt == requestRetryLimit {
			return data, encodedBytes, err
		}
		if retryErr := waitForRetry(ctx, attempt, operation, err); retryErr != nil {
			return nil, -1, retryErr
		}
	}
	return nil, -1, errors.New("chunk cache retries exhausted")
}

func parseEncodedBodyBytes(value string) int64 {
	encodedBytes, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || encodedBytes < 0 || encodedBytes > maxEncodedChunkSize {
		return -1
	}
	return encodedBytes
}

func finishRemoteSession(ctx context.Context, client *http.Client, base, token, id, action string) error {
	return finishRemoteSessionAt(ctx, client, base, token, id, action, "")
}

func newSessionRenewalID() (string, error) {
	var generated [16]byte
	if _, err := rand.Read(generated[:]); err != nil {
		return "", fmt.Errorf("generate final sync session renewal ID: %w", err)
	}
	return hex.EncodeToString(generated[:]), nil
}

func heartbeatRemoteSession(ctx context.Context, client *http.Client, base, token, id string, requestTimeout time.Duration) (time.Duration, error) {
	renewalID, err := newSessionRenewalID()
	if err != nil {
		return 0, err
	}
	requestTimeout = min(requestTimeout, maxSessionHeartbeatTimeout)
	if requestTimeout <= 0 {
		requestTimeout = time.Millisecond
	}
	heartbeatCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	url := base + "/api/v1/replication/sync-jobs/" + id + "/session/heartbeat?renewal_id=" + renewalID
	resp, err := doRetryableRequest(heartbeatCtx, client, http.MethodPost, url, token, "heartbeat remote sync session")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, responseStatusError("heartbeat sync session", resp)
	}
	var result struct {
		LeaseNS int64 `json:"lease_ns"`
	}
	if err := decodeBoundedJSONResponse(resp, &result); err != nil {
		return 0, fmt.Errorf("read heartbeat sync session response: %w", err)
	}
	if result.LeaseNS <= 0 {
		return 0, errors.New("heartbeat response has no positive lease")
	}
	return time.Duration(result.LeaseNS), nil
}

func retryableHeartbeatError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	// Keep the lease heartbeat active after long server backoff responses.
	return errors.Is(err, context.DeadlineExceeded) || retryablePollError(err)
}

func isCompletedSessionHeartbeatError(err error) bool {
	var statusErr *httpResponseStatusError
	return errors.As(err, &statusErr) && statusErr.statusCode == http.StatusConflict &&
		strings.Contains(statusErr.message, "sync session is not active")
}

func finalSessionProgressHeartbeat(ctx context.Context, client *http.Client, base, token, id string) func(time.Duration) (time.Duration, bool) {
	var stopped atomic.Bool
	var heartbeatMu sync.Mutex
	var consecutiveFailures int
	var lastFailureLog time.Time
	return func(requestTimeout time.Duration) (time.Duration, bool) {
		if stopped.Load() {
			return 0, false
		}
		heartbeatMu.Lock()
		defer heartbeatMu.Unlock()
		if stopped.Load() {
			return 0, false
		}
		heartbeatStarted := time.Now()
		lease, err := heartbeatRemoteSession(ctx, client, base, token, id, requestTimeout)
		if err != nil {
			if ctx.Err() != nil {
				stopped.Store(true)
				return 0, false
			}
			if isCompletedSessionHeartbeatError(err) {
				stopped.Store(true)
				log.Debug("Final sync heartbeat stopped because the remote session is no longer active: snapshot=%s", id)
				return 0, false
			}
			retryable := retryableHeartbeatError(ctx, err)
			if !retryable {
				stopped.Store(true)
			}
			consecutiveFailures++
			if lastFailureLog.IsZero() || time.Since(lastFailureLog) >= time.Minute {
				log.Warn("Final sync session progress heartbeat failed: snapshot=%s failures=%d retryable=%t error=%v", id, consecutiveFailures, retryable, err)
				lastFailureLog = time.Now()
			}
			return 0, retryable
		}
		if consecutiveFailures > 0 {
			log.Info("Final sync session progress heartbeat recovered: snapshot=%s consecutive_failures=%d", id, consecutiveFailures)
			consecutiveFailures = 0
			lastFailureLog = time.Time{}
		}
		log.Debug("Final sync session progress heartbeat renewed: snapshot=%s lease=%s request_timeout=%s duration=%s", id, lease, requestTimeout, time.Since(heartbeatStarted))
		return lease, true
	}
}

func startFinalSessionProgressHeartbeats(ctx context.Context, client *http.Client, base, token, id string, timeout time.Duration) func() {
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	if timeout <= 0 {
		timeout = defaultConfig().FinalSessionTimeout
	}
	interval := min(timeout/sessionHeartbeatLeaseDivisor, maxSessionProgressInterval)
	if interval <= 0 {
		interval = time.Millisecond
	}
	heartbeat := finalSessionProgressHeartbeat(heartbeatCtx, client, base, token, id)
	done, stopped := make(chan struct{}), make(chan struct{})
	var stopOnce sync.Once
	go func() {
		defer close(stopped)
		defer cancelHeartbeat()
		for {
			lease, active := heartbeat(min(interval, maxSessionHeartbeatTimeout))
			if !active {
				return
			}
			if lease > 0 {
				interval = min(lease/sessionHeartbeatLeaseDivisor, maxSessionProgressInterval)
				if interval <= 0 {
					interval = time.Millisecond
				}
			}
			timer := time.NewTimer(interval)
			select {
			case <-done:
				timer.Stop()
				return
			case <-heartbeatCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	return func() {
		stopOnce.Do(func() {
			close(done)
			cancelHeartbeat()
		})
		<-stopped
	}
}

func finishRemoteSessionAt(ctx context.Context, client *http.Client, base, token, id, action, query string) error {
	resp, err := doRetryableRequest(ctx, client, http.MethodPost, base+"/api/v1/replication/sync-jobs/"+id+"/session/"+action+query, token, action+" remote sync session")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseStatusError(action+" sync session", resp)
	}
	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, statusBodyPreviewLimit+1)); err != nil {
		return fmt.Errorf("read %s sync session response: %w", action, err)
	}
	return nil
}

func previousManifest(path, token string) *SnapshotManifest {
	manifest, err := loadTrustedManifest(path, token, "ready")
	if err == nil {
		return manifest
	}
	if !errors.Is(err, errManifestTrailingData) {
		if !os.IsNotExist(err) {
			log.Warn("Ignoring persisted standby baseline %s: %v", path, err)
		}
	} else {
		manifest, recoveryErr := recoverTrustedBaseline(path, token)
		if recoveryErr == nil {
			if err := writeManifestAt(path, manifest); err != nil {
				log.Warn("Recovered persisted standby baseline %s but could not rewrite it: %v", path, err)
			} else {
				log.Warn("Recovered persisted standby baseline %s with trailing data; rewrote canonical manifest", path)
			}
			return manifest
		}
		log.Warn("Ignoring persisted standby baseline %s: %v", path, recoveryErr)
	}
	if manifest := latestTrustedReadyManifest(filepath.Dir(path), token); manifest != nil {
		log.Warn("Using retained standby baseline %s because current checkpoint %s is unavailable", manifest.ID, path)
		if err := writeManifestAt(path, manifest); err != nil {
			log.Warn("Could not restore standby current checkpoint %s from retained baseline %s: %v", path, manifest.ID, err)
		} else {
			log.Info("Restored standby current checkpoint %s from retained baseline %s", path, manifest.ID)
		}
		return manifest
	}
	return nil
}

func latestTrustedReadyManifest(snapshotDir, token string) *SnapshotManifest {
	paths, err := listManifestPaths(snapshotDir)
	if err != nil {
		log.Warn("Cannot list standby manifests for baseline recovery in %s: %v", snapshotDir, err)
		return nil
	}
	var latest *SnapshotManifest
	var candidates, invalid, mismatchedIDs int
	var firstInvalid string
	for _, path := range paths {
		name := filepath.Base(path)
		id := strings.TrimSuffix(name, ".json")
		isBaseline := name == baselineManifestName
		if !isBaseline && !validSnapshotID(id) {
			continue
		}
		candidates++
		manifest, err := loadTrustedManifest(path, token, "ready")
		if err != nil {
			invalid++
			if firstInvalid == "" {
				firstInvalid = fmt.Sprintf("%s: %v", name, err)
			}
			continue
		}
		if !isBaseline && manifest.ID != id {
			mismatchedIDs++
			if firstInvalid == "" {
				firstInvalid = fmt.Sprintf("%s: manifest ID %s does not match filename", name, manifest.ID)
			}
			continue
		}
		if newerManifest(manifest, latest) {
			latest = manifest
		}
	}
	if invalid+mismatchedIDs > 0 {
		log.Warn("Skipped invalid retained standby baseline candidates: directory=%s candidates=%d invalid=%d mismatched_ids=%d first_error=%s", snapshotDir, candidates, invalid, mismatchedIDs, firstInvalid)
	}
	return latest
}

// recoverTrustedBaseline accepts only the first JSON value in a local ready
// checkpoint that loadManifestFile rejected for trailing data. The checkpoint
// must still pass every normal structural and cryptographic verification, then
// is atomically rewritten by previousManifest before being reused.
func recoverTrustedBaseline(path, token string) (*SnapshotManifest, error) {
	file, err := openManifestFile(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var manifest SnapshotManifest
	if err := newManifestDecoder(io.LimitReader(file, int64(maxManifestSize)+1)).Decode(&manifest); err != nil {
		return nil, err
	}
	if err := validateIncrementalManifest(&manifest); err != nil {
		return nil, err
	}
	if manifest.State != "ready" {
		return nil, fmt.Errorf("manifest state is %q, expected %q", manifest.State, "ready")
	}
	if err := validateManifestIdentity(&manifest, token); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func cacheHas(cacheDir, hash string) bool {
	if len(hash) != sha256.Size*2 || !isLowerHex(hash) {
		return false
	}
	return verifyFile(cachePath(cacheDir, hash), hash) == nil
}

// Verify persisted cache candidates before they are trusted without being read
// again. The final pass verifies too: the primary may already be released, so a
// cache entry that fails the in-place update can no longer be refetched.
func cachedChunkAvailable(cacheDir, hash string, size int64) bool {
	if len(hash) != sha256.Size*2 || !isLowerHex(hash) {
		return false
	}
	path := cachePath(cacheDir, hash)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != size {
		return false
	}
	return verifyFile(path, hash) == nil
}

func pruneChunkCache(ctx context.Context, cacheDir, snapshotID string, manifests ...*SnapshotManifest) error {
	keep := make(map[string]int64)
	keptManifestCount := 0
	for _, manifest := range manifests {
		if manifest == nil {
			continue
		}
		keptManifestCount++
		for entryIndex, entry := range manifest.Files {
			if entryIndex&0xff == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			for _, chunk := range entry.Chunks {
				if err := ctx.Err(); err != nil {
					return err
				}
				if chunk.Zero {
					continue
				}
				keep[chunk.Hash] = chunk.Size
			}
		}
	}

	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		return err
	}
	pruneStarted := time.Now()
	nextProgressLog := pruneStarted.Add(30 * time.Second)
	var shardsProcessed, chunksProcessed int
	var removed, removedFileBytes int64
	var changedShards sync.Map
	logProgress := func() {
		if now := time.Now(); !now.Before(nextProgressLog) {
			log.Info("Replication chunk cache pruning progress: snapshot=%s retained_manifests=%d shards_processed=%d/%d chunks_processed=%d removed_entries=%d removed_file_bytes=%d retained_hashes=%d elapsed=%s", snapshotID, keptManifestCount, shardsProcessed, len(entries), chunksProcessed, removed, removedFileBytes, len(keep), now.Sub(pruneStarted))
			nextProgressLog = now.Add(30 * time.Second)
		}
	}
	for _, shard := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		shardPath := filepath.Join(cacheDir, shard.Name())
		if len(shard.Name()) != 2 || !isLowerHex(shard.Name()) {
			info, err := os.Lstat(shardPath)
			if err != nil {
				return err
			}
			if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
				if err := makeTreeRemovable(ctx, shardPath); err != nil {
					return fmt.Errorf("make unexpected cache entry removable: %w", err)
				}
			}
			if err := os.RemoveAll(shardPath); err != nil {
				return fmt.Errorf("remove unexpected cache entry %q: %w", shard.Name(), err)
			}
			removed++
			if info.Mode().IsRegular() {
				removedFileBytes += info.Size()
			}
			shardsProcessed++
			logProgress()
			continue
		}
		shardInfo, err := os.Lstat(shardPath)
		if err != nil {
			return err
		}
		if !shardInfo.IsDir() || shardInfo.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("replication chunk cache shard %q is not a real directory", shard.Name())
		}
		chunks, err := os.ReadDir(shardPath)
		if err != nil {
			return err
		}
		for _, chunk := range chunks {
			if err := ctx.Err(); err != nil {
				return err
			}
			chunksProcessed++
			name := chunk.Name()
			chunkPath := filepath.Join(shardPath, name)
			info, err := os.Lstat(chunkPath)
			if err != nil {
				return err
			}
			wantSize, needed := keep[name]
			if needed && len(name) == sha256.Size*2 && isLowerHex(name) && strings.HasPrefix(name, shard.Name()) && info.Mode().IsRegular() && info.Size() == wantSize {
				continue
			}
			if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
				if err := makeTreeRemovable(ctx, chunkPath); err != nil {
					return fmt.Errorf("make stale chunk cache entry removable: %w", err)
				}
			}
			if err := os.RemoveAll(chunkPath); err != nil {
				return fmt.Errorf("remove stale chunk cache entry %q: %w", name, err)
			}
			removed++
			if info.Mode().IsRegular() {
				removedFileBytes += info.Size()
			}
			changedShards.LoadOrStore(shard.Name(), struct{}{})
			if chunksProcessed&0x3ff == 0 {
				logProgress()
			}
		}
		shardsProcessed++
		logProgress()
	}
	if removed == 0 {
		log.Info("Replication chunk cache pruning completed: snapshot=%s retained_manifests=%d shards_processed=%d chunks_processed=%d removed_entries=0 retained_hashes=%d duration=%s", snapshotID, keptManifestCount, shardsProcessed, chunksProcessed, len(keep), time.Since(pruneStarted))
		return nil
	}
	hasChangedShard := false
	changedShards.Range(func(_, _ any) bool {
		hasChangedShard = true
		return false
	})
	if !hasChangedShard {
		if err := syncDirectory(cacheDir); err != nil {
			return fmt.Errorf("persist replication chunk cache pruning: %w", err)
		}
	} else if err := syncChunkCacheDirectories(cacheDir, snapshotID, "prune", &changedShards); err != nil {
		return fmt.Errorf("persist replication chunk cache pruning: %w", err)
	}
	log.Info("Pruned stale replication chunk cache: snapshot=%s retained_manifests=%d shards_processed=%d chunks_processed=%d removed_entries=%d removed_file_bytes=%d retained_hashes=%d duration=%s", snapshotID, keptManifestCount, shardsProcessed, chunksProcessed, removed, removedFileBytes, len(keep), time.Since(pruneStarted))
	return nil
}

func readCachedChunkForBatch(cacheDir, hash string, changedShards *sync.Map) ([]byte, error) {
	path := cachePath(cacheDir, hash)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > chunkMaxSize {
		return nil, errors.New("cached chunk is not a regular file within the size limit")
	}
	file, openedInfo, err := openRegularFile(path, info)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if openedInfo.Size() > chunkMaxSize {
		return nil, errors.New("cached chunk is not a regular file within the size limit")
	}
	data, err := io.ReadAll(io.LimitReader(file, chunkMaxSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > chunkMaxSize {
		return nil, errors.New("cached chunk exceeds maximum size")
	}
	sum := sha256.Sum256(data)
	if !matchesSHA256Hex(sum, hash) {
		actual := hex.EncodeToString(sum[:])
		if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
			log.Warn("Remove corrupted replication chunk cache entry failed: hash=%s error=%v", hash, removeErr)
		} else {
			changedShards.LoadOrStore(hash[:2], struct{}{})
			log.Warn("Discarded corrupted replication chunk cache entry: hash=%s", hash)
		}
		return nil, fmt.Errorf("cached chunk sha256 mismatch: got %s want %s", actual, hash)
	}
	return data, nil
}

func transferRateMiBPerSecond(size int64, duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}
	return float64(size) / duration.Seconds() / (1024 * 1024)
}

func logChunkEncodingSummary(phase, snapshotID string, payloadBytes, encodedBodyBytes, responses int64) {
	if responses == 0 || payloadBytes <= 0 {
		return
	}
	savedBytes := payloadBytes - encodedBodyBytes
	savedPercent := float64(savedBytes) * 100 / float64(payloadBytes)
	log.Info("%s chunk transfer encoding summary: snapshot=%s measured_responses=%d decoded_payload_bytes=%d encoded_body_bytes=%d saved_bytes=%d saved_percent=%.2f", phase, snapshotID, responses, payloadBytes, encodedBodyBytes, savedBytes, savedPercent)
}

type chunkPlanState uint8

const (
	chunkPlanNeeded chunkPlanState = iota + 1
	chunkPlanReusable
	chunkPlanSeen
	chunkPlanZero
)

type chunkPlanEntry struct {
	state chunkPlanState
	size  int64
}

type chunkFetchPlan struct {
	hashes                        []string
	sizes                         []int64
	total                         int
	reusable                      int
	reusableSize                  int64
	zeroChunks                    int
	zeroSize                      int64
	missingSize                   int64
	previousLocalFilesSkipped     int
	previousLocalFilesUnavailable int
	indexedLocalCandidates        int
	indexedLocalSize              int64
	invalidatedLocalCandidates    int
	localCandidates               map[string][]chunkLocation
	verifiedCacheShards           map[string]struct{}
	localReindexAttempts          int
	localReindexBytes             int64
	localNewCandidateChunks       int
	localNewCandidateSize         int64
}

func addLocalChunkCandidate(candidates map[string][]chunkLocation, hash string, location chunkLocation) {
	locations := candidates[hash]
	if slices.Contains(locations, location) {
		return
	}
	for _, existing := range locations {
		if existing.Path == location.Path && existing.SourceChangeID != location.SourceChangeID {
			refreshed := make([]chunkLocation, 0, maxChunkSourceAlternates+1)
			refreshed = append(refreshed, location)
			for _, candidate := range locations {
				if candidate.Path == location.Path || len(refreshed) == maxChunkSourceAlternates+1 {
					continue
				}
				refreshed = append(refreshed, candidate)
			}
			candidates[hash] = refreshed
			return
		}
	}
	if len(locations) < maxChunkSourceAlternates+1 {
		candidates[hash] = append(locations, location)
		return
	}
	if slices.ContainsFunc(locations, func(existing chunkLocation) bool { return existing.Path == location.Path }) {
		return
	}
	for i := len(locations) - 1; i > 0; i-- {
		if slices.ContainsFunc(locations[:i], func(existing chunkLocation) bool { return existing.Path == locations[i].Path }) {
			locations[i] = location
			candidates[hash] = locations
			return
		}
	}
}

func setChunkPlanState(states map[string]chunkPlanEntry, hash string, state chunkPlanState) {
	entry := states[hash]
	entry.state = state
	states[hash] = entry
}

func addChunkPlanEntry(states map[string]chunkPlanEntry, chunk ChunkDescriptor, zero bool) error {
	entry, exists := states[chunk.Hash]
	if exists && entry.size != chunk.Size {
		return fmt.Errorf("chunk %s has inconsistent sizes: got %d, previously %d", chunk.Hash, chunk.Size, entry.size)
	}
	if !exists {
		entry = chunkPlanEntry{state: chunkPlanNeeded, size: chunk.Size}
	}
	if zero {
		entry.state = chunkPlanZero
	}
	states[chunk.Hash] = entry
	return nil
}

func previousEntryLocalChangeID(previous *SnapshotManifest, entry TreeEntry) string {
	if entry.LocalChangeID != "" {
		return entry.LocalChangeID
	}
	if previous.State == "" {
		return entry.ChangeID
	}
	return ""
}

func previousEntryLocalChunkInfo(root, resolvedRoot string, previous *SnapshotManifest, entry TreeEntry) os.FileInfo {
	if validateTreePath(entry.Path) != nil {
		return nil
	}
	path := filepath.Join(root, filepath.FromSlash(entry.Path))
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil
	}
	changeID := previousEntryLocalChangeID(previous, entry)
	if changeID == "" || info.Size() != entry.Size || uint32(info.Mode().Perm()) != entry.Mode ||
		info.ModTime().UnixNano() != entry.ModTimeNS || fileChangeID(info) != changeID {
		return nil
	}
	pathResolved, err := filepath.EvalSymlinks(path)
	if err != nil || !isWithin(resolvedRoot, pathResolved) {
		return nil
	}
	return info
}

func previousEntryNeedsLocalReindex(root, resolvedRoot string, previous *SnapshotManifest, entry TreeEntry) bool {
	if err := validateTreePath(entry.Path); err != nil {
		return false
	}
	path := filepath.Join(root, filepath.FromSlash(entry.Path))
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	metadataChanged := info.Size() != entry.Size || uint32(info.Mode().Perm()) != entry.Mode || info.ModTime().UnixNano() != entry.ModTimeNS
	expectedChangeID := previousEntryLocalChangeID(previous, entry)
	needsReindex := metadataChanged || expectedChangeID == ""
	if !needsReindex {
		changeID := fileChangeID(info)
		needsReindex = changeID == "" || changeID != expectedChangeID
	}
	if !needsReindex {
		return false
	}
	pathResolved, err := filepath.EvalSymlinks(path)
	return err == nil && isWithin(resolvedRoot, pathResolved)
}

func previousEntryChunksLocally(ctx context.Context, root, resolvedRoot string, previous *SnapshotManifest, entry TreeEntry) ([]ChunkDescriptor, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if err := validateTreePath(entry.Path); err != nil {
		return nil, "", nil
	}
	path := filepath.Join(root, filepath.FromSlash(entry.Path))
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, "", nil
	}
	pathResolved, err := filepath.EvalSymlinks(path)
	if err != nil || !isWithin(resolvedRoot, pathResolved) {
		return nil, "", nil
	}
	expectedChangeID := previousEntryLocalChangeID(previous, entry)
	metadataUnchanged := info.Size() == entry.Size && uint32(info.Mode().Perm()) == entry.Mode && info.ModTime().UnixNano() == entry.ModTimeNS
	var beforeChangeID string
	if expectedChangeID != "" && metadataUnchanged {
		beforeChangeID = fileChangeID(info)
		if beforeChangeID == expectedChangeID {
			return entry.Chunks, beforeChangeID, nil
		}
	}

	chunks, err := splitFileForManifest(ctx, path, info)
	if err != nil {
		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}
		return nil, "", nil
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	after, err := os.Lstat(path)
	if err != nil {
		return nil, "", nil
	}
	if beforeChangeID == "" {
		beforeChangeID = fileChangeID(info)
	}
	afterChangeID := fileChangeID(after)
	pathResolved, err = filepath.EvalSymlinks(path)
	if err != nil || !isWithin(resolvedRoot, pathResolved) || !after.Mode().IsRegular() ||
		!os.SameFile(info, after) || info.Size() != after.Size() || info.Mode().Perm() != after.Mode().Perm() ||
		!info.ModTime().Equal(after.ModTime()) || (beforeChangeID != "" && beforeChangeID != afterChangeID) {
		return nil, "", nil
	}
	return chunks, afterChangeID, nil
}

func planMissingChunks(ctx context.Context, manifest, previous *SnapshotManifest, cacheDir string, preflight bool, localCandidates map[string][]chunkLocation) (chunkFetchPlan, error) {
	plan := chunkFetchPlan{}
	if err := ctx.Err(); err != nil {
		return plan, err
	}
	states := make(map[string]chunkPlanEntry)
	var localCandidateRoot, resolvedLocalCandidateRoot string
	var localCandidateRootErr error
	type localCandidateFile struct {
		info     os.FileInfo
		changeID string
	}
	localCandidateFiles := make(map[string]localCandidateFile)
	if !preflight && (previous != nil || len(localCandidates) > 0) {
		localCandidateRoot, localCandidateRootErr = filepath.Abs(filepath.Clean(setting.AppWorkPath))
		if localCandidateRootErr == nil {
			resolvedLocalCandidateRoot, localCandidateRootErr = resolvedPath(localCandidateRoot)
		}
	}
	// A missing file identity is safe here because the in-place update verifies candidate bytes by hash.
	localCandidateAvailable := func(location chunkLocation, size int64) bool {
		if localCandidateRootErr != nil || location.Size != size || location.Offset < 0 || location.Size <= 0 {
			return false
		}
		if err := validateTreePath(location.Path); err != nil {
			return false
		}
		candidate, checked := localCandidateFiles[location.Path]
		if !checked {
			path := filepath.Join(localCandidateRoot, filepath.FromSlash(location.Path))
			info, _ := os.Lstat(path)
			changeID := ""
			if info != nil && info.Mode().IsRegular() {
				changeID = fileChangeID(info)
				resolved, err := filepath.EvalSymlinks(path)
				if err != nil || !isWithin(resolvedLocalCandidateRoot, resolved) {
					info = nil
				}
			} else {
				info = nil
			}
			candidate = localCandidateFile{info: info, changeID: changeID}
			localCandidateFiles[location.Path] = candidate
		}
		info := candidate.info
		return info != nil && candidate.changeID == location.SourceChangeID &&
			location.Offset <= info.Size() && location.Size <= info.Size()-location.Offset
	}
	var wantedFiles map[string]bool
	if preflight {
		wantedFiles = make(map[string]bool)
		for entryIndex, entry := range manifest.Files {
			if entryIndex&0xff == 0 {
				if err := ctx.Err(); err != nil {
					return chunkFetchPlan{}, err
				}
			}
			hasNonZeroChunks := false
			for _, chunk := range entry.Chunks {
				if err := ctx.Err(); err != nil {
					return chunkFetchPlan{}, err
				}
				zero := chunk.Zero
				hasNonZeroChunks = hasNonZeroChunks || !zero
				if err := addChunkPlanEntry(states, chunk, zero); err != nil {
					return chunkFetchPlan{}, err
				}
			}
			if entry.Type == "file" && len(entry.Chunks) > 0 {
				wantedFiles[entry.Path] = hasNonZeroChunks
			}
		}
	} else {
		for entryIndex, entry := range manifest.Files {
			if entryIndex&0xff == 0 {
				if err := ctx.Err(); err != nil {
					return chunkFetchPlan{}, err
				}
			}
			for _, chunk := range entry.Chunks {
				if err := ctx.Err(); err != nil {
					return chunkFetchPlan{}, err
				}
				if err := addChunkPlanEntry(states, chunk, chunk.Zero); err != nil {
					return chunkFetchPlan{}, err
				}
			}
		}
	}
	if previous != nil {
		if preflight && len(states) > 0 {
			root := filepath.Clean(setting.AppWorkPath)
			resolvedRoot, err := resolvedPath(root)
			if err != nil {
				log.Debug("Cannot resolve standby data root while planning preflight chunks: %v", err)
			} else {
				for _, entry := range previous.Files {
					if err := ctx.Err(); err != nil {
						return chunkFetchPlan{}, err
					}
					if entry.Type != "file" {
						continue
					}
					needed := false
					reindexWithoutOverlap := false
					for chunkIndex, chunk := range entry.Chunks {
						if chunkIndex&0xff == 0 {
							if err := ctx.Err(); err != nil {
								return chunkFetchPlan{}, err
							}
						}
						if target, ok := states[chunk.Hash]; ok && target.state == chunkPlanNeeded {
							needed = true
							break
						}
					}
					if !needed {
						if hasNonZeroChunks, exists := wantedFiles[entry.Path]; !exists || !hasNonZeroChunks || !previousEntryNeedsLocalReindex(root, resolvedRoot, previous, entry) {
							continue
						}
						reindexWithoutOverlap = true
						plan.localReindexAttempts++
						plan.localReindexBytes += entry.Size
						log.Debug("Reindexing changed standby file for preflight chunk reuse: snapshot=%s path=%s manifest_bytes=%d", manifest.ID, entry.Path, entry.Size)
					}
					var reindexStarted time.Time
					if reindexWithoutOverlap {
						reindexStarted = time.Now()
					}
					localChunks, sourceChangeID, err := previousEntryChunksLocally(ctx, root, resolvedRoot, previous, entry)
					if err != nil {
						return chunkFetchPlan{}, err
					}
					if localChunks == nil {
						if reindexWithoutOverlap {
							log.Debug("Skipped changed standby file during preflight chunk reindex: snapshot=%s path=%s duration=%s", manifest.ID, entry.Path, time.Since(reindexStarted))
						}
						continue
					}
					if reindexWithoutOverlap {
						log.Debug("Reindexed changed standby file for preflight chunk reuse: snapshot=%s path=%s chunks=%d duration=%s", manifest.ID, entry.Path, len(localChunks), time.Since(reindexStarted))
					}
					localChunksMatchManifest := sameChunks(localChunks, entry.Chunks)
					localChangeIDChanged := sourceChangeID != previousEntryLocalChangeID(previous, entry)
					for chunkIndex, chunk := range localChunks {
						if chunkIndex&0xff == 0 {
							if err := ctx.Err(); err != nil {
								return chunkFetchPlan{}, err
							}
						}
						if target, ok := states[chunk.Hash]; ok && target.state != chunkPlanZero && target.size == chunk.Size {
							if !localChunksMatchManifest || localChangeIDChanged {
								if plan.localCandidates == nil {
									plan.localCandidates = make(map[string][]chunkLocation)
								}
								addLocalChunkCandidate(plan.localCandidates, chunk.Hash, chunkLocation{Path: entry.Path, Offset: chunk.Offset, Size: chunk.Size, SourceChangeID: sourceChangeID})
							}
							setChunkPlanState(states, chunk.Hash, chunkPlanReusable)
						}
					}
				}
			}
			candidateIndex := 0
			for _, locations := range plan.localCandidates {
				if candidateIndex&0xff == 0 {
					if err := ctx.Err(); err != nil {
						return chunkFetchPlan{}, err
					}
				}
				candidateIndex++
				if len(locations) > 0 {
					plan.localNewCandidateChunks++
					plan.localNewCandidateSize += locations[0].Size
				}
			}
		} else if !preflight {
			root := localCandidateRoot
			resolvedRoot := resolvedLocalCandidateRoot
			for _, entry := range previous.Files {
				if err := ctx.Err(); err != nil {
					return chunkFetchPlan{}, err
				}
				if entry.Type != "file" || len(entry.Chunks) == 0 {
					continue
				}
				needed := false
				for _, chunk := range entry.Chunks {
					if err := ctx.Err(); err != nil {
						return chunkFetchPlan{}, err
					}
					target, ok := states[chunk.Hash]
					if ok && target.state == chunkPlanNeeded && target.size == chunk.Size {
						needed = true
						break
					}
				}
				if !needed {
					plan.previousLocalFilesSkipped++
					continue
				}
				info := previousEntryLocalChunkInfo(root, resolvedRoot, previous, entry)
				if info == nil {
					plan.previousLocalFilesUnavailable++
					allChunksHaveCandidates := true
					for _, chunk := range entry.Chunks {
						if err := ctx.Err(); err != nil {
							return chunkFetchPlan{}, err
						}
						target, ok := states[chunk.Hash]
						if !ok || target.state != chunkPlanNeeded || target.size != chunk.Size {
							continue
						}
						found := false
						for _, location := range localCandidates[chunk.Hash] {
							if localCandidateAvailable(location, chunk.Size) {
								found = true
								break
							}
						}
						if !found {
							allChunksHaveCandidates = false
							break
						}
					}
					if !allChunksHaveCandidates && previousEntryNeedsLocalReindex(root, resolvedRoot, previous, entry) {
						plan.localReindexAttempts++
						plan.localReindexBytes += entry.Size
						reindexStarted := time.Now()
						log.Debug("Reindexing changed standby file while planning final chunks: snapshot=%s path=%s manifest_bytes=%d", manifest.ID, entry.Path, entry.Size)
						localChunks, sourceChangeID, err := previousEntryChunksLocally(ctx, root, resolvedRoot, previous, entry)
						if err != nil {
							return chunkFetchPlan{}, err
						}
						if localChunks == nil {
							log.Debug("Skipped changed standby file during final chunk reindex: snapshot=%s path=%s duration=%s", manifest.ID, entry.Path, time.Since(reindexStarted))
						} else {
							log.Debug("Reindexed changed standby file for final chunk reuse: snapshot=%s path=%s chunks=%d duration=%s", manifest.ID, entry.Path, len(localChunks), time.Since(reindexStarted))
							// Reindexing can observe a newer file version than candidate validation cached.
							delete(localCandidateFiles, entry.Path)
							for chunkIndex, chunk := range localChunks {
								if chunkIndex&0xff == 0 {
									if err := ctx.Err(); err != nil {
										return chunkFetchPlan{}, err
									}
								}
								target, needed := states[chunk.Hash]
								if !needed || target.state != chunkPlanNeeded || target.size != chunk.Size {
									continue
								}
								location := chunkLocation{Path: entry.Path, Offset: chunk.Offset, Size: chunk.Size, SourceChangeID: sourceChangeID}
								if localCandidates == nil {
									localCandidates = make(map[string][]chunkLocation)
								}
								addLocalChunkCandidate(localCandidates, chunk.Hash, location)
								if !slices.Contains(localCandidates[chunk.Hash], location) {
									continue
								}
								if sourceChangeID == "" {
									setChunkPlanState(states, chunk.Hash, chunkPlanReusable)
								}
							}
							plan.localCandidates = localCandidates
						}
					}
					continue
				}
				sourceChangeID := fileChangeID(info)
				for _, chunk := range entry.Chunks {
					if err := ctx.Err(); err != nil {
						return chunkFetchPlan{}, err
					}
					target, ok := states[chunk.Hash]
					if ok && target.state == chunkPlanNeeded && target.size == chunk.Size && chunk.Offset <= info.Size() && chunk.Size <= info.Size()-chunk.Offset {
						if plan.localCandidates == nil {
							plan.localCandidates = localCandidates
							if plan.localCandidates == nil {
								plan.localCandidates = make(map[string][]chunkLocation)
							}
						}
						localCandidates = plan.localCandidates
						locations := plan.localCandidates[chunk.Hash]
						available := locations[:0]
						for _, location := range locations {
							if localCandidateAvailable(location, chunk.Size) {
								available = append(available, location)
							} else {
								plan.invalidatedLocalCandidates++
							}
						}
						if len(available) == 0 {
							delete(plan.localCandidates, chunk.Hash)
						} else {
							plan.localCandidates[chunk.Hash] = available
						}
						addLocalChunkCandidate(plan.localCandidates, chunk.Hash, chunkLocation{
							Path: entry.Path, Offset: chunk.Offset, Size: chunk.Size, SourceChangeID: sourceChangeID,
						})
						plan.indexedLocalCandidates++
						plan.indexedLocalSize += chunk.Size
						setChunkPlanState(states, chunk.Hash, chunkPlanReusable)
					}
				}
			}
		}
	}
	if preflight {
		for hash, target := range states {
			if err := ctx.Err(); err != nil {
				return chunkFetchPlan{}, err
			}
			if target.state == chunkPlanNeeded && cachedChunkAvailable(cacheDir, hash, target.size) {
				setChunkPlanState(states, hash, chunkPlanReusable)
				if plan.verifiedCacheShards == nil {
					plan.verifiedCacheShards = make(map[string]struct{})
				}
				plan.verifiedCacheShards[hash[:2]] = struct{}{}
			}
		}
	}
	for entryIndex, entry := range manifest.Files {
		if entryIndex&0xff == 0 {
			if err := ctx.Err(); err != nil {
				return chunkFetchPlan{}, err
			}
		}
		for _, chunk := range entry.Chunks {
			if err := ctx.Err(); err != nil {
				return chunkFetchPlan{}, err
			}
			state := states[chunk.Hash].state
			if state == chunkPlanSeen {
				continue
			}
			if state == chunkPlanZero {
				setChunkPlanState(states, chunk.Hash, chunkPlanSeen)
				plan.total++
				plan.zeroChunks++
				plan.zeroSize += chunk.Size
				continue
			}
			if !preflight && state == chunkPlanNeeded {
				for _, location := range localCandidates[chunk.Hash] {
					if localCandidateAvailable(location, chunk.Size) {
						state = chunkPlanReusable
						plan.indexedLocalCandidates++
						plan.indexedLocalSize += chunk.Size
						break
					}
				}
			}
			setChunkPlanState(states, chunk.Hash, chunkPlanSeen)
			plan.total++
			if state == chunkPlanReusable || (!preflight && cachedChunkAvailable(cacheDir, chunk.Hash, chunk.Size)) {
				plan.reusable++
				plan.reusableSize += chunk.Size
				continue
			}
			plan.hashes = append(plan.hashes, chunk.Hash)
			plan.sizes = append(plan.sizes, chunk.Size)
			plan.missingSize += chunk.Size
		}
	}
	return plan, nil
}

// inPlaceApplyCapacity estimates the extra data-tree bytes an in-place apply needs: only
// entries that do not exist yet, or that must be rebuilt through a temporary file, grow the
// tree. Changed files are patched in place and therefore need no additional space.
func inPlaceApplyCapacity(manifest, previous *SnapshotManifest) (int64, int64, error) {
	if manifest == nil {
		return 0, 0, errors.New("in-place capacity estimate requires a manifest")
	}
	baseline := make(map[string]TreeEntry)
	if previous != nil {
		for _, entry := range previous.Files {
			baseline[entry.Path] = entry
		}
	}
	var bytesNeeded, inodesNeeded int64
	for _, entry := range manifest.Files {
		prior, existed := baseline[entry.Path]
		var needsEntry bool
		switch entry.Type {
		case "file":
			needsEntry = !existed || prior.Type != "file"
			if needsEntry {
				if entry.Size > math.MaxInt64-bytesNeeded {
					return 0, 0, errors.New("in-place apply capacity estimate overflows")
				}
				bytesNeeded += entry.Size
			}
		case "dir", "symlink":
			needsEntry = !existed || prior.Type != entry.Type
		default:
			return 0, 0, fmt.Errorf("unsupported capacity entry type %q", entry.Type)
		}
		if needsEntry {
			if inodesNeeded == math.MaxInt64 {
				return 0, 0, errors.New("in-place apply inode estimate overflows")
			}
			inodesNeeded++
		}
	}
	return bytesNeeded, inodesNeeded, nil
}

// checkApplyCapacityBeforeFinalize bounds the in-place update footprint from the preflight
// manifest while the primary is still online. Only entries that do not exist yet are
// counted, so the estimate is a lower bound; the final check still runs after release.
func checkApplyCapacityBeforeFinalize(preflight, previous *SnapshotManifest, cacheDir string) error {
	if preflight == nil {
		return nil
	}
	treeDir := setting.AppWorkPath
	if treeDir == "" {
		treeDir = filepath.Dir(cacheDir)
	}
	treeBytes, treeInodes, err := inPlaceApplyCapacity(preflight, previous)
	if err != nil {
		return fmt.Errorf("estimate in-place apply capacity before finalization: %w", err)
	}
	if err := checkRestoreCapacity(cacheDir, treeDir, treeBytes, 0, treeInodes, 0); err != nil {
		return fmt.Errorf("standby apply capacity is insufficient before finalization: %w", err)
	}
	return nil
}

// deferredChunkCacheLimit bounds how much transferred chunk data the standby keeps on disk
// before the in-place update runs. Chunks beyond the limit are fetched on demand while the
// update writes them, so the chunk cache and the data tree together stay near one data tree
// even when almost everything changes.
var deferredChunkCacheLimit int64 = 1 * 1024 * 1024 * 1024

func trimPlanToCacheLimit(plan *chunkFetchPlan) (deferredChunks int, deferredBytes int64) {
	if deferredChunkCacheLimit <= 0 || plan.missingSize <= deferredChunkCacheLimit {
		return 0, 0
	}
	var keptBytes int64
	kept := 0
	for i, size := range plan.sizes {
		if keptBytes+size > deferredChunkCacheLimit {
			break
		}
		keptBytes += size
		kept = i + 1
	}
	if kept >= len(plan.hashes) {
		return 0, 0
	}
	deferredChunks = len(plan.hashes) - kept
	for _, size := range plan.sizes[kept:] {
		deferredBytes += size
	}
	plan.hashes = plan.hashes[:kept]
	plan.sizes = plan.sizes[:kept]
	plan.missingSize -= deferredBytes
	return deferredChunks, deferredBytes
}

// fetchMissingChunks transfers the chunks the target manifest needs and returns the hashes
// it stored in the chunk cache, so callers can bound cache bookkeeping to that set.
func fetchMissingChunks(ctx context.Context, client *http.Client, base, token string, manifest, previous *SnapshotManifest, cacheDir string, preflight bool, localCandidates ...*map[string][]chunkLocation) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var knownLocalCandidates map[string][]chunkLocation
	if len(localCandidates) > 0 && localCandidates[0] != nil {
		knownLocalCandidates = *localCandidates[0]
	}
	passStarted := time.Now()
	planningStarted := time.Now()
	plan, err := planMissingChunks(ctx, manifest, previous, cacheDir, preflight, knownLocalCandidates)
	if err != nil {
		return nil, err
	}
	treeDir := setting.AppWorkPath
	if treeDir == "" {
		treeDir = filepath.Dir(cacheDir)
	}
	var treeBytes, treeInodes int64
	if preflight {
		treeBytes, treeInodes = 0, 0
	} else {
		treeBytes, treeInodes, err = inPlaceApplyCapacity(manifest, previous)
		if err != nil {
			return nil, fmt.Errorf("estimate in-place apply capacity: %w", err)
		}
	}
	if deferredChunks, deferredBytes := trimPlanToCacheLimit(&plan); deferredChunks > 0 {
		log.Info("Deferring transferred chunks to on-demand retrieval during the in-place update: snapshot=%s deferred_chunks=%d deferred_payload_bytes=%d cache_limit=%d", manifest.ID, deferredChunks, deferredBytes, deferredChunkCacheLimit)
	}
	if err := checkRestoreCapacity(cacheDir, treeDir, treeBytes, plan.missingSize, treeInodes, int64(len(plan.hashes))); err != nil {
		return nil, fmt.Errorf("preflight replication disk capacity: %w", err)
	}
	if preflight && plan.localReindexAttempts > 0 {
		log.Info("Preflight local chunk planning summary: snapshot=%s changed_file_reindex_attempts=%d manifest_bytes=%d local_candidate_chunks=%d local_candidate_payload_bytes=%d planning_duration=%s", manifest.ID, plan.localReindexAttempts, plan.localReindexBytes, plan.localNewCandidateChunks, plan.localNewCandidateSize, time.Since(planningStarted))
	}
	if len(localCandidates) > 0 && localCandidates[0] != nil && (preflight || plan.localCandidates != nil) {
		*localCandidates[0] = plan.localCandidates
	}
	if !preflight {
		log.Info("Final chunk preparation plan: snapshot=%s download_chunks=%d expected_payload_bytes=%d reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d previous_local_files_skipped=%d previous_local_files_unavailable=%d changed_file_reindex_attempts=%d changed_file_manifest_bytes=%d invalidated_local_candidates=%d indexed_local_candidates=%d indexed_local_payload_bytes=%d total_chunks=%d", manifest.ID, len(plan.hashes), plan.missingSize, plan.reusable, plan.reusableSize, plan.zeroChunks, plan.zeroSize, plan.previousLocalFilesSkipped, plan.previousLocalFilesUnavailable, plan.localReindexAttempts, plan.localReindexBytes, plan.invalidatedLocalCandidates, plan.indexedLocalCandidates, plan.indexedLocalSize, plan.total)
		if err := fetchChunksConcurrently(ctx, client, base, token, manifest.ID, plan.hashes, plan.sizes, cacheDir, plan.total, plan.reusable, plan.reusableSize, plan.zeroChunks, plan.zeroSize, plan.missingSize); err != nil {
			return nil, err
		}
		log.Info("Final chunk pass prepared for snapshot %s: download_chunks=%d reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d previous_local_files_skipped=%d previous_local_files_unavailable=%d changed_file_reindex_attempts=%d changed_file_manifest_bytes=%d invalidated_local_candidates=%d indexed_local_candidates=%d indexed_local_payload_bytes=%d total_chunks=%d expected_payload_bytes=%d duration=%s", manifest.ID, len(plan.hashes), plan.reusable, plan.reusableSize, plan.zeroChunks, plan.zeroSize, plan.previousLocalFilesSkipped, plan.previousLocalFilesUnavailable, plan.localReindexAttempts, plan.localReindexBytes, plan.invalidatedLocalCandidates, plan.indexedLocalCandidates, plan.indexedLocalSize, plan.total, plan.missingSize, time.Since(passStarted))
		return plan.hashes, nil
	}
	if err := fetchPreflightChunksConcurrently(ctx, client, base, token, manifest.ID, plan.hashes, plan.sizes, cacheDir, plan.verifiedCacheShards, plan.total, plan.reusable, plan.reusableSize, plan.zeroChunks, plan.zeroSize, plan.missingSize, time.Since(passStarted)); err != nil {
		return nil, err
	}
	return plan.hashes, nil
}

type chunkFetchRequest struct {
	hash string
	size int64
}

const (
	chunkBatchRequestLimit   = 32
	chunkBatchByteLimit      = 4 * 1024 * 1024
	chunkBatchMemberMaxBytes = 128 * 1024
)

// requestChunkBatch asks the primary for several small chunks at once. The caller
// stores and hash-verifies the payloads before trusting them.
func requestChunkBatch(ctx context.Context, client *http.Client, base, token, id string, hashes []string) (map[string][]byte, error) {
	operation := fmt.Sprintf("request chunk batch: snapshot=%s chunks=%d", id, len(hashes))
	payload, err := json.Marshal(chunkBatchRequest{Hashes: hashes})
	if err != nil {
		return nil, err
	}
	resp, err := doRetryableJSONRequestWithAcceptEncoding(ctx, client, http.MethodPost, base+"/api/v1/replication/sync-jobs/"+id+"/chunks", token, operation, payload, "identity")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, responseStatusError("chunk batch", resp)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBatchChunkResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read chunk batch response: %w", err)
	}
	if len(body) > maxBatchChunkResponseBytes {
		return nil, errors.New("chunk batch response exceeds maximum size")
	}
	return parseChunkBatchResponse(hashes, body)
}

func parseChunkBatchResponse(hashes []string, body []byte) (map[string][]byte, error) {
	payloads := make(map[string][]byte, len(hashes))
	for offset := 0; offset < len(body); {
		if offset+2 > len(body) {
			return nil, errors.New("truncated chunk batch record header")
		}
		status, index := body[offset], int(body[offset+1])
		offset += 2
		if status != chunkBatchStatusOK {
			continue
		}
		if offset+4 > len(body) {
			return nil, errors.New("truncated chunk batch record length")
		}
		length := int(binary.BigEndian.Uint32(body[offset : offset+4]))
		offset += 4
		if length < 0 || offset+length > len(body) {
			return nil, errors.New("truncated chunk batch record payload")
		}
		if index >= len(hashes) {
			return nil, errors.New("chunk batch record index out of range")
		}
		payloads[hashes[index]] = body[offset : offset+length]
		offset += length
	}
	return payloads, nil
}

// fetchChunkBatchIntoCache stores the chunks a batch response returned and reports
// their sizes. Entries the primary could not serve are left for the caller to fetch
// individually, and a whole-batch failure returns an error so the caller falls back.
func fetchChunkBatchIntoCache(ctx context.Context, client *http.Client, base, token, id, cacheDir string, batch []chunkFetchRequest, shards *sync.Map) (map[string]int, error) {
	if len(batch) < 2 {
		return map[string]int{}, nil
	}
	hashes := make([]string, len(batch))
	batchBytes := int64(0)
	for i, request := range batch {
		hashes[i] = request.hash
		batchBytes += request.size
	}
	payloads, err := requestChunkBatch(ctx, client, base, token, id, hashes)
	if err != nil {
		return nil, err
	}
	served := make(map[string]int, len(payloads))
	for hash, data := range payloads {
		if err := storeChunkForBatch(cacheDir, hash, data); err != nil {
			log.Debug("Batch chunk failed verification; fetching it individually: snapshot=%s hash=%s error=%v", id, hash, err)
			continue
		}
		shards.LoadOrStore(hash[:2], struct{}{})
		served[hash] = len(data)
	}
	log.Debug("Fetched replication chunk batch: snapshot=%s requested=%d served=%d bytes=%d", id, len(batch), len(served), batchBytes)
	return served, nil
}

// chunkFetchStats tracks transfer progress for the concurrent fetch loops.
type chunkFetchStats struct {
	chunksStarted           atomic.Int64
	fetched                 atomic.Int64
	fetchedBytes            atomic.Int64
	encodedPayloadBytes     atomic.Int64
	encodedBodyBytes        atomic.Int64
	encodedBodyMeasurements atomic.Int64
	lastProgress            atomic.Int64
}

// record accounts one fetched chunk and reports whether a progress log is due.
func (s *chunkFetchStats) record(payloadBytes int, encodedBytes int64) (int64, bool) {
	count := s.fetched.Add(1)
	s.fetchedBytes.Add(int64(payloadBytes))
	if encodedBytes >= 0 {
		s.encodedPayloadBytes.Add(int64(payloadBytes))
		s.encodedBodyBytes.Add(encodedBytes)
		s.encodedBodyMeasurements.Add(1)
	}
	now := time.Now()
	if last := s.lastProgress.Load(); last == 0 {
		return count, s.lastProgress.CompareAndSwap(0, now.UnixNano())
	} else if now.Sub(time.Unix(0, last)) >= 30*time.Second {
		return count, s.lastProgress.CompareAndSwap(last, now.UnixNano())
	}
	return count, false
}

func fetchChunksConcurrently(ctx context.Context, client *http.Client, base, token, id string, hashes []string, sizes []int64, cacheDir string, total, reusableCandidates int, reusableBytes int64, zeroChunks int, zeroBytes, totalBytes int64) error {
	if len(hashes) != len(sizes) {
		return errors.New("replication chunk fetch plan has inconsistent hash and size counts")
	}
	if len(hashes) == 0 {
		return ctx.Err()
	}
	started := time.Now()
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan []chunkFetchRequest)
	workerCount := min(finalChunkFetchWorkers, len(hashes))
	var workers sync.WaitGroup
	var firstErr error
	var firstHash string
	var firstErrOnce sync.Once
	var cacheShards sync.Map
	var stats chunkFetchStats
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for batch := range jobs {
				if workerCtx.Err() != nil {
					return
				}
				served, batchErr := fetchChunkBatchIntoCache(workerCtx, client, base, token, id, cacheDir, batch, &cacheShards)
				if batchErr != nil {
					log.Debug("Chunk batch request failed; falling back to individual chunks: snapshot=%s chunks=%d error=%v", id, len(batch), batchErr)
					served = nil
				}
				for _, request := range batch {
					if size, ok := served[request.hash]; ok {
						stats.chunksStarted.Add(1)
						if count, logNow := stats.record(size, -1); logNow {
							elapsed := time.Since(started)
							rate := transferRateMiBPerSecond(stats.fetchedBytes.Load(), elapsed)
							log.Info("Final chunk transfer progress: snapshot=%s chunks_started=%d fetched_chunks=%d download_chunks=%d total_chunks=%d payload_bytes=%d/%d server_encoded_body_bytes=%d measured_responses=%d/%d average_payload_mib_per_sec=%.2f reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d elapsed=%s", id, stats.chunksStarted.Load(), count, len(hashes), total, stats.fetchedBytes.Load(), totalBytes, stats.encodedBodyBytes.Load(), stats.encodedBodyMeasurements.Load(), count, rate, reusableCandidates, reusableBytes, zeroChunks, zeroBytes, elapsed)
						}
						continue
					}
					stats.chunksStarted.Add(1)
					data, encodedBytes, err := fetchChunkWithIntegrityRetry(workerCtx, id, request.hash, request.size,
						func() ([]byte, int64, error) { return requestChunk(workerCtx, client, base, token, id, request.hash) },
						func(data []byte) error { return storeChunkForBatch(cacheDir, request.hash, data) },
					)
					if err != nil {
						firstErrOnce.Do(func() {
							firstHash = request.hash
							firstErr = fmt.Errorf("fetch chunk %s: %w", request.hash, err)
							cancel()
						})
						return
					}
					cacheShards.LoadOrStore(request.hash[:2], struct{}{})
					if count, logNow := stats.record(len(data), encodedBytes); logNow {
						elapsed := time.Since(started)
						rate := transferRateMiBPerSecond(stats.fetchedBytes.Load(), elapsed)
						log.Info("Final chunk transfer progress: snapshot=%s chunks_started=%d fetched_chunks=%d download_chunks=%d total_chunks=%d payload_bytes=%d/%d server_encoded_body_bytes=%d measured_responses=%d/%d average_payload_mib_per_sec=%.2f reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d elapsed=%s", id, stats.chunksStarted.Load(), count, len(hashes), total, stats.fetchedBytes.Load(), totalBytes, stats.encodedBodyBytes.Load(), stats.encodedBodyMeasurements.Load(), count, rate, reusableCandidates, reusableBytes, zeroChunks, zeroBytes, elapsed)
					}
				}
			}
		}()
	}
sendJobs:
	for i := 0; i < len(hashes); {
		batch := make([]chunkFetchRequest, 0, chunkBatchRequestLimit)
		batchBytes := int64(0)
		for i < len(hashes) && len(batch) < chunkBatchRequestLimit {
			size := sizes[i]
			if size > chunkBatchMemberMaxBytes {
				if len(batch) == 0 {
					batch = append(batch, chunkFetchRequest{hash: hashes[i], size: size})
					i++
				}
				break
			}
			if len(batch) > 0 && batchBytes+size > chunkBatchByteLimit {
				break
			}
			batch = append(batch, chunkFetchRequest{hash: hashes[i], size: size})
			batchBytes += size
			i++
		}
		select {
		case <-workerCtx.Done():
			break sendJobs
		case jobs <- batch:
		}
	}
	close(jobs)
	workers.Wait()
	if err := syncChunkCacheDirectories(cacheDir, id, "final-fetch", &cacheShards); err != nil {
		log.Error("Persist final chunk cache failed: snapshot=%s error=%v", id, err)
		firstErr = errors.Join(firstErr, err)
	}
	logChunkEncodingSummary("Final", id, stats.encodedPayloadBytes.Load(), stats.encodedBodyBytes.Load(), stats.encodedBodyMeasurements.Load())
	elapsed := time.Since(started)
	bytes := stats.fetchedBytes.Load()
	rate := transferRateMiBPerSecond(bytes, elapsed)
	if err := ctx.Err(); err != nil && errors.Is(firstErr, err) {
		startedCount := stats.chunksStarted.Load()
		fetchedCount := stats.fetched.Load()
		log.Error("Final chunk transfer canceled: snapshot=%s hash=%s chunks_started=%d fetched=%d/%d incomplete=%d not_started=%d payload_bytes=%d/%d server_encoded_body_bytes=%d measured_responses=%d/%d average_payload_mib_per_sec=%.2f reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d elapsed=%s error=%v", id, firstHash, startedCount, fetchedCount, len(hashes), max(int64(0), startedCount-fetchedCount), max(int64(0), int64(len(hashes))-startedCount), bytes, totalBytes, stats.encodedBodyBytes.Load(), stats.encodedBodyMeasurements.Load(), fetchedCount, rate, reusableCandidates, reusableBytes, zeroChunks, zeroBytes, elapsed, firstErr)
		return firstErr
	}
	if firstErr != nil {
		startedCount := stats.chunksStarted.Load()
		fetchedCount := stats.fetched.Load()
		log.Error("Final chunk transfer failed: snapshot=%s hash=%s chunks_started=%d fetched=%d/%d incomplete=%d not_started=%d payload_bytes=%d/%d server_encoded_body_bytes=%d measured_responses=%d/%d average_payload_mib_per_sec=%.2f reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d elapsed=%s error=%v", id, firstHash, startedCount, fetchedCount, len(hashes), max(int64(0), startedCount-fetchedCount), max(int64(0), int64(len(hashes))-startedCount), bytes, totalBytes, stats.encodedBodyBytes.Load(), stats.encodedBodyMeasurements.Load(), fetchedCount, rate, reusableCandidates, reusableBytes, zeroChunks, zeroBytes, elapsed, firstErr)
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		startedCount := stats.chunksStarted.Load()
		fetchedCount := stats.fetched.Load()
		log.Error("Final chunk transfer canceled: snapshot=%s chunks_started=%d fetched=%d/%d incomplete=%d not_started=%d payload_bytes=%d/%d server_encoded_body_bytes=%d measured_responses=%d/%d average_payload_mib_per_sec=%.2f reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d elapsed=%s error=%v", id, startedCount, fetchedCount, len(hashes), max(int64(0), startedCount-fetchedCount), max(int64(0), int64(len(hashes))-startedCount), bytes, totalBytes, stats.encodedBodyBytes.Load(), stats.encodedBodyMeasurements.Load(), fetchedCount, rate, reusableCandidates, reusableBytes, zeroChunks, zeroBytes, elapsed, err)
		return err
	}
	log.Info("Final chunk transfer completed: snapshot=%s chunks_started=%d fetched=%d/%d payload_bytes=%d/%d server_encoded_body_bytes=%d measured_responses=%d/%d average_payload_mib_per_sec=%.2f reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d elapsed=%s", id, stats.chunksStarted.Load(), stats.fetched.Load(), len(hashes), bytes, totalBytes, stats.encodedBodyBytes.Load(), stats.encodedBodyMeasurements.Load(), stats.fetched.Load(), rate, reusableCandidates, reusableBytes, zeroChunks, zeroBytes, elapsed)
	return nil
}

func fetchPreflightChunksConcurrently(ctx context.Context, client *http.Client, base, token, id string, hashes []string, sizes []int64, cacheDir string, verifiedCacheShards map[string]struct{}, total, cached int, cachedBytes int64, zeroChunks int, zeroBytes, expectedBytes int64, preparationDuration time.Duration) error {
	if len(hashes) != len(sizes) {
		return errors.New("replication preflight fetch plan has inconsistent hash and size counts")
	}
	var cacheShards sync.Map
	for shard := range verifiedCacheShards {
		cacheShards.LoadOrStore(shard, struct{}{})
	}
	if len(hashes) == 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := syncChunkCacheDirectories(cacheDir, id, "preflight-fetch", &cacheShards); err != nil {
			return fmt.Errorf("persist verified preflight chunk cache: %w", err)
		}
		log.Info("Finished preflight chunk pass for snapshot %s: chunks_started=0 fetched=0/0 payload_bytes=0 expected_payload_bytes=%d server_encoded_body_bytes=0 measured_responses=0/0 reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d average_payload_mib_per_sec=%.2f deferred_changed=0 incomplete=0 not_started=0 total=%d duration=%s", id, expectedBytes, cached, cachedBytes, zeroChunks, zeroBytes, transferRateMiBPerSecond(0, preparationDuration), total, preparationDuration)
		return nil
	}
	started := time.Now()
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan []chunkFetchRequest)
	workerCount := min(preflightChunkWorkers, len(hashes))
	var workers sync.WaitGroup
	var firstErr error
	var firstHash string
	var firstErrOnce sync.Once
	var stats chunkFetchStats
	var deferred atomic.Int64
	var stopAfterChurn atomic.Bool
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for batch := range jobs {
				if stopAfterChurn.Load() {
					continue
				}
				if workerCtx.Err() != nil {
					return
				}
				served, batchErr := fetchChunkBatchIntoCache(workerCtx, client, base, token, id, cacheDir, batch, &cacheShards)
				if batchErr != nil {
					log.Debug("Preflight chunk batch request failed; falling back to individual chunks: snapshot=%s chunks=%d error=%v", id, len(batch), batchErr)
					served = nil
				}
				for _, request := range batch {
					if size, ok := served[request.hash]; ok {
						stats.chunksStarted.Add(1)
						if count, logNow := stats.record(size, -1); logNow {
							elapsed := time.Since(started)
							rate := transferRateMiBPerSecond(stats.fetchedBytes.Load(), elapsed)
							log.Info("Preflight chunk transfer progress: snapshot=%s chunks_started=%d fetched_chunks=%d/%d total_chunks=%d payload_bytes=%d expected_payload_bytes=%d server_encoded_body_bytes=%d measured_responses=%d/%d average_payload_mib_per_sec=%.2f reusable_candidates=%d zero_chunks=%d zero_payload_bytes=%d deferred_changed=%d elapsed=%s", id, stats.chunksStarted.Load(), count, len(hashes), total, stats.fetchedBytes.Load(), expectedBytes, stats.encodedBodyBytes.Load(), stats.encodedBodyMeasurements.Load(), count, rate, cached, zeroChunks, zeroBytes, deferred.Load(), elapsed)
						}
						continue
					}
					stats.chunksStarted.Add(1)
					data, encodedBytes, err := fetchChunkWithIntegrityRetry(workerCtx, id, request.hash, request.size,
						func() ([]byte, int64, error) { return requestChunk(workerCtx, client, base, token, id, request.hash) },
						func(data []byte) error { return storeChunkForBatch(cacheDir, request.hash, data) },
					)
					if err != nil {
						if stopAfterChurn.Load() && errors.Is(err, context.Canceled) {
							return
						}
						if _, ok := errors.AsType[*chunkSourceChangedError](err); ok {
							count := deferred.Add(1)
							if count <= chunkChangeWarnBurst {
								log.Warn("Preflight chunk changed; snapshot=%s hash=%s deferred_to=final_sync", id, request.hash)
							} else if count%chunkProgressLogStride == 0 {
								processed := int64(cached) + stats.fetched.Load() + count
								log.Warn("Preflight progress for snapshot %s remains unstable: processed=%d/%d fetched=%d cached=%d deferred=%d", id, processed, total, stats.fetched.Load(), cached, count)
							}
							fetchedCount := stats.fetched.Load()
							if count >= chunkChangeStopStride && count > fetchedCount+int64(workerCount) && stopAfterChurn.CompareAndSwap(false, true) {
								log.Warn("Preflight prefetch for snapshot %s stopped scheduling after changed chunks exceeded successful fetches: changed=%d fetched=%d", id, count, fetchedCount)
								cancel()
							}
							continue
						}
						firstErrOnce.Do(func() {
							firstErr, firstHash = err, request.hash
							cancel()
						})
						return
					}
					cacheShards.LoadOrStore(request.hash[:2], struct{}{})
					if count, logNow := stats.record(len(data), encodedBytes); logNow {
						elapsed := time.Since(started)
						rate := transferRateMiBPerSecond(stats.fetchedBytes.Load(), elapsed)
						log.Info("Preflight chunk transfer progress: snapshot=%s chunks_started=%d fetched_chunks=%d/%d total_chunks=%d payload_bytes=%d expected_payload_bytes=%d server_encoded_body_bytes=%d measured_responses=%d/%d average_payload_mib_per_sec=%.2f reusable_candidates=%d zero_chunks=%d zero_payload_bytes=%d deferred_changed=%d elapsed=%s", id, stats.chunksStarted.Load(), count, len(hashes), total, stats.fetchedBytes.Load(), expectedBytes, stats.encodedBodyBytes.Load(), stats.encodedBodyMeasurements.Load(), count, rate, cached, zeroChunks, zeroBytes, deferred.Load(), elapsed)
					}
				}
			}
		}()
	}
sendJobs:
	for i := 0; i < len(hashes); {
		if stopAfterChurn.Load() {
			break
		}
		batch := make([]chunkFetchRequest, 0, chunkBatchRequestLimit)
		batchBytes := int64(0)
		for i < len(hashes) && len(batch) < chunkBatchRequestLimit {
			size := sizes[i]
			if size > chunkBatchMemberMaxBytes {
				if len(batch) == 0 {
					batch = append(batch, chunkFetchRequest{hash: hashes[i], size: size})
					i++
				}
				break
			}
			if len(batch) > 0 && batchBytes+size > chunkBatchByteLimit {
				break
			}
			batch = append(batch, chunkFetchRequest{hash: hashes[i], size: size})
			batchBytes += size
			i++
		}
		select {
		case <-workerCtx.Done():
			break sendJobs
		case jobs <- batch:
		}
	}
	close(jobs)
	workers.Wait()
	if err := syncChunkCacheDirectories(cacheDir, id, "preflight-fetch", &cacheShards); err != nil {
		log.Error("Persist preflight chunk cache failed: snapshot=%s error=%v", id, err)
		firstErr = errors.Join(firstErr, err)
	}
	logChunkEncodingSummary("Preflight", id, stats.encodedPayloadBytes.Load(), stats.encodedBodyBytes.Load(), stats.encodedBodyMeasurements.Load())
	elapsed := preparationDuration + time.Since(started)
	startedCount := stats.chunksStarted.Load()
	fetchedCount, bytes, deferredCount := stats.fetched.Load(), stats.fetchedBytes.Load(), deferred.Load()
	incompleteCount := max(int64(0), startedCount-fetchedCount-deferredCount)
	notStartedCount := max(int64(0), int64(len(hashes))-startedCount)
	if err := ctx.Err(); err != nil && errors.Is(firstErr, err) {
		log.Error("Preflight chunk transfer canceled: snapshot=%s hash=%s chunks_started=%d fetched=%d/%d payload_bytes=%d expected_payload_bytes=%d server_encoded_body_bytes=%d measured_responses=%d/%d reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d average_payload_mib_per_sec=%.2f deferred_changed=%d incomplete=%d not_started=%d total=%d duration=%s error=%v", id, firstHash, startedCount, fetchedCount, len(hashes), bytes, expectedBytes, stats.encodedBodyBytes.Load(), stats.encodedBodyMeasurements.Load(), fetchedCount, cached, cachedBytes, zeroChunks, zeroBytes, transferRateMiBPerSecond(bytes, elapsed), deferredCount, incompleteCount, notStartedCount, total, elapsed, firstErr)
		return firstErr
	}
	if firstErr != nil {
		log.Error("Preflight chunk transfer failed: snapshot=%s hash=%s chunks_started=%d fetched=%d/%d payload_bytes=%d expected_payload_bytes=%d server_encoded_body_bytes=%d measured_responses=%d/%d reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d average_payload_mib_per_sec=%.2f deferred_changed=%d incomplete=%d not_started=%d total=%d duration=%s error=%v", id, firstHash, startedCount, fetchedCount, len(hashes), bytes, expectedBytes, stats.encodedBodyBytes.Load(), stats.encodedBodyMeasurements.Load(), fetchedCount, cached, cachedBytes, zeroChunks, zeroBytes, transferRateMiBPerSecond(bytes, elapsed), deferredCount, incompleteCount, notStartedCount, total, elapsed, firstErr)
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		log.Error("Preflight chunk transfer canceled: snapshot=%s chunks_started=%d fetched=%d/%d payload_bytes=%d expected_payload_bytes=%d server_encoded_body_bytes=%d measured_responses=%d/%d reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d average_payload_mib_per_sec=%.2f deferred_changed=%d incomplete=%d not_started=%d total=%d duration=%s error=%v", id, startedCount, fetchedCount, len(hashes), bytes, expectedBytes, stats.encodedBodyBytes.Load(), stats.encodedBodyMeasurements.Load(), fetchedCount, cached, cachedBytes, zeroChunks, zeroBytes, transferRateMiBPerSecond(bytes, elapsed), deferredCount, incompleteCount, notStartedCount, total, elapsed, err)
		return err
	}
	if fetchedCount == 0 && deferredCount > 0 {
		log.Warn("Finished preflight chunk pass for snapshot %s without caching any chunks; source changed before every fetch (cached=%d deferred=%d total=%d elapsed=%s)", id, cached, deferredCount, total, elapsed)
	}
	log.Info("Finished preflight chunk pass for snapshot %s: chunks_started=%d fetched=%d/%d payload_bytes=%d expected_payload_bytes=%d server_encoded_body_bytes=%d measured_responses=%d/%d reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d average_payload_mib_per_sec=%.2f deferred_changed=%d incomplete=%d not_started=%d total=%d duration=%s", id, startedCount, fetchedCount, len(hashes), bytes, expectedBytes, stats.encodedBodyBytes.Load(), stats.encodedBodyMeasurements.Load(), fetchedCount, cached, cachedBytes, zeroChunks, zeroBytes, transferRateMiBPerSecond(bytes, elapsed), deferredCount, incompleteCount, notStartedCount, total, elapsed)
	return nil
}

func sameManifestFileData(a, b TreeEntry) bool {
	return a.Type == "file" && b.Type == "file" && a.Size == b.Size && sameChunks(a.Chunks, b.Chunks)
}

func isZeroChunk(data []byte) bool {
	for len(data) >= 8 {
		if binary.LittleEndian.Uint64(data[:8]) != 0 {
			return false
		}
		data = data[8:]
	}
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

func setLocalChangeID(entry *TreeEntry, info os.FileInfo) bool {
	entry.LocalChangeID = ""
	if entry.Type != "file" || info == nil || !info.Mode().IsRegular() || info.Size() != entry.Size ||
		uint32(info.Mode().Perm()) != entry.Mode || info.ModTime().UnixNano() != entry.ModTimeNS {
		return false
	}
	entry.LocalChangeID = fileChangeID(info)
	return entry.LocalChangeID != ""
}

func recordLocalChangeIDs(ctx context.Context, root string, manifest *SnapshotManifest) error {
	filesTotal := 0
	for _, entry := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Type == "file" {
			filesTotal++
		}
	}
	var filesProcessed, recorded, skipped atomic.Int64
	var firstSkip error
	started := time.Now()
	stopProgress := startPeriodicProgressLog(func() {
		log.Info("Recording standby file identities progress: snapshot=%s files_processed=%d/%d recorded=%d skipped=%d elapsed=%s", manifest.ID, filesProcessed.Load(), filesTotal, recorded.Load(), skipped.Load(), time.Since(started))
	})
	defer stopProgress()
	for i := range manifest.Files {
		if err := ctx.Err(); err != nil {
			log.Warn("Canceled recording standby file identities: snapshot=%s files_processed=%d/%d recorded=%d skipped=%d elapsed=%s error=%v", manifest.ID, filesProcessed.Load(), filesTotal, recorded.Load(), skipped.Load(), time.Since(started), err)
			return err
		}
		entry := &manifest.Files[i]
		if entry.Type != "file" {
			entry.LocalChangeID = ""
			continue
		}
		verifiedChangeID := entry.LocalChangeID
		entry.LocalChangeID = ""
		info, err := os.Lstat(filepath.Join(root, filepath.FromSlash(entry.Path)))
		if err == nil && setLocalChangeID(entry, info) {
			if verifiedChangeID == "" || entry.LocalChangeID == verifiedChangeID {
				recorded.Add(1)
				filesProcessed.Add(1)
				continue
			}
			entry.LocalChangeID = ""
			err = errors.New("local file identity changed after content verification")
		} else if err == nil {
			if info.Mode().IsRegular() && info.Size() == entry.Size &&
				uint32(info.Mode().Perm()) == entry.Mode && info.ModTime().UnixNano() == entry.ModTimeNS {
				err = errors.New("filesystem does not provide a stable file identity")
			} else {
				err = errors.New("local file metadata does not match the restored manifest")
			}
		}
		skipped.Add(1)
		filesProcessed.Add(1)
		if firstSkip == nil {
			firstSkip = fmt.Errorf("%s: %w", entry.Path, err)
		}
	}
	stopProgress()
	if skipped.Load() > 0 {
		log.Warn("Could not record local file identities for some files: snapshot=%s recorded=%d skipped=%d first_error=%v duration=%s", manifest.ID, recorded.Load(), skipped.Load(), firstSkip, time.Since(started))
	} else {
		log.Info("Recorded local file identities: snapshot=%s files=%d duration=%s", manifest.ID, recorded.Load(), time.Since(started))
	}
	return nil
}

func reusableWholeFileInfo(path string, entry TreeEntry) (os.FileInfo, bool) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != entry.Size ||
		uint32(info.Mode().Perm()) != entry.Mode || info.ModTime().UnixNano() != entry.ModTimeNS {
		return nil, false
	}
	return info, true
}

func fileMatchesManifestChunksWithInfo(ctx context.Context, path string, entry TreeEntry) (bool, os.FileInfo, error) {
	before, reusable := reusableWholeFileInfo(path, entry)
	if !reusable {
		return false, nil, nil
	}
	file, openedInfo, err := openRegularFile(path, before)
	if err != nil {
		return false, nil, err
	}
	defer file.Close()
	beforeChangeID := fileChangeID(openedInfo)
	chunks, err := splitFileFromOpen(ctx, file, openedInfo, path)
	if err != nil {
		return false, nil, err
	}
	after, err := file.Stat()
	if err != nil {
		return false, nil, err
	}
	pathAfter, err := os.Lstat(path)
	if err != nil {
		return false, nil, err
	}
	if !after.Mode().IsRegular() || !pathAfter.Mode().IsRegular() || !os.SameFile(openedInfo, after) || !os.SameFile(openedInfo, pathAfter) ||
		after.Size() != entry.Size || pathAfter.Size() != entry.Size ||
		uint32(after.Mode().Perm()) != entry.Mode || uint32(pathAfter.Mode().Perm()) != entry.Mode ||
		after.ModTime().UnixNano() != entry.ModTimeNS || pathAfter.ModTime().UnixNano() != entry.ModTimeNS ||
		(beforeChangeID != "" && (beforeChangeID != fileChangeID(after) || beforeChangeID != fileChangeID(pathAfter))) {
		return false, nil, errIncrementalTreeChanged
	}
	return sameChunks(chunks, entry.Chunks), pathAfter, nil
}

func makeTreeRemovable(ctx context.Context, root string) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if info.IsDir() {
			return os.Chmod(path, 0o700)
		}
		return nil
	})
}

func applyPathError(operation, rel string, err error) error {
	return fmt.Errorf("%s %q: %w", operation, rel, err)
}

const chunkMemoryCacheLimit = 16 * 1024 * 1024

type chunkMemoryCacheEntry struct {
	hash string
	data []byte
}

type chunkMemoryCache struct {
	maxBytes int64
	used     int64
	entries  map[string]*list.Element
	lru      list.List
}

func newStageChunkMemoryCache(maxBytes int64) *chunkMemoryCache {
	return &chunkMemoryCache{maxBytes: maxBytes, entries: make(map[string]*list.Element)}
}

func (c *chunkMemoryCache) get(hash string, size int64) []byte {
	element := c.entries[hash]
	if element == nil {
		return nil
	}
	entry := element.Value.(*chunkMemoryCacheEntry) //nolint:forcetypeassert // The cache inserts only this entry type.
	if int64(len(entry.data)) != size {
		c.used -= int64(len(entry.data))
		delete(c.entries, hash)
		c.lru.Remove(element)
		return nil
	}
	c.lru.MoveToFront(element)
	return entry.data
}

func (c *chunkMemoryCache) add(hash string, data []byte) {
	if c.maxBytes <= 0 || int64(len(data)) > c.maxBytes {
		return
	}
	if element := c.entries[hash]; element != nil {
		entry := element.Value.(*chunkMemoryCacheEntry) //nolint:forcetypeassert // The cache inserts only this entry type.
		c.used -= int64(len(entry.data))
		c.lru.Remove(element)
		delete(c.entries, hash)
	}
	for c.used+int64(len(data)) > c.maxBytes {
		element := c.lru.Back()
		if element == nil {
			break
		}
		entry := element.Value.(*chunkMemoryCacheEntry) //nolint:forcetypeassert // The cache inserts only this entry type.
		c.used -= int64(len(entry.data))
		delete(c.entries, entry.hash)
		c.lru.Remove(element)
	}
	entry := &chunkMemoryCacheEntry{hash: hash, data: data}
	c.entries[hash] = c.lru.PushFront(entry)
	c.used += int64(len(data))
}

func persistStandbyManifest(snapshotDir string, manifest *SnapshotManifest) error {
	return persistStandbyManifestContext(context.Background(), snapshotDir, manifest)
}

func persistStandbyManifestContext(ctx context.Context, snapshotDir string, manifest *SnapshotManifest) error {
	return writeManifestAtContext(ctx, manifestPath(snapshotDir, manifest.ID), manifest)
}

func matchesCompletedFinalManifest(final, ready *SnapshotManifest) bool {
	if final == nil || ready == nil || final.ID != ready.ID || final.State != "transferring" || ready.State != "ready" {
		return false
	}
	transfer := *ready
	transfer.Snapshot.State = "transferring"
	transfer.Snapshot.Error = ""
	digest, err := manifestDigestWithoutLocalChangeIDs(&transfer)
	return err == nil && digest == final.SHA256
}

func replicationManifestEntryIgnored(entry TreeEntry, excluded, localOnly map[string]string) bool {
	if _, ok := localOnly[entry.Path]; ok {
		return true
	}
	if _, ok := configuredReplicationLogEntry(entry.Path, localOnly); ok {
		return true
	}
	for excludedPath := range excluded {
		if entry.Path == excludedPath {
			return entry.Type != "symlink"
		}
		if strings.HasPrefix(entry.Path, excludedPath+"/") {
			return true
		}
	}
	return false
}

func sameManifestTreeEntries(a, b []TreeEntry, root string) bool {
	excluded := replicationTreeExclusions(filepath.Clean(root))
	localOnly := replicationLocalOnlyFiles(filepath.Clean(root))
	i, j := 0, 0
	for {
		if i < len(a) && j < len(b) && a[i].Path == b[j].Path {
			ignoreA := replicationManifestEntryIgnored(a[i], excluded, localOnly)
			ignoreB := replicationManifestEntryIgnored(b[j], excluded, localOnly)
			if ignoreA || ignoreB {
				if !ignoreA || !ignoreB {
					return false
				}
				i++
				j++
				continue
			}
			entry, other := a[i], b[j]
			if entry.Type != other.Type || entry.Mode != other.Mode || entry.Size != other.Size ||
				entry.ModTimeNS != other.ModTimeNS || entry.LinkTarget != other.LinkTarget || !sameChunks(entry.Chunks, other.Chunks) {
				return false
			}
			i++
			j++
			continue
		}
		if i < len(a) && replicationManifestEntryIgnored(a[i], excluded, localOnly) {
			i++
			continue
		}
		if j < len(b) && replicationManifestEntryIgnored(b[j], excluded, localOnly) {
			j++
			continue
		}
		return i == len(a) && j == len(b)
	}
}

func sameManifestContentTree(a, b *SnapshotManifest, root string) bool {
	return a != nil && b != nil && a.FormatVersion == incrementalFormatVersion && b.FormatVersion == incrementalFormatVersion &&
		a.GiteaVersion == b.GiteaVersion && a.AppWorkPath == b.AppWorkPath &&
		a.InstanceFingerprint == b.InstanceFingerprint && a.OAuth2SigningConfig == b.OAuth2SigningConfig &&
		a.GeneralTokenSecretFingerprint == b.GeneralTokenSecretFingerprint && a.RootMode == b.RootMode &&
		sameManifestTreeEntries(a.Files, b.Files, root)
}

func installedTreeMatchesManifest(ctx context.Context, root string, manifest *SnapshotManifest) (bool, error) {
	if manifest == nil {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	root = filepath.Clean(root)
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return false, err
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 || uint32(rootInfo.Mode().Perm()) != manifest.RootMode {
		return false, nil
	}
	expected := make(map[string]*TreeEntry, len(manifest.Files))
	configuredRoot := filepath.Clean(setting.AppWorkPath)
	excluded := replicationTreeExclusions(configuredRoot)
	localOnly := replicationLocalOnlyFiles(configuredRoot)
	for i := range manifest.Files {
		entry := &manifest.Files[i]
		if !replicationManifestEntryIgnored(*entry, excluded, localOnly) {
			expected[filepath.FromSlash(entry.Path)] = entry
		}
	}
	logDirectoryAncestors := make(map[string]struct{})
	for file, kind := range localOnly {
		if kind != "node-local log file" {
			continue
		}
		for directory := pathpkg.Dir(file); directory != "."; directory = pathpkg.Dir(directory) {
			logDirectoryAncestors[directory] = struct{}{}
		}
	}
	extraLogDirectories := make(map[string]struct{})
	insideExtraLogDirectory := func(rel string) bool {
		for directory := pathpkg.Dir(rel); directory != "."; directory = pathpkg.Dir(directory) {
			if _, ok := extraLogDirectories[directory]; ok {
				return true
			}
		}
		return false
	}
	matched := true
	err = filepath.Walk(root, func(filePath string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, filePath)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if err := validateTreePath(rel); err != nil {
			return err
		}
		_, ignored := localOnly[rel]
		if !ignored {
			_, ignored = configuredReplicationLogEntry(rel, localOnly)
		}
		if ignored {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if _, regenerable := excluded[rel]; regenerable && info.Mode()&os.ModeSymlink == 0 {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.Mode().Type() != 0 && !info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			// Sockets, FIFOs and device nodes are node-local runtime artifacts and are
			// never replicated, so they must not invalidate an otherwise matching tree.
			log.Warn("Ignoring unsupported filesystem entry while verifying the standby tree: path=%q mode=%s", rel, info.Mode().Type())
			return nil
		}
		if insideExtraLogDirectory(rel) {
			if info.IsDir() {
				return nil
			}
			// Only node-local log files may live in a directory created for logs.
			matched = false
			return nil
		}
		entry, ok := expected[filepath.FromSlash(rel)]
		if !ok {
			if info.IsDir() {
				if _, isLogDirectory := logDirectoryAncestors[rel]; isLogDirectory {
					extraLogDirectories[rel] = struct{}{}
					return nil
				}
				matched = false
				return filepath.SkipDir
			}
			matched = false
			return nil
		}
		delete(expected, filepath.FromSlash(rel))
		mode := uint32(info.Mode().Perm())
		if mode != entry.Mode {
			matched = false
			return nil
		}
		switch {
		case info.IsDir():
			if entry.Type != "dir" || (info.ModTime().UnixNano() != entry.ModTimeNS && !replicationHasLocalOnlyFileBelow(rel, localOnly)) {
				matched = false
			}
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(filePath)
			if err != nil {
				return err
			}
			if entry.Type != "symlink" || target != entry.LinkTarget {
				matched = false
			}
		case info.Mode().IsRegular():
			if entry.Type != "file" || info.Size() != entry.Size || info.ModTime().UnixNano() != entry.ModTimeNS {
				matched = false
				return nil
			}
			if entry.LocalChangeID != "" && fileChangeID(info) == entry.LocalChangeID {
				return nil
			}
			contentMatches, verifiedInfo, err := fileMatchesManifestChunksWithInfo(ctx, filePath, *entry)
			if err != nil {
				return err
			}
			if !contentMatches {
				matched = false
				return nil
			}
			entry.LocalChangeID = fileChangeID(verifiedInfo)
		default:
			return fmt.Errorf("unsupported filesystem entry %q", rel)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return matched && len(expected) == 0, nil
}

func removeNodeLocalLogDirectories(local, expected *SnapshotManifest, localOnlyFiles map[string]string) int {
	logDirectoryAncestors := make(map[string]struct{})
	for file, kind := range localOnlyFiles {
		if kind != "node-local log file" {
			continue
		}
		for directory := pathpkg.Dir(file); directory != "."; directory = pathpkg.Dir(directory) {
			logDirectoryAncestors[directory] = struct{}{}
		}
	}
	expectedLogDirectories := make(map[string]struct{})
	for _, entry := range expected.Files {
		if _, isLogDirectory := logDirectoryAncestors[entry.Path]; isLogDirectory {
			expectedLogDirectories[entry.Path] = struct{}{}
		}
	}
	extraLogDirectories := make(map[string]struct{})
	for _, entry := range local.Files {
		if entry.Type != "dir" {
			continue
		}
		if _, isLogDirectory := logDirectoryAncestors[entry.Path]; !isLogDirectory {
			continue
		}
		if _, expectedPath := expectedLogDirectories[entry.Path]; !expectedPath {
			extraLogDirectories[entry.Path] = struct{}{}
		}
	}
	if len(extraLogDirectories) == 0 {
		return 0
	}
	for _, entry := range local.Files {
		insideExtraDirectory := false
		for directory := entry.Path; directory != "."; directory = pathpkg.Dir(directory) {
			if _, ok := extraLogDirectories[directory]; ok {
				insideExtraDirectory = true
				break
			}
		}
		if !insideExtraDirectory {
			continue
		}
		if entry.Type != "dir" {
			return 0
		}
		if _, isLogDirectory := logDirectoryAncestors[entry.Path]; !isLogDirectory {
			return 0
		}
	}
	filtered := local.Files[:0]
	for _, entry := range local.Files {
		if _, remove := extraLogDirectories[entry.Path]; remove {
			continue
		}
		filtered = append(filtered, entry)
	}
	removed := len(local.Files) - len(filtered)
	local.Files = filtered
	local.FileCount = len(filtered)
	return removed
}

func scanInstalledTreeAgainstManifest(ctx context.Context, root string, base, manifest *SnapshotManifest) (*SnapshotManifest, error) {
	local, err := scanIncrementalTreeWithoutDigestUsingLocalChangeIDs(ctx, root, base, manifest.ID)
	if err != nil {
		return nil, err
	}
	localOnlyFiles := replicationLocalOnlyFiles(root)
	if removed := removeNodeLocalLogDirectories(local, manifest, localOnlyFiles); removed > 0 {
		log.Debug("Ignored standby directories created only for node-local log files: snapshot=%s entries=%d", manifest.ID, removed)
	}
	if local.RootMode != manifest.RootMode || local.Size != manifest.Size || local.FileCount != manifest.FileCount || len(local.Files) != len(manifest.Files) {
		return local, fmt.Errorf("root or tree totals differ: root_mode=%o/%o size=%d/%d file_count=%d/%d entries=%d/%d",
			local.RootMode, manifest.RootMode, local.Size, manifest.Size, local.FileCount, manifest.FileCount, len(local.Files), len(manifest.Files))
	}
	for i, expected := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		actual := local.Files[i]
		mtimeChanged := actual.ModTimeNS != expected.ModTimeNS
		if mtimeChanged && actual.Type == "dir" && replicationHasLocalOnlyFileBelow(actual.Path, localOnlyFiles) {
			log.Debug("Ignored node-local activity timestamp while verifying standby directory: snapshot=%s path=%s", manifest.ID, actual.Path)
			mtimeChanged = false
		}
		if actual.Path != expected.Path || actual.Type != expected.Type || actual.Mode != expected.Mode ||
			actual.Size != expected.Size || mtimeChanged || actual.LinkTarget != expected.LinkTarget ||
			!sameChunks(actual.Chunks, expected.Chunks) {
			return local, fmt.Errorf("manifest entry differs: expected_path=%q actual_path=%q type=%t mode=%t size=%t mtime=%t link_target=%t chunks=%t",
				expected.Path, actual.Path, actual.Type != expected.Type, actual.Mode != expected.Mode,
				actual.Size != expected.Size, actual.ModTimeNS != expected.ModTimeNS,
				actual.LinkTarget != expected.LinkTarget, !sameChunks(actual.Chunks, expected.Chunks))
		}
	}
	return local, nil
}

// verifyInstalledStandbyContent rehashes every installed file against the manifest,
// ignoring the local identities recorded at the previous verification.
func verifyInstalledStandbyContent(ctx context.Context, root string, manifest *SnapshotManifest) error {
	content := *manifest
	content.Files = slices.Clone(manifest.Files)
	for i := range content.Files {
		content.Files[i].LocalChangeID = ""
	}
	_, err := scanInstalledTreeAgainstManifest(ctx, root, &content, manifest)
	return err
}

// verifyRestoredStandbyTree verifies the installed tree against manifest and records the
// standby's own file identities. When base is the trusted manifest for the tree state before
// this update, unchanged files are recognised through the identities it recorded and only
// new or changed content is rehashed. A due full scan or an untrusted base rehashes every
// file, which is also what scanInstalledTreeAgainstManifest does when the base carries no
// local identities.
func verifyRestoredStandbyTree(ctx context.Context, root string, manifest, base *SnapshotManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fullScan := base == nil || base.State != "ready" ||
		(!manifest.FullScanAt.IsZero() && (base.FullScanAt.IsZero() || manifest.FullScanAt.After(base.FullScanAt)))
	scanBase := base
	if fullScan {
		scanBase = &SnapshotManifest{Files: manifest.Files, FullScanAt: manifest.FullScanAt}
	}
	local, err := scanInstalledTreeAgainstManifest(ctx, root, scanBase, manifest)
	if err != nil {
		return fmt.Errorf("verify restored standby tree: %w", err)
	}
	var files, localIdentities int
	for i := range manifest.Files {
		if manifest.Files[i].Type != "file" {
			manifest.Files[i].LocalChangeID = ""
			continue
		}
		files++
		if local.Files[i].ChangeID != "" {
			localIdentities++
		}
		manifest.Files[i].LocalChangeID = local.Files[i].ChangeID
	}
	log.Info("Verified restored standby tree after readiness: snapshot=%s full_scan=%t files=%d local_identities=%d", manifest.ID, fullScan, files, localIdentities)
	return nil
}

func signRestoredStandbyManifest(ctx context.Context, manifest *SnapshotManifest, token string) error {
	manifest.State = "ready"
	manifest.Error = ""
	// Measure the digest once and reserve room for the signed fields. Signing enforces
	// the same limit, so the optional local identities must be dropped first to stay
	// droppable instead of failing the whole sync.
	digest, digestSize, err := manifestDigestWithSizeContext(ctx, manifest)
	if err != nil && !errors.Is(err, errManifestTooLarge) {
		return err
	}
	if errors.Is(err, errManifestTooLarge) || digestSize > manifestSizeLimit-manifestSignedFieldsSize {
		for i := range manifest.Files {
			manifest.Files[i].LocalChangeID = ""
		}
		log.Warn("Omit local standby file identities because the ready manifest exceeds its size limit: snapshot=%s", manifest.ID)
		digest, digestSize, err = manifestDigestWithSizeContext(ctx, manifest)
		if err != nil {
			return err
		}
		if digestSize > manifestSizeLimit-manifestSignedFieldsSize {
			return errManifestTooLarge
		}
	}
	applyIncrementalManifestSignature(manifest, digest, token)
	return nil
}

func persistReadyStandbySync(ctx context.Context, cfg *config, manifest *SnapshotManifest, cacheDir string) error {
	if err := signRestoredStandbyManifest(ctx, manifest, cfg.ControlToken); err != nil {
		return err
	}
	if err := persistStandbyManifestContext(ctx, cfg.SnapshotDir, manifest); err != nil {
		return err
	}
	currentPath := filepath.Join(cfg.SnapshotDir, "current.json")
	if err := linkFileSyncedAt(ctx, manifestPath(cfg.SnapshotDir, manifest.ID), currentPath); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Debug("Cannot hard-link current standby manifest; writing an independent copy: snapshot=%s error=%v", manifest.ID, err)
		if writeErr := writeManifestAtContext(ctx, currentPath, manifest); writeErr != nil {
			return errors.Join(fmt.Errorf("link current standby manifest: %w", err), fmt.Errorf("write current standby manifest: %w", writeErr))
		}
	}
	if err := removeStandbyFinalizeRequestCheckpoint(cfg.SnapshotDir); err != nil {
		log.Warn("Remove completed standby final request checkpoint: snapshot=%s error=%v", manifest.ID, err)
	}
	if err := removeStandbyPreflightRequestCheckpoint(cfg.SnapshotDir); err != nil {
		log.Warn("Remove completed standby preflight request checkpoint: snapshot=%s error=%v", manifest.ID, err)
	}
	pruneManifestFiles(cfg.SnapshotDir, cfg.SnapshotRetention, cfg.ControlToken)
	if err := os.RemoveAll(cacheDir); err != nil {
		log.Warn("Remove completed incremental cache: snapshot=%s error=%v", manifest.ID, err)
	} else if err := syncDirectory(filepath.Dir(cacheDir)); err != nil {
		log.Warn("Persist completed incremental cache removal: snapshot=%s error=%v", manifest.ID, err)
	}
	return nil
}

func resumablePreflightManifest(snapshotDir, token string) *SnapshotManifest {
	paths, err := listManifestPaths(snapshotDir)
	if err != nil {
		log.Warn("Cannot list standby manifests for preflight recovery in %s: %v", snapshotDir, err)
		return nil
	}
	sort.Sort(sort.Reverse(sort.StringSlice(paths)))
	now := time.Now().UTC()
	for _, path := range paths {
		id := strings.TrimSuffix(filepath.Base(path), ".json")
		if !validSnapshotID(id) {
			continue
		}
		manifest, err := loadManifestFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				log.Warn("Ignore invalid standby preflight recovery manifest %s: %v", path, err)
			}
			continue
		}
		if manifest.State != "preflight" {
			continue
		}
		if err := validateManifestIdentity(manifest, token); err != nil {
			log.Warn("Ignore untrusted standby preflight recovery manifest %s: %v", path, err)
			continue
		}
		if !preflightIsFresh(manifest, now) {
			log.Debug("Ignore stale standby preflight recovery manifest %s: created_at=%s", path, manifest.CreatedAt)
			continue
		}
		return manifest
	}
	return nil
}

func resumableFinalManifest(snapshotDir, token string) *SnapshotManifest {
	paths, err := listManifestPaths(snapshotDir)
	if err != nil {
		log.Warn("Cannot list standby manifests for final recovery in %s: %v", snapshotDir, err)
		return nil
	}
	sort.Sort(sort.Reverse(sort.StringSlice(paths)))
	for _, path := range paths {
		id := strings.TrimSuffix(filepath.Base(path), ".json")
		if !validSnapshotID(id) {
			continue
		}
		manifest, err := loadManifestFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				log.Warn("Ignore invalid standby final recovery manifest %s: %v", path, err)
			}
			continue
		}
		if manifest.State != "transferring" {
			continue
		}
		if err := validateManifestIdentity(manifest, token); err != nil {
			log.Warn("Ignore untrusted standby final recovery manifest %s: %v", path, err)
			continue
		}
		return manifest
	}
	return nil
}

func preserveFinalSession(err error) bool {
	if _, ok := errors.AsType[*chunkSourceChangedError](err); ok {
		return false
	}
	if statusErr, ok := errors.AsType[*httpResponseStatusError](err); ok {
		return shouldRetryHTTPStatus(statusErr.statusCode)
	}
	return shouldRetryRequestError(err)
}

func abortFinalSyncSession(cfg *config, client *http.Client, base, id string) {
	abortCtx, cancel := context.WithTimeout(context.Background(), cfg.ServiceTimeout)
	defer cancel()
	log.Warn("Final sync session %s did not complete; aborting remote session", id)
	if err := finishRemoteSession(abortCtx, client, base, cfg.ControlToken, id, "abort"); err != nil {
		log.Warn("Abort remote final sync session failed: snapshot=%s error=%v", id, err)
	}
}

func completeFinalSync(ctx context.Context, cfg *config, base string, client *http.Client, final, previous *SnapshotManifest, cacheDir string, localChunkCandidates ...map[string][]chunkLocation) error {
	syncStarted := time.Now()
	completed := false
	abortSession := true
	defer func() {
		if !completed && abortSession {
			abortFinalSyncSession(cfg, client, base, final.ID)
		}
	}()
	stopProgressHeartbeats := startFinalSessionProgressHeartbeats(ctx, client, base, cfg.ControlToken, final.ID, cfg.FinalSessionTimeout)
	defer stopProgressHeartbeats()
	if previous != nil && previous.State == "ready" && sameManifestContentTree(final, previous, setting.AppWorkPath) {
		var verifyErr error
		if !final.FullScanAt.IsZero() && (previous.FullScanAt.IsZero() || final.FullScanAt.After(previous.FullScanAt)) {
			// A scheduled full verification must rehash the installed tree instead of
			// trusting the local identities recorded when it was last verified.
			if verifyErr = verifyInstalledStandbyContent(ctx, setting.AppWorkPath, previous); verifyErr != nil {
				log.Warn("Full verification of the installed standby tree failed; rebuilding it: snapshot=%s error=%v", final.ID, verifyErr)
			}
		} else {
			var matches bool
			matches, verifyErr = installedTreeMatchesManifest(ctx, setting.AppWorkPath, previous)
			if verifyErr != nil {
				log.Warn("Could not verify the installed standby tree for unchanged sync; rebuilding it: snapshot=%s error=%v", final.ID, verifyErr)
			} else if !matches {
				verifyErr = errors.New("installed standby tree no longer matches its trusted baseline")
				log.Warn("Installed standby tree no longer matches its trusted baseline; rebuilding it: snapshot=%s", final.ID)
			}
		}
		if verifyErr != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		if verifyErr == nil {
			log.Info("Final manifest and installed standby tree match the trusted baseline outside regenerable data; skipping chunk preparation and tree activation: snapshot=%s files=%d bytes=%d", final.ID, final.FileCount, final.Size)
			remoteFinishStarted := time.Now()
			if err := finishRemoteSession(ctx, client, base, cfg.ControlToken, final.ID, "complete"); err != nil {
				log.Error("Complete unchanged final sync session failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(remoteFinishStarted), err)
				if preserveFinalSession(err) {
					abortSession = false
					log.Warn("Final sync session %s is retained for retry after transient completion failure: %v", final.ID, err)
				}
				return err
			}
			stopProgressHeartbeats()
			completed = true
			identityStarted := time.Now()
			if err := recordLocalChangeIDs(ctx, filepath.Clean(setting.AppWorkPath), final); err != nil {
				log.Error("Record standby file identities after unchanged sync failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(identityStarted), err)
				return fmt.Errorf("record standby file identities after unchanged sync: %w", err)
			}
			if err := persistReadyStandbySync(ctx, cfg, final, cacheDir); err != nil {
				log.Error("Persist unchanged standby sync failed: snapshot=%s error=%v", final.ID, err)
				return err
			}
			log.Info("Standby sync completed without rebuilding its data tree: snapshot=%s total_duration=%s", final.ID, time.Since(syncStarted))
			return nil
		}
	}
	if err := pruneChunkCache(ctx, cacheDir, final.ID, final); err != nil {
		return fmt.Errorf("prune stale replication chunk cache: %w", err)
	}
	chunkPassStarted := time.Now()
	var knownLocalCandidates map[string][]chunkLocation
	if len(localChunkCandidates) > 0 {
		knownLocalCandidates = localChunkCandidates[0]
	}
	cachedHashes, err := fetchMissingChunks(ctx, client, base, cfg.ControlToken, final, previous, cacheDir, false, &knownLocalCandidates)
	if err != nil {
		log.Error("Final chunk preparation failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(chunkPassStarted), err)
		if preserveFinalSession(err) {
			abortSession = false
			log.Warn("Final sync session %s is retained for retry after transient transport failure: %v", final.ID, err)
		}
		return err
	}
	log.Info("Final chunk preparation completed: snapshot=%s duration=%s", final.ID, time.Since(chunkPassStarted))
	// Release after final chunks are available; stage assembly still verifies each chunk hash.
	releaseStarted := time.Now()
	if err := finishRemoteSession(ctx, client, base, cfg.ControlToken, final.ID, "release"); err != nil {
		log.Error("Release primary after final chunk preparation failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(releaseStarted), err)
		return err
	}
	log.Info("Primary released after final chunk preparation; standby will update its data in place: snapshot=%s duration=%s", final.ID, time.Since(releaseStarted))
	root := filepath.Clean(setting.AppWorkPath)
	applyStarted := time.Now()
	var onDemandResponsesReceived, onDemandPayloadBytesReceived, onDemandEncodedPayloadBytes, onDemandEncodedBodyBytes, onDemandEncodedBodyMeasurements atomic.Int64
	fetch := func(fetchCtx context.Context, hash string) ([]byte, error) {
		started := time.Now()
		data, encodedBytes, err := requestChunk(fetchCtx, client, base, cfg.ControlToken, final.ID, hash)
		if err != nil {
			log.Warn("On-demand final chunk fetch failed: snapshot=%s hash=%s duration=%s error=%v", final.ID, hash, time.Since(started), err)
			return nil, err
		}
		onDemandResponsesReceived.Add(1)
		onDemandPayloadBytesReceived.Add(int64(len(data)))
		if encodedBytes >= 0 {
			onDemandEncodedPayloadBytes.Add(int64(len(data)))
			onDemandEncodedBodyBytes.Add(encodedBytes)
			onDemandEncodedBodyMeasurements.Add(1)
		}
		log.Debug("Fetched on-demand final chunk: snapshot=%s hash=%s payload_bytes=%d server_encoded_body_bytes=%d encoded_body_measured=%t duration=%s", final.ID, hash, len(data), encodedBytes, encodedBytes >= 0, time.Since(started))
		return data, nil
	}
	cachedSet := make(map[string]struct{}, len(cachedHashes))
	for _, hash := range cachedHashes {
		cachedSet[hash] = struct{}{}
	}
	_, applyErr := applyInPlace(ctx, inPlaceApplyOptions{
		Manifest: final, Previous: previous, CacheDir: cacheDir, Fetch: fetch,
		LocalCandidates: knownLocalCandidates, CachedHashes: cachedSet,
	})
	if responses := onDemandResponsesReceived.Load(); responses > 0 {
		log.Info("On-demand final chunk transfer summary: snapshot=%s responses_received=%d payload_bytes_received=%d server_encoded_body_bytes=%d measured_responses=%d/%d", final.ID, responses, onDemandPayloadBytesReceived.Load(), onDemandEncodedBodyBytes.Load(), onDemandEncodedBodyMeasurements.Load(), responses)
		logChunkEncodingSummary("On-demand final", final.ID, onDemandEncodedPayloadBytes.Load(), onDemandEncodedBodyBytes.Load(), onDemandEncodedBodyMeasurements.Load())
	}
	if applyErr != nil {
		log.Error("In-place standby update failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(applyStarted), applyErr)
		if preserveFinalSession(applyErr) {
			abortSession = false
			log.Warn("Final sync session %s is retained for retry after transient transport failure: %v", final.ID, applyErr)
		}
		return applyErr
	}
	verifyStarted := time.Now()
	if err := verifyRestoredStandbyTree(ctx, root, final, previous); err != nil {
		log.Error("In-place standby update verification failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(verifyStarted), err)
		return err
	}
	log.Info("Verified in-place standby update: snapshot=%s duration=%s", final.ID, time.Since(verifyStarted))
	log.Info("Standby update completed: snapshot=%s duration=%s; marking remote session complete", final.ID, time.Since(applyStarted))
	remoteFinishStarted := time.Now()
	finishErr := finishRemoteSession(ctx, client, base, cfg.ControlToken, final.ID, "complete")
	stopProgressHeartbeats()
	if finishErr != nil {
		log.Error("Remote final sync completion failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(remoteFinishStarted), finishErr)
		if preserveFinalSession(finishErr) {
			abortSession = false
			log.Warn("Final sync session %s is retained for retry after transient transport failure: %v", final.ID, finishErr)
		}
		return finishErr
	}
	log.Info("Remote final sync session completed: snapshot=%s duration=%s", final.ID, time.Since(remoteFinishStarted))
	completed = true
	if err := persistReadyStandbySync(ctx, cfg, final, cacheDir); err != nil {
		log.Error("Persist completed standby sync failed: snapshot=%s error=%v", final.ID, err)
		return err
	}
	log.Info("Standby restore completed successfully: snapshot=%s total_duration=%s", final.ID, time.Since(syncStarted))
	return nil
}

func resumeFinalSync(ctx context.Context, cfg *config, base string, client *http.Client, previous, final *SnapshotManifest, cacheDir string) (bool, error) {
	if final == nil {
		return false, nil
	}
	var stateChangedErr error
	for attempt := 1; attempt <= requestRetryLimit; attempt++ {
		status, err := requestSnapshotStatus(ctx, client, base, cfg.ControlToken, final.ID)
		if err != nil {
			if errors.Is(err, errRemoteSnapshotUnavailable) {
				log.Info("Remote final sync checkpoint is unavailable: snapshot=%s; starting a new sync", final.ID)
				return false, nil
			}
			log.Error("Cannot check remote status for final sync checkpoint: snapshot=%s error=%v", final.ID, err)
			return true, err
		}
		if status.State == "ready" {
			ready, err := requestManifestByID(ctx, client, base, cfg.ControlToken, final.ID, "ready")
			if errors.Is(err, errRemoteManifestStateChanged) {
				stateChangedErr = err
				log.Info("Remote final sync state changed while loading its completed manifest: snapshot=%s attempt=%d/%d; rechecking status", final.ID, attempt, requestRetryLimit)
				continue
			}
			if err != nil {
				log.Error("Cannot load completed remote manifest for final sync checkpoint: snapshot=%s error=%v", final.ID, err)
				return true, err
			}
			if !matchesCompletedFinalManifest(final, ready) {
				log.Warn("Completed remote final sync does not match local checkpoint: snapshot=%s; starting a new sync", final.ID)
				return false, nil
			}
			// Recovery cannot assume the tree matches the recorded identities, so it rehashes.
			if err := verifyRestoredStandbyTree(ctx, filepath.Clean(setting.AppWorkPath), ready, nil); err != nil {
				if ctx.Err() != nil {
					return true, ctx.Err()
				}
				log.Warn("Local data does not match completed final sync checkpoint: snapshot=%s error=%v; starting a new sync", final.ID, err)
				return false, nil
			}
			if err := persistReadyStandbySync(ctx, cfg, ready, cacheDir); err != nil {
				log.Error("Persist recovered completed standby sync failed: snapshot=%s error=%v", final.ID, err)
				return true, err
			}
			log.Info("Recovered completed standby sync without retransferring data: snapshot=%s", final.ID)
			return true, nil
		}
		if status.State == "failed" {
			log.Info("Remote final sync checkpoint is no longer active: snapshot=%s state=%s; starting a new sync", final.ID, status.State)
			return false, nil
		}
		if status.State != "transferring" {
			return true, fmt.Errorf("remote final sync checkpoint has unexpected state %q: snapshot=%s", status.State, final.ID)
		}
		stopManifestHeartbeats := startFinalSessionProgressHeartbeats(ctx, client, base, cfg.ControlToken, final.ID, cfg.FinalSessionTimeout)
		remote, err := requestManifestByID(ctx, client, base, cfg.ControlToken, final.ID, "transferring")
		stopManifestHeartbeats()
		if errors.Is(err, errRemoteManifestStateChanged) {
			stateChangedErr = err
			log.Info("Remote final sync state changed while loading its active manifest: snapshot=%s attempt=%d/%d; rechecking status", final.ID, attempt, requestRetryLimit)
			continue
		}
		if err != nil {
			log.Error("Cannot load remote manifest for final sync checkpoint: snapshot=%s error=%v", final.ID, err)
			return true, err
		}
		if remote.SHA256 != final.SHA256 {
			log.Error("Remote final sync manifest does not match local checkpoint: snapshot=%s local_sha256=%s remote_sha256=%s", final.ID, final.SHA256, remote.SHA256)
			return true, errors.New("active final sync manifest does not match the local recovery checkpoint")
		}
		log.Info("Resuming active final sync session %s from verified local chunk cache", final.ID)
		return true, completeFinalSync(ctx, cfg, base, client, remote, previous, cacheDir)
	}
	return true, fmt.Errorf("remote final sync state did not stabilize: %w", stateChangedErr)
}

func restoreIncremental(ctx context.Context, cfg *config, base string, client *http.Client) error {
	if err := ensurePrivateSnapshotDirectory(cfg.SnapshotDir); err != nil {
		return err
	}
	cacheDir := filepath.Join(cfg.SnapshotDir, ".chunks")
	finalRecovery := resumableFinalManifest(cfg.SnapshotDir, cfg.ControlToken)
	var stopRecoveryHeartbeats func()
	defer func() {
		if stopRecoveryHeartbeats != nil {
			stopRecoveryHeartbeats()
		}
	}()
	if finalRecovery != nil && stopRecoveryHeartbeats == nil {
		stopRecoveryHeartbeats = startFinalSessionProgressHeartbeats(ctx, client, base, cfg.ControlToken, finalRecovery.ID, cfg.FinalSessionTimeout)
	}
	pruneReplicationTemporaryFiles(cfg.SnapshotDir)
	pruneManifestFiles(cfg.SnapshotDir, cfg.SnapshotRetention, cfg.ControlToken)
	currentPath := filepath.Join(cfg.SnapshotDir, "current.json")
	previous := previousManifest(currentPath, cfg.ControlToken)
	trustedBaseline := previous != nil
	if previous == nil {
		root := filepath.Clean(setting.AppWorkPath)
		indexStarted := time.Now()
		log.Info("Indexing existing standby data for verified chunk reuse: path=%s", root)
		local, err := scanIncrementalTreeWithoutDigestForTask(ctx, root, nil, true, "local")
		if err != nil {
			log.Warn("Cannot index existing standby data for chunk reuse: duration=%s error=%v", time.Since(indexStarted), err)
		} else {
			previous = local
			log.Info("No trusted standby baseline; indexed local data for verified chunk reuse: entries=%d bytes=%d duration=%s", local.FileCount, local.Size, time.Since(indexStarted))
		}
	}
	if err := prepareChunkCache(cacheDir); err != nil {
		return err
	}
	if stopRecoveryHeartbeats != nil {
		stopRecoveryHeartbeats()
		stopRecoveryHeartbeats = nil
	}
	if resumed, err := resumeFinalSync(ctx, cfg, base, client, previous, finalRecovery, cacheDir); resumed {
		return err
	}
	if trustedBaseline {
		log.Info("Starting standby restore from trusted local baseline %s", previous.ID)
	} else if previous != nil {
		log.Info("Starting standby restore without a trusted baseline; local content will be verified and reused")
	} else {
		log.Info("Starting standby restore without reusable local data; a full preflight scan is expected")
	}
	recoveryPreflight := resumablePreflightManifest(cfg.SnapshotDir, cfg.ControlToken)
	for finalizeAttempt := 1; finalizeAttempt <= 2; finalizeAttempt++ {
		var preflight *SnapshotManifest
		var err error
		if recoveryPreflight != nil {
			checkpoint := recoveryPreflight
			recoveryPreflight = nil
			log.Info("Requesting verified preflight recovery checkpoint %s", checkpoint.ID)
			requestStarted := time.Now()
			preflight, err = requestManifest(ctx, client, base, cfg.ControlToken, "preflight?resume="+checkpoint.ID)
			if err != nil {
				if !isUnavailablePreflightCheckpointError(err) {
					log.Error("Preflight recovery checkpoint request failed: snapshot=%s duration=%s error=%v", checkpoint.ID, time.Since(requestStarted), err)
					return err
				}
				log.Warn("Preflight recovery checkpoint %s is unavailable; request a new preflight", checkpoint.ID)
			} else if preflight.SHA256 != checkpoint.SHA256 {
				log.Error("Preflight recovery checkpoint does not match local manifest: snapshot=%s", checkpoint.ID)
				return errors.New("preflight recovery checkpoint does not match the local manifest")
			}
		}
		if preflight == nil {
			log.Info("Requesting preflight manifest from %s", redactedEndpointLabel(base))
			preflightRequestCheckpoint, checkpointExists, err := loadStandbyPreflightRequestCheckpoint(cfg.SnapshotDir, cfg.ControlToken)
			if err != nil {
				log.Error("Cannot load standby preflight request checkpoint: %v", err)
				return err
			}
			recoveredRequest := checkpointExists
			if checkpointExists {
				log.Info("Loaded standby preflight request checkpoint: request_id=%s", preflightRequestCheckpoint.RequestID)
			} else {
				preflightRequestCheckpoint, err = newStandbyPreflightRequestCheckpoint(cfg.ControlToken)
				if err != nil {
					return err
				}
				if err := persistStandbyPreflightRequestCheckpoint(cfg.SnapshotDir, preflightRequestCheckpoint, cfg.ControlToken); err != nil {
					return fmt.Errorf("persist standby preflight request checkpoint: %w", err)
				}
				log.Info("Persisted standby preflight request checkpoint: request_id=%s", preflightRequestCheckpoint.RequestID)
			}
			persistRequestID := func(requestID string) error {
				preflightRequestCheckpoint.RequestID = requestID
				return persistStandbyPreflightRequestCheckpoint(cfg.SnapshotDir, preflightRequestCheckpoint, cfg.ControlToken)
			}
			requestStarted := time.Now()
			preflight, err = requestManifestWithRequestIDAndCallback(ctx, client, base, cfg.ControlToken, "preflight", preflightRequestCheckpoint.RequestID, 0, persistRequestID)
			if err != nil {
				log.Error("Preflight manifest request failed: duration=%s error=%v", time.Since(requestStarted), err)
				return err
			}
			if recoveredRequest && !preflightIsFresh(preflight, time.Now().UTC()) {
				log.Warn("Recovered preflight request returned a stale manifest: snapshot=%s age=%s reuse_window=%s; starting a fresh preflight", preflight.ID, time.Since(preflight.CreatedAt), reusablePreflightMaxAge)
				preflightRequestCheckpoint, err = newStandbyPreflightRequestCheckpoint(cfg.ControlToken)
				if err != nil {
					return err
				}
				if err := persistStandbyPreflightRequestCheckpoint(cfg.SnapshotDir, preflightRequestCheckpoint, cfg.ControlToken); err != nil {
					return fmt.Errorf("persist replacement standby preflight request checkpoint: %w", err)
				}
				persistRequestID = func(requestID string) error {
					preflightRequestCheckpoint.RequestID = requestID
					return persistStandbyPreflightRequestCheckpoint(cfg.SnapshotDir, preflightRequestCheckpoint, cfg.ControlToken)
				}
				requestStarted = time.Now()
				preflight, err = requestManifestWithRequestIDAndCallback(ctx, client, base, cfg.ControlToken, "preflight", preflightRequestCheckpoint.RequestID, 0, persistRequestID)
				if err != nil {
					log.Error("Fresh preflight manifest request failed: duration=%s error=%v", time.Since(requestStarted), err)
					return err
				}
			}
		}
		if err := persistStandbyManifestContext(ctx, cfg.SnapshotDir, preflight); err != nil {
			return err
		}
		if err := removeStandbyPreflightRequestCheckpoint(cfg.SnapshotDir); err != nil {
			log.Warn("Remove persisted standby preflight request checkpoint failed: snapshot=%s error=%v", preflight.ID, err)
		}
		pruneManifestFiles(cfg.SnapshotDir, cfg.SnapshotRetention, cfg.ControlToken)
		log.Info("Received preflight manifest %s with %d entries and %s of content", preflight.ID, preflight.FileCount, strconv.FormatInt(preflight.Size, 10))
		// The final scan can differ; retain baseline chunks until its manifest arrives.
		if err := pruneChunkCache(ctx, cacheDir, preflight.ID, preflight, previous); err != nil {
			return fmt.Errorf("prune stale replication chunk cache: %w", err)
		}
		var localChunkCandidates map[string][]chunkLocation
		if trustedBaseline && sameManifestContentTree(preflight, previous, setting.AppWorkPath) {
			log.Info("Preflight manifest matches the trusted standby baseline outside regenerable data; defer chunk transfer until the final manifest: snapshot=%s files=%d bytes=%d", preflight.ID, preflight.FileCount, preflight.Size)
		} else {
			if _, err := fetchMissingChunks(ctx, client, base, cfg.ControlToken, preflight, previous, cacheDir, true, &localChunkCandidates); err != nil {
				return err
			}
		}
		if err := checkApplyCapacityBeforeFinalize(preflight, previous, cacheDir); err != nil {
			log.Error("Refusing to stop the primary because the standby cannot hold the in-place update: snapshot=%s error=%v", preflight.ID, err)
			return err
		}
		log.Info("Requesting final sync manifest based on preflight %s", preflight.ID)
		requestStarted := time.Now()
		loadedRequestCheckpoint, requestCheckpointExists, err := loadStandbyFinalizeRequestCheckpoint(cfg.SnapshotDir, preflight.ID, cfg.ControlToken)
		if err != nil {
			log.Error("Cannot load standby final request checkpoint: preflight=%s error=%v", preflight.ID, err)
			return err
		}
		var requestCheckpoint *standbyFinalizeRequestCheckpoint
		if requestCheckpointExists {
			requestCheckpoint = &loadedRequestCheckpoint
		}
		if requestCheckpoint == nil {
			requestCheckpoint, err = newStandbyFinalizeRequestCheckpoint(preflight.ID, cfg.ControlToken)
			if err != nil {
				return err
			}
			if err := persistStandbyFinalizeRequestCheckpoint(cfg.SnapshotDir, requestCheckpoint, cfg.ControlToken); err != nil {
				return fmt.Errorf("persist standby final request checkpoint: %w", err)
			}
			log.Info("Persisted standby final request checkpoint: request_id=%s preflight=%s", requestCheckpoint.RequestID, preflight.ID)
		} else {
			log.Info("Resuming standby final request checkpoint: request_id=%s preflight=%s", requestCheckpoint.RequestID, preflight.ID)
		}
		final, err := requestManifestWithRequestID(ctx, client, base, cfg.ControlToken, "final?base="+preflight.ID, requestCheckpoint.RequestID, cfg.FinalSessionTimeout)
		if isEndedFinalizeRequestError(err) {
			log.Warn("Previous final sync request already ended; creating a new request: request_id=%s preflight=%s", requestCheckpoint.RequestID, preflight.ID)
			requestCheckpoint, err = newStandbyFinalizeRequestCheckpoint(preflight.ID, cfg.ControlToken)
			if err != nil {
				return err
			}
			if err := persistStandbyFinalizeRequestCheckpoint(cfg.SnapshotDir, requestCheckpoint, cfg.ControlToken); err != nil {
				return fmt.Errorf("persist replacement standby final request checkpoint: %w", err)
			}
			final, err = requestManifestWithRequestID(ctx, client, base, cfg.ControlToken, "final?base="+preflight.ID, requestCheckpoint.RequestID, cfg.FinalSessionTimeout)
		}
		if err != nil {
			if finalizeAttempt == 1 && isUnavailablePreflightBaseError(err) {
				log.Warn("Finalize rejected preflight base %s; rerun preflight once", preflight.ID)
				continue
			}
			log.Error("Final sync manifest request failed: preflight=%s duration=%s error=%v", preflight.ID, time.Since(requestStarted), err)
			return err
		}
		stopManifestPersistenceHeartbeats := startFinalSessionProgressHeartbeats(ctx, client, base, cfg.ControlToken, final.ID, cfg.FinalSessionTimeout)
		if err := func() error {
			defer stopManifestPersistenceHeartbeats()
			if err := persistStandbyManifestContext(ctx, cfg.SnapshotDir, final); err != nil {
				return err
			}
			if err := removeStandbyFinalizeRequestCheckpoint(cfg.SnapshotDir); err != nil {
				log.Warn("Remove persisted standby final request checkpoint failed: snapshot=%s error=%v", final.ID, err)
			}
			pruneManifestFiles(cfg.SnapshotDir, cfg.SnapshotRetention, cfg.ControlToken)
			log.Info("Received final sync manifest %s with %d entries and %s of content", final.ID, final.FileCount, strconv.FormatInt(final.Size, 10))
			return nil
		}(); err != nil {
			return err
		}
		return completeFinalSync(ctx, cfg, base, client, final, previous, cacheDir, localChunkCandidates)
	}
	return errors.New("final sync manifest was rejected twice")
}

func writeManifestAt(path string, manifest *SnapshotManifest) error {
	return writeManifestAtContext(context.Background(), path, manifest)
}

func writeManifestAtContext(ctx context.Context, path string, manifest *SnapshotManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := file.Name()
	defer os.Remove(tmp)
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if err := writeManifestJSONContext(ctx, file, manifest, false); err != nil {
		return errors.Join(err, file.Close())
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func incrementalBase(cfg *config) string {
	base := strings.TrimRight(cfg.ControlSourceURL, "/")
	if base == "" {
		base = strings.TrimRight(cfg.SourceURL, "/") + "/_replication"
	}
	return base
}
