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
	baselineCopyBufferSize       = 256 << 10
	statusBodyPreviewLimit       = 4 << 10
	maxEncodedStatusBodySize     = 64 << 10
	maxJSONResponseSize          = 1 << 20
	maxRetryResponseDrain        = 64 << 10
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
	baselineCopyBufferPool        = sync.Pool{New: func() any { return new([baselineCopyBufferSize]byte) }}
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
	var streamErr http2.StreamError
	if errors.As(err, &streamErr) {
		switch streamErr.Code {
		case http2.ErrCodeInternal, http2.ErrCodeRefusedStream, http2.ErrCodeCancel, http2.ErrCodeEnhanceYourCalm:
			return true
		}
	}
	var goAwayErr http2.GoAwayError
	if errors.As(err, &goAwayErr) && goAwayErr.ErrCode == http2.ErrCodeNo {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
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

func decodeBoundedJSON(body io.Reader, maxSize int64, value any) error {
	data, err := io.ReadAll(io.LimitReader(body, maxSize+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > maxSize {
		return fmt.Errorf("JSON response exceeds maximum size of %d bytes", maxSize)
	}
	return json.Unmarshal(data, value)
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
	decoder := json.NewDecoderV1(limited)
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
	var statusErr *httpResponseStatusError
	if errors.As(err, &statusErr) {
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
	var statusErr *httpResponseStatusError
	if errors.As(err, &statusErr) {
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

func renewRemoteSession(ctx context.Context, client *http.Client, base, token, id, renewalID string) error {
	if !validReplicationRequestID(renewalID) {
		return errors.New("invalid final sync session renewal ID")
	}
	return finishRemoteSessionAt(ctx, client, base, token, id, "renew", "?renewal_id="+renewalID)
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
		if recovered := recoverCheckpointedReadyManifest(path, token, manifest); recovered != nil {
			return recovered
		}
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

func recoverCheckpointedReadyManifest(currentPath, token string, current *SnapshotManifest) *SnapshotManifest {
	snapshotDir := filepath.Dir(currentPath)
	checkpointPath := filepath.Join(snapshotDir, stageCheckpointName)
	checkpoint, err := readStageCheckpoint(checkpointPath)
	if err != nil || !checkpoint.StandbyReady ||
		!validSnapshotID(checkpoint.SnapshotID) || len(checkpoint.ManifestSHA) != sha256.Size*2 || !isLowerHex(checkpoint.ManifestSHA) {
		return nil
	}
	ready, err := loadTrustedManifest(manifestPath(snapshotDir, checkpoint.SnapshotID), token, "ready")
	if err != nil || !matchesStageCheckpoint(ready, checkpoint) {
		return nil
	}
	if current.ID != ready.ID || current.SHA256 != ready.SHA256 {
		log.Warn("Recover completed standby baseline from installation checkpoint: snapshot=%s current=%s", ready.ID, current.ID)
		if err := writeManifestAt(currentPath, ready); err != nil {
			log.Warn("Could not update current standby baseline from installation checkpoint: snapshot=%s error=%v", ready.ID, err)
			return ready
		}
		log.Info("Recovered current standby baseline from completed installation: snapshot=%s", ready.ID)
	}
	if err := removeFileSynced(checkpointPath); err != nil {
		log.Warn("Remove recovered standby installation checkpoint: snapshot=%s error=%v", ready.ID, err)
	}
	return ready
}

func matchesStageCheckpoint(ready *SnapshotManifest, checkpoint stageCheckpoint) bool {
	if ready == nil || ready.ID != checkpoint.SnapshotID || ready.State != "ready" {
		return false
	}
	transfer := *ready
	transfer.Snapshot.State = "transferring"
	transfer.Snapshot.Error = ""
	digest, err := manifestDigestWithoutLocalChangeIDs(&transfer)
	return err == nil && digest == checkpoint.ManifestSHA
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
	if err := json.NewDecoderV1(io.LimitReader(file, int64(maxManifestSize)+1)).Decode(&manifest); err != nil {
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
	return verifyFile(cachePath(cacheDir, hash), hash) == nil
}

// Verify persisted cache candidates during preflight, before the primary outage begins.
func cachedChunkAvailable(cacheDir, hash string, size int64, verify bool) bool {
	path := cachePath(cacheDir, hash)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != size {
		return false
	}
	return !verify || verifyFile(path, hash) == nil
}

func pruneChunkCache(ctx context.Context, cacheDir, snapshotID string, manifests ...*SnapshotManifest) error {
	keep := make(map[string]int64)
	keptManifestCount := 0
	for _, manifest := range manifests {
		if manifest == nil {
			continue
		}
		keptManifestCount++
		var inferredZeroHashes map[string]struct{}
		if manifest.FormatVersion == legacyIncrementalFormatVersion {
			var err error
			inferredZeroHashes, err = manifestZeroChunkHashes(ctx, manifest)
			if err != nil {
				return err
			}
		}
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
				if chunkIsZero(chunk, inferredZeroHashes) {
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
	return float64(size) / duration.Seconds() / (1 << 20)
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
	var inferredZeroHashes map[string]struct{}
	if manifest.FormatVersion == legacyIncrementalFormatVersion {
		var err error
		inferredZeroHashes, err = manifestZeroChunkHashes(ctx, manifest)
		if err != nil {
			return chunkFetchPlan{}, err
		}
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
	// A missing file identity is safe here because staging verifies candidate bytes by hash.
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
				zero := chunkIsZero(chunk, inferredZeroHashes)
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
				if err := addChunkPlanEntry(states, chunk, chunkIsZero(chunk, inferredZeroHashes)); err != nil {
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
			if target.state == chunkPlanNeeded && cachedChunkAvailable(cacheDir, hash, target.size, true) {
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
			if state == chunkPlanReusable || (!preflight && cachedChunkAvailable(cacheDir, chunk.Hash, chunk.Size, false)) {
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

func fetchMissingChunks(ctx context.Context, client *http.Client, base, token string, manifest, previous *SnapshotManifest, cacheDir string, preflight bool, localCandidates ...*map[string][]chunkLocation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var knownLocalCandidates map[string][]chunkLocation
	if len(localCandidates) > 0 && localCandidates[0] != nil {
		knownLocalCandidates = *localCandidates[0]
	}
	passStarted := time.Now()
	planningStarted := time.Now()
	plan, err := planMissingChunks(ctx, manifest, previous, cacheDir, preflight, knownLocalCandidates)
	if err != nil {
		return err
	}
	stageDir := filepath.Dir(setting.AppWorkPath)
	if setting.AppWorkPath == "" {
		stageDir = filepath.Dir(cacheDir)
	}
	if err := checkRestoreCapacity(cacheDir, stageDir, manifest.Size, plan.missingSize, int64(len(manifest.Files)), int64(len(plan.hashes))); err != nil {
		return fmt.Errorf("preflight replication disk capacity: %w", err)
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
			return err
		}
		log.Info("Final chunk pass prepared for snapshot %s: download_chunks=%d reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d previous_local_files_skipped=%d previous_local_files_unavailable=%d changed_file_reindex_attempts=%d changed_file_manifest_bytes=%d invalidated_local_candidates=%d indexed_local_candidates=%d indexed_local_payload_bytes=%d total_chunks=%d expected_payload_bytes=%d duration=%s", manifest.ID, len(plan.hashes), plan.reusable, plan.reusableSize, plan.zeroChunks, plan.zeroSize, plan.previousLocalFilesSkipped, plan.previousLocalFilesUnavailable, plan.localReindexAttempts, plan.localReindexBytes, plan.invalidatedLocalCandidates, plan.indexedLocalCandidates, plan.indexedLocalSize, plan.total, plan.missingSize, time.Since(passStarted))
		return nil
	}
	return fetchPreflightChunksConcurrently(ctx, client, base, token, manifest.ID, plan.hashes, plan.sizes, cacheDir, plan.verifiedCacheShards, plan.total, plan.reusable, plan.reusableSize, plan.zeroChunks, plan.zeroSize, plan.missingSize, time.Since(passStarted))
}

type chunkFetchRequest struct {
	hash string
	size int64
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
	jobs := make(chan chunkFetchRequest)
	workerCount := min(finalChunkFetchWorkers, len(hashes))
	var workers sync.WaitGroup
	var firstErr error
	var firstHash string
	var firstErrOnce sync.Once
	var cacheShards sync.Map
	var chunksStarted atomic.Int64
	var fetched atomic.Int64
	var fetchedBytes atomic.Int64
	var encodedPayloadBytes atomic.Int64
	var encodedBodyBytes atomic.Int64
	var encodedBodyMeasurements atomic.Int64
	var lastProgress atomic.Int64
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for request := range jobs {
				if workerCtx.Err() != nil {
					return
				}
				chunksStarted.Add(1)
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
				count := fetched.Add(1)
				if encodedBytes >= 0 {
					encodedPayloadBytes.Add(int64(len(data)))
					encodedBodyBytes.Add(encodedBytes)
					encodedBodyMeasurements.Add(1)
				}
				bytes := fetchedBytes.Add(int64(len(data)))
				now := time.Now()
				logProgress := false
				if last := lastProgress.Load(); last == 0 {
					logProgress = lastProgress.CompareAndSwap(0, now.UnixNano())
				} else if now.Sub(time.Unix(0, last)) >= 30*time.Second && lastProgress.CompareAndSwap(last, now.UnixNano()) {
					logProgress = true
				}
				if logProgress {
					elapsed := time.Since(started)
					rate := transferRateMiBPerSecond(bytes, elapsed)
					log.Info("Final chunk transfer progress: snapshot=%s chunks_started=%d fetched_chunks=%d download_chunks=%d total_chunks=%d payload_bytes=%d/%d server_encoded_body_bytes=%d measured_responses=%d/%d average_payload_mib_per_sec=%.2f reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d elapsed=%s", id, chunksStarted.Load(), count, len(hashes), total, bytes, totalBytes, encodedBodyBytes.Load(), encodedBodyMeasurements.Load(), count, rate, reusableCandidates, reusableBytes, zeroChunks, zeroBytes, elapsed)
				}
			}
		}()
	}
sendJobs:
	for i, hash := range hashes {
		select {
		case <-workerCtx.Done():
			break sendJobs
		case jobs <- chunkFetchRequest{hash: hash, size: sizes[i]}:
		}
	}
	close(jobs)
	workers.Wait()
	if err := syncChunkCacheDirectories(cacheDir, id, "final-fetch", &cacheShards); err != nil {
		log.Error("Persist final chunk cache failed: snapshot=%s error=%v", id, err)
		firstErr = errors.Join(firstErr, err)
	}
	logChunkEncodingSummary("Final", id, encodedPayloadBytes.Load(), encodedBodyBytes.Load(), encodedBodyMeasurements.Load())
	elapsed := time.Since(started)
	bytes := fetchedBytes.Load()
	rate := transferRateMiBPerSecond(bytes, elapsed)
	if err := ctx.Err(); err != nil && errors.Is(firstErr, err) {
		startedCount := chunksStarted.Load()
		fetchedCount := fetched.Load()
		log.Error("Final chunk transfer canceled: snapshot=%s hash=%s chunks_started=%d fetched=%d/%d incomplete=%d not_started=%d payload_bytes=%d/%d server_encoded_body_bytes=%d measured_responses=%d/%d average_payload_mib_per_sec=%.2f reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d elapsed=%s error=%v", id, firstHash, startedCount, fetchedCount, len(hashes), max(int64(0), startedCount-fetchedCount), max(int64(0), int64(len(hashes))-startedCount), bytes, totalBytes, encodedBodyBytes.Load(), encodedBodyMeasurements.Load(), fetchedCount, rate, reusableCandidates, reusableBytes, zeroChunks, zeroBytes, elapsed, firstErr)
		return firstErr
	}
	if firstErr != nil {
		startedCount := chunksStarted.Load()
		fetchedCount := fetched.Load()
		log.Error("Final chunk transfer failed: snapshot=%s hash=%s chunks_started=%d fetched=%d/%d incomplete=%d not_started=%d payload_bytes=%d/%d server_encoded_body_bytes=%d measured_responses=%d/%d average_payload_mib_per_sec=%.2f reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d elapsed=%s error=%v", id, firstHash, startedCount, fetchedCount, len(hashes), max(int64(0), startedCount-fetchedCount), max(int64(0), int64(len(hashes))-startedCount), bytes, totalBytes, encodedBodyBytes.Load(), encodedBodyMeasurements.Load(), fetchedCount, rate, reusableCandidates, reusableBytes, zeroChunks, zeroBytes, elapsed, firstErr)
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		startedCount := chunksStarted.Load()
		fetchedCount := fetched.Load()
		log.Error("Final chunk transfer canceled: snapshot=%s chunks_started=%d fetched=%d/%d incomplete=%d not_started=%d payload_bytes=%d/%d server_encoded_body_bytes=%d measured_responses=%d/%d average_payload_mib_per_sec=%.2f reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d elapsed=%s error=%v", id, startedCount, fetchedCount, len(hashes), max(int64(0), startedCount-fetchedCount), max(int64(0), int64(len(hashes))-startedCount), bytes, totalBytes, encodedBodyBytes.Load(), encodedBodyMeasurements.Load(), fetchedCount, rate, reusableCandidates, reusableBytes, zeroChunks, zeroBytes, elapsed, err)
		return err
	}
	log.Info("Final chunk transfer completed: snapshot=%s chunks_started=%d fetched=%d/%d payload_bytes=%d/%d server_encoded_body_bytes=%d measured_responses=%d/%d average_payload_mib_per_sec=%.2f reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d elapsed=%s", id, chunksStarted.Load(), fetched.Load(), len(hashes), bytes, totalBytes, encodedBodyBytes.Load(), encodedBodyMeasurements.Load(), fetched.Load(), rate, reusableCandidates, reusableBytes, zeroChunks, zeroBytes, elapsed)
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
	jobs := make(chan chunkFetchRequest)
	workerCount := min(preflightChunkWorkers, len(hashes))
	var workers sync.WaitGroup
	var firstErr error
	var firstHash string
	var firstErrOnce sync.Once
	var chunksStarted atomic.Int64
	var fetched atomic.Int64
	var fetchedBytes atomic.Int64
	var encodedPayloadBytes atomic.Int64
	var encodedBodyBytes atomic.Int64
	var encodedBodyMeasurements atomic.Int64
	var deferred atomic.Int64
	var lastProgress atomic.Int64
	var stopAfterChurn atomic.Bool
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for request := range jobs {
				if stopAfterChurn.Load() {
					continue
				}
				if workerCtx.Err() != nil {
					return
				}
				chunksStarted.Add(1)
				data, encodedBytes, err := fetchChunkWithIntegrityRetry(workerCtx, id, request.hash, request.size,
					func() ([]byte, int64, error) { return requestChunk(workerCtx, client, base, token, id, request.hash) },
					func(data []byte) error { return storeChunkForBatch(cacheDir, request.hash, data) },
				)
				if err != nil {
					if stopAfterChurn.Load() && errors.Is(err, context.Canceled) {
						return
					}
					var changed *chunkSourceChangedError
					if errors.As(err, &changed) {
						count := deferred.Add(1)
						if count <= chunkChangeWarnBurst {
							log.Warn("Preflight chunk changed; snapshot=%s hash=%s deferred_to=final_sync", id, request.hash)
						} else if count%chunkProgressLogStride == 0 {
							processed := int64(cached) + fetched.Load() + count
							log.Warn("Preflight progress for snapshot %s remains unstable: processed=%d/%d fetched=%d cached=%d deferred=%d", id, processed, total, fetched.Load(), cached, count)
						}
						fetchedCount := fetched.Load()
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
				count := fetched.Add(1)
				if encodedBytes >= 0 {
					encodedPayloadBytes.Add(int64(len(data)))
					encodedBodyBytes.Add(encodedBytes)
					encodedBodyMeasurements.Add(1)
				}
				bytes := fetchedBytes.Add(int64(len(data)))
				now := time.Now()
				logProgress := false
				if last := lastProgress.Load(); last == 0 {
					logProgress = lastProgress.CompareAndSwap(0, now.UnixNano())
				} else if now.Sub(time.Unix(0, last)) >= 30*time.Second && lastProgress.CompareAndSwap(last, now.UnixNano()) {
					logProgress = true
				}
				if logProgress {
					elapsed := time.Since(started)
					rate := transferRateMiBPerSecond(bytes, elapsed)
					log.Info("Preflight chunk transfer progress: snapshot=%s chunks_started=%d fetched_chunks=%d/%d total_chunks=%d payload_bytes=%d expected_payload_bytes=%d server_encoded_body_bytes=%d measured_responses=%d/%d average_payload_mib_per_sec=%.2f reusable_candidates=%d zero_chunks=%d zero_payload_bytes=%d deferred_changed=%d elapsed=%s", id, chunksStarted.Load(), count, len(hashes), total, bytes, expectedBytes, encodedBodyBytes.Load(), encodedBodyMeasurements.Load(), count, rate, cached, zeroChunks, zeroBytes, deferred.Load(), elapsed)
				}
			}
		}()
	}
sendJobs:
	for i, hash := range hashes {
		if stopAfterChurn.Load() {
			break
		}
		select {
		case <-workerCtx.Done():
			break sendJobs
		case jobs <- chunkFetchRequest{hash: hash, size: sizes[i]}:
		}
	}
	close(jobs)
	workers.Wait()
	if err := syncChunkCacheDirectories(cacheDir, id, "preflight-fetch", &cacheShards); err != nil {
		log.Error("Persist preflight chunk cache failed: snapshot=%s error=%v", id, err)
		firstErr = errors.Join(firstErr, err)
	}
	logChunkEncodingSummary("Preflight", id, encodedPayloadBytes.Load(), encodedBodyBytes.Load(), encodedBodyMeasurements.Load())
	elapsed := preparationDuration + time.Since(started)
	startedCount := chunksStarted.Load()
	fetchedCount, bytes, deferredCount := fetched.Load(), fetchedBytes.Load(), deferred.Load()
	incompleteCount := max(int64(0), startedCount-fetchedCount-deferredCount)
	notStartedCount := max(int64(0), int64(len(hashes))-startedCount)
	if err := ctx.Err(); err != nil && errors.Is(firstErr, err) {
		log.Error("Preflight chunk transfer canceled: snapshot=%s hash=%s chunks_started=%d fetched=%d/%d payload_bytes=%d expected_payload_bytes=%d server_encoded_body_bytes=%d measured_responses=%d/%d reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d average_payload_mib_per_sec=%.2f deferred_changed=%d incomplete=%d not_started=%d total=%d duration=%s error=%v", id, firstHash, startedCount, fetchedCount, len(hashes), bytes, expectedBytes, encodedBodyBytes.Load(), encodedBodyMeasurements.Load(), fetchedCount, cached, cachedBytes, zeroChunks, zeroBytes, transferRateMiBPerSecond(bytes, elapsed), deferredCount, incompleteCount, notStartedCount, total, elapsed, firstErr)
		return firstErr
	}
	if firstErr != nil {
		log.Error("Preflight chunk transfer failed: snapshot=%s hash=%s chunks_started=%d fetched=%d/%d payload_bytes=%d expected_payload_bytes=%d server_encoded_body_bytes=%d measured_responses=%d/%d reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d average_payload_mib_per_sec=%.2f deferred_changed=%d incomplete=%d not_started=%d total=%d duration=%s error=%v", id, firstHash, startedCount, fetchedCount, len(hashes), bytes, expectedBytes, encodedBodyBytes.Load(), encodedBodyMeasurements.Load(), fetchedCount, cached, cachedBytes, zeroChunks, zeroBytes, transferRateMiBPerSecond(bytes, elapsed), deferredCount, incompleteCount, notStartedCount, total, elapsed, firstErr)
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		log.Error("Preflight chunk transfer canceled: snapshot=%s chunks_started=%d fetched=%d/%d payload_bytes=%d expected_payload_bytes=%d server_encoded_body_bytes=%d measured_responses=%d/%d reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d average_payload_mib_per_sec=%.2f deferred_changed=%d incomplete=%d not_started=%d total=%d duration=%s error=%v", id, startedCount, fetchedCount, len(hashes), bytes, expectedBytes, encodedBodyBytes.Load(), encodedBodyMeasurements.Load(), fetchedCount, cached, cachedBytes, zeroChunks, zeroBytes, transferRateMiBPerSecond(bytes, elapsed), deferredCount, incompleteCount, notStartedCount, total, elapsed, err)
		return err
	}
	if fetchedCount == 0 && deferredCount > 0 {
		log.Warn("Finished preflight chunk pass for snapshot %s without caching any chunks; source changed before every fetch (cached=%d deferred=%d total=%d elapsed=%s)", id, cached, deferredCount, total, elapsed)
	}
	log.Info("Finished preflight chunk pass for snapshot %s: chunks_started=%d fetched=%d/%d payload_bytes=%d expected_payload_bytes=%d server_encoded_body_bytes=%d measured_responses=%d/%d reusable_candidates=%d reusable_candidate_payload_bytes=%d zero_chunks=%d zero_payload_bytes=%d average_payload_mib_per_sec=%.2f deferred_changed=%d incomplete=%d not_started=%d total=%d duration=%s", id, startedCount, fetchedCount, len(hashes), bytes, expectedBytes, encodedBodyBytes.Load(), encodedBodyMeasurements.Load(), fetchedCount, cached, cachedBytes, zeroChunks, zeroBytes, transferRateMiBPerSecond(bytes, elapsed), deferredCount, incompleteCount, notStartedCount, total, elapsed)
	return nil
}

func sameFile(a, b TreeEntry) bool {
	return a.Type == "file" && b.Type == "file" && a.ChangeID != "" && b.ChangeID != "" &&
		a.Size == b.Size && a.Mode == b.Mode &&
		a.ModTimeNS == b.ModTimeNS && sameChunks(a.Chunks, b.Chunks)
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

func copyFileWithContext(ctx context.Context, dst, src *os.File, expectedSize int64) (int64, error) {
	if expectedSize < 0 {
		return 0, errors.New("cannot copy a baseline file with a negative size")
	}
	buffer := baselineCopyBufferPool.Get().(*[baselineCopyBufferSize]byte)
	defer baselineCopyBufferPool.Put(buffer)
	var copied int64
	remaining := expectedSize
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return copied, err
		}
		readSize := min(int64(len(buffer)), remaining)
		read, readErr := src.Read(buffer[:int(readSize)])
		if read > 0 {
			remaining -= int64(read)
			if isZeroChunk(buffer[:read]) {
				if _, err := dst.Seek(int64(read), io.SeekCurrent); err != nil {
					return copied, err
				}
			} else {
				for written := 0; written < read; {
					n, writeErr := dst.Write(buffer[written:read])
					copied += int64(n)
					written += n
					if writeErr != nil {
						return copied, writeErr
					}
					if n == 0 {
						return copied, io.ErrShortWrite
					}
				}
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				if remaining > 0 {
					return copied, fmt.Errorf("%w: source file shrank while copying", errIncrementalTreeChanged)
				}
				break
			}
			return copied, readErr
		}
		if read == 0 && remaining > 0 {
			return copied, io.ErrNoProgress
		}
	}
	if err := dst.Truncate(expectedSize); err != nil {
		return copied, err
	}
	return copied, nil
}

// Keep staging independent from the old root so standby writes cannot change rollback data.
func materializeBaselineFile(ctx context.Context, sourceRoot, sourceRelative string, sourceInfo os.FileInfo, dst string, entry TreeEntry) (bool, int64, os.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return false, 0, nil, err
	}
	sourceFile, openedInfo, err := openRegularFileBeneath(sourceRoot, sourceRelative)
	if err != nil {
		return false, 0, nil, err
	}
	defer sourceFile.Close()
	if !openedInfo.Mode().IsRegular() || !os.SameFile(sourceInfo, openedInfo) || openedInfo.Size() != entry.Size ||
		uint32(openedInfo.Mode().Perm()) != entry.Mode || openedInfo.ModTime().UnixNano() != entry.ModTimeNS {
		return false, 0, nil, errIncrementalTreeChanged
	}
	openedChangeID := fileChangeID(openedInfo)
	if openedChangeID == "" || openedChangeID != fileChangeID(sourceInfo) {
		return false, 0, nil, errIncrementalTreeChanged
	}

	out, err := os.CreateTemp(filepath.Dir(dst), ".replication-reuse-*")
	if err != nil {
		return false, 0, nil, err
	}
	tmp := out.Name()
	defer os.Remove(tmp)
	cloned := cloneFileData(out, sourceFile) == nil
	var copied int64
	if !cloned {
		if err := out.Truncate(0); err != nil {
			_ = out.Close()
			return false, 0, nil, err
		}
		if _, err := out.Seek(0, io.SeekStart); err != nil {
			_ = out.Close()
			return false, 0, nil, err
		}
		if _, err := sourceFile.Seek(0, io.SeekStart); err != nil {
			_ = out.Close()
			return false, 0, nil, err
		}
		copied, err = copyFileWithContext(ctx, out, sourceFile, entry.Size)
		if err != nil {
			_ = out.Close()
			return false, copied, nil, fmt.Errorf("copy baseline file %q: %w", sourceRelative, err)
		}
	}
	if err := ctx.Err(); err != nil {
		_ = out.Close()
		return false, copied, nil, err
	}
	if err := out.Chmod(os.FileMode(entry.Mode)); err != nil {
		_ = out.Close()
		return false, copied, nil, err
	}
	mtime := time.Unix(0, entry.ModTimeNS)
	if err := os.Chtimes(tmp, mtime, mtime); err != nil {
		_ = out.Close()
		return false, copied, nil, err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return false, copied, nil, err
	}
	if err := out.Close(); err != nil {
		return false, copied, nil, err
	}
	after, err := sourceFile.Stat()
	if err != nil {
		return false, copied, nil, err
	}
	pathFile, pathInfo, err := openRegularFileBeneath(sourceRoot, sourceRelative)
	if err != nil {
		return false, copied, nil, err
	}
	pathCloseErr := pathFile.Close()
	if pathCloseErr != nil {
		return false, copied, nil, pathCloseErr
	}
	if !after.Mode().IsRegular() || !os.SameFile(openedInfo, after) || !os.SameFile(openedInfo, pathInfo) || after.Size() != entry.Size ||
		uint32(after.Mode().Perm()) != entry.Mode || after.ModTime().UnixNano() != entry.ModTimeNS {
		return false, copied, nil, errIncrementalTreeChanged
	}
	if fileChangeID(after) != openedChangeID {
		return false, copied, nil, errIncrementalTreeChanged
	}
	if err := os.Rename(tmp, dst); err != nil {
		return false, copied, nil, err
	}
	stagedInfo, err := os.Lstat(dst)
	if err != nil {
		return false, copied, nil, err
	}
	if !stagedInfo.Mode().IsRegular() || stagedInfo.Size() != entry.Size ||
		uint32(stagedInfo.Mode().Perm()) != entry.Mode || stagedInfo.ModTime().UnixNano() != entry.ModTimeNS {
		return false, copied, nil, errors.New("materialized baseline file metadata does not match manifest")
	}
	return cloned, copied, stagedInfo, nil
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

func reusableWholeFileInfoBeneath(root, relative string, entry TreeEntry) (os.FileInfo, bool) {
	file, info, err := openRegularFileBeneath(root, relative)
	if err != nil {
		return nil, false
	}
	defer file.Close()
	if info.Size() != entry.Size || uint32(info.Mode().Perm()) != entry.Mode || info.ModTime().UnixNano() != entry.ModTimeNS {
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

func fileMatchesManifestChunksBeneath(ctx context.Context, root, relative string, entry TreeEntry) (bool, os.FileInfo, error) {
	file, before, err := openRegularFileBeneath(root, relative)
	if err != nil {
		return false, nil, err
	}
	defer file.Close()
	if before.Size() != entry.Size || uint32(before.Mode().Perm()) != entry.Mode || before.ModTime().UnixNano() != entry.ModTimeNS {
		return false, nil, nil
	}
	beforeChangeID := fileChangeID(before)
	chunks, err := splitFileFromOpen(ctx, file, before, filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		return false, nil, err
	}
	after, err := file.Stat()
	if err != nil {
		return false, nil, err
	}
	pathFile, pathInfo, err := openRegularFileBeneath(root, relative)
	if err != nil {
		return false, nil, err
	}
	pathCloseErr := pathFile.Close()
	if pathCloseErr != nil {
		return false, nil, pathCloseErr
	}
	if !after.Mode().IsRegular() || !os.SameFile(before, after) || !os.SameFile(before, pathInfo) ||
		after.Size() != entry.Size || uint32(after.Mode().Perm()) != entry.Mode || after.ModTime().UnixNano() != entry.ModTimeNS ||
		(beforeChangeID != "" && beforeChangeID != fileChangeID(after)) {
		return false, nil, errIncrementalTreeChanged
	}
	return sameChunks(chunks, entry.Chunks), after, nil
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

func stagingPathError(operation, rel string, err error) error {
	return fmt.Errorf("%s staging path %q: %w", operation, rel, err)
}

func createStageTempDir(stage string, manifest *SnapshotManifest) (string, error) {
	for {
		dir, err := os.MkdirTemp(stage, ".replication-tmp-*")
		if err != nil {
			return "", err
		}
		rel := filepath.ToSlash(filepath.Base(dir))
		conflicts := false
		for _, entry := range manifest.Files {
			if entry.Path == rel || strings.HasPrefix(entry.Path, rel+"/") {
				conflicts = true
				break
			}
		}
		if !conflicts {
			return dir, nil
		}
		if err := os.Remove(dir); err != nil {
			return "", fmt.Errorf("remove conflicting staging temporary directory %q: %w", rel, err)
		}
	}
}

func stageDirectoryHasPrivateWritableMode(info os.FileInfo) bool {
	const specialModeBits = os.ModeSetuid | os.ModeSetgid | os.ModeSticky
	return info.Mode().Perm() == 0o700 && info.Mode()&specialModeBits == 0
}

func pruneUnexpectedStageEntries(ctx context.Context, stage string, manifest *SnapshotManifest) error {
	expected := make(map[string]struct{}, len(manifest.Files))
	for entryIndex, entry := range manifest.Files {
		if entryIndex&0xff == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		expected[entry.Path] = struct{}{}
	}
	removed := 0
	err := filepath.Walk(stage, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if path == stage {
			return nil
		}
		rel, err := filepath.Rel(stage, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if _, ok := expected[rel]; !ok {
			if info.IsDir() {
				if err := makeTreeRemovable(ctx, path); err != nil {
					return fmt.Errorf("make unexpected staging tree %q removable: %w", rel, err)
				}
			}
			if err := os.RemoveAll(path); err != nil {
				return fmt.Errorf("remove unexpected staging entry %q: %w", rel, err)
			}
			removed++
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.IsDir() && !stageDirectoryHasPrivateWritableMode(info) {
			return os.Chmod(path, 0o700)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if removed > 0 {
		log.Info("Removed unexpected entries from resumed staging tree: snapshot=%s entries=%d", manifest.ID, removed)
	}
	return nil
}

const stageChunkMemoryCacheLimit = 16 << 20

type stageChunkMemoryCacheEntry struct {
	hash string
	data []byte
}

type stageChunkMemoryCache struct {
	maxBytes int64
	used     int64
	entries  map[string]*list.Element
	lru      list.List
}

func newStageChunkMemoryCache(maxBytes int64) *stageChunkMemoryCache {
	return &stageChunkMemoryCache{maxBytes: maxBytes, entries: make(map[string]*list.Element)}
}

func (c *stageChunkMemoryCache) get(hash string, size int64) []byte {
	element := c.entries[hash]
	if element == nil {
		return nil
	}
	entry := element.Value.(*stageChunkMemoryCacheEntry)
	if int64(len(entry.data)) != size {
		c.used -= int64(len(entry.data))
		delete(c.entries, hash)
		c.lru.Remove(element)
		return nil
	}
	c.lru.MoveToFront(element)
	return entry.data
}

func (c *stageChunkMemoryCache) add(hash string, data []byte) {
	if c.maxBytes <= 0 || int64(len(data)) > c.maxBytes {
		return
	}
	if element := c.entries[hash]; element != nil {
		entry := element.Value.(*stageChunkMemoryCacheEntry)
		c.used -= int64(len(entry.data))
		c.lru.Remove(element)
		delete(c.entries, hash)
	}
	for c.used+int64(len(data)) > c.maxBytes {
		element := c.lru.Back()
		if element == nil {
			break
		}
		entry := element.Value.(*stageChunkMemoryCacheEntry)
		c.used -= int64(len(entry.data))
		delete(c.entries, entry.hash)
		c.lru.Remove(element)
	}
	entry := &stageChunkMemoryCacheEntry{hash: hash, data: data}
	c.entries[hash] = c.lru.PushFront(entry)
	c.used += int64(len(data))
}

func buildIncrementalStage(ctx context.Context, root, stage, cacheDir string, manifest, previous *SnapshotManifest, fetch func(string) ([]byte, error), localChunkCandidates ...map[string][]chunkLocation) (retErr error) {
	stageStarted := time.Now()
	if err := ctx.Err(); err != nil {
		return err
	}
	var cacheShards sync.Map
	defer func() {
		if err := syncChunkCacheDirectories(cacheDir, manifest.ID, "staging-fetch", &cacheShards); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("persist on-demand replication chunk cache: %w", err))
		}
	}()
	var inferredZeroHashes map[string]struct{}
	if manifest.FormatVersion == legacyIncrementalFormatVersion {
		var err error
		inferredZeroHashes, err = manifestZeroChunkHashes(ctx, manifest)
		if err != nil {
			return err
		}
	}
	stageInfo, err := os.Lstat(stage)
	if err != nil {
		return fmt.Errorf("inspect incremental staging directory %q: %w", stage, err)
	}
	if !stageInfo.IsDir() || stageInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("incremental staging path %q must be a real directory", stage)
	}
	if !stageDirectoryHasPrivateWritableMode(stageInfo) {
		if err := os.Chmod(stage, 0o700); err != nil {
			return fmt.Errorf("make incremental staging root writable: %w", err)
		}
	}
	if err := pruneUnexpectedStageEntries(ctx, stage, manifest); err != nil {
		return err
	}
	fileCount, identitiesRecorded := 0, 0
	stagedFilesReused, stagedFilesCloned, stagedFilesCopied := 0, 0, 0
	sourceFilesCloned, sourceFilesCopied, filesRebuilt := 0, 0, 0
	cacheChunks, localChunks, fetchedChunks := 0, 0, 0
	zeroChunksSkipped := 0
	localCandidateAttempts, localCandidateFailures, memoryCacheChunks := 0, 0, 0
	previousManifestCandidateAttempts, previousManifestCandidateFailures := 0, 0
	var cacheBytes, localBytes, fetchedBytes, baselineCopyBytes, memoryCacheBytes, zeroBytesSkipped int64
	chunkMemoryCache := newStageChunkMemoryCache(stageChunkMemoryCacheLimit)
	directories := make([]TreeEntry, 0)
	for i := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry := &manifest.Files[i]
		entry.LocalChangeID = ""
		switch entry.Type {
		case "file":
			fileCount++
		case "dir":
			directories = append(directories, *entry)
		}
	}
	var progressFilesStarted, progressFilesCompleted, progressChunksProcessed, progressPayloadBytesProcessed atomic.Int64
	var progressMemoryCacheBytes, progressCacheBytes, progressLocalBytes, progressFetchedBytes atomic.Int64
	stopProgress := startPeriodicProgressLog(func() {
		log.Info("Incremental staging progress: snapshot=%s files_started=%d files_completed=%d/%d chunks_processed=%d chunk_payload_bytes_processed=%d memory_cache_payload_bytes=%d cache_payload_bytes=%d local_baseline_payload_bytes=%d fetched_payload_bytes=%d elapsed=%s", manifest.ID, progressFilesStarted.Load(), progressFilesCompleted.Load(), fileCount, progressChunksProcessed.Load(), progressPayloadBytesProcessed.Load(), progressMemoryCacheBytes.Load(), progressCacheBytes.Load(), progressLocalBytes.Load(), progressFetchedBytes.Load(), time.Since(stageStarted))
	})
	defer stopProgress()
	captureIdentity := func(entryIndex int, info os.FileInfo) {
		if setLocalChangeID(&manifest.Files[entryIndex], info) {
			identitiesRecorded++
		}
	}
	stageTempDir, err := createStageTempDir(stage, manifest)
	if err != nil {
		return fmt.Errorf("create incremental staging temporary directory: %w", err)
	}
	stageTempDirOwned := true
	defer func() {
		if stageTempDirOwned {
			if err := os.RemoveAll(stageTempDir); err != nil {
				log.Warn("Remove incomplete staging temporary directory failed: snapshot=%s path=%s error=%v", manifest.ID, filepath.Base(stageTempDir), err)
			}
		}
	}()
	// Keep directories writable while their children are reconstructed;
	// the manifest permissions are restored after the complete tree exists.
	slices.SortFunc(directories, func(a, b TreeEntry) int {
		depthA, depthB := strings.Count(a.Path, "/"), strings.Count(b.Path, "/")
		if depthA != depthB {
			return depthA - depthB
		}
		return strings.Compare(a.Path, b.Path)
	})
	for _, entry := range directories {
		if err := ctx.Err(); err != nil {
			return err
		}
		dst := filepath.Join(stage, filepath.FromSlash(entry.Path))
		info, err := os.Lstat(dst)
		if err == nil {
			if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
				if !stageDirectoryHasPrivateWritableMode(info) {
					if err := os.Chmod(dst, 0o700); err != nil {
						return stagingPathError("make directory writable", entry.Path, err)
					}
				}
				continue
			}
			if err := os.RemoveAll(dst); err != nil {
				return stagingPathError("remove conflicting directory entry", entry.Path, err)
			}
		} else if !os.IsNotExist(err) {
			return stagingPathError("inspect directory", entry.Path, err)
		}
		if err := os.Mkdir(dst, 0o700); err != nil {
			return stagingPathError("create directory", entry.Path, err)
		}
		if err := os.Chmod(dst, 0o700); err != nil {
			return stagingPathError("make directory writable", entry.Path, err)
		}
	}
	// Manifest topology was validated before staging, so all entry parents now exist.
	var oldEntries map[string]TreeEntry
	oldChunks := map[string]chunkLocation{}
	oldChunkAlternates := map[string][]chunkLocation{}
	oldChunksIndexed := previous == nil
	var localChunkLocations map[string][]chunkLocation
	if len(localChunkCandidates) > 0 {
		localChunkLocations = localChunkCandidates[0]
	}
	var resolvedRoot string
	verifyLocal := previous != nil && !manifest.FullScanAt.IsZero() && manifest.FullScanAt.After(previous.FullScanAt)
	if previous != nil {
		resolvedRoot, err = resolvedPath(root)
		if err != nil {
			return fmt.Errorf("resolve source data root %q: %w", root, err)
		}
		oldEntries = make(map[string]TreeEntry, len(previous.Files))
		for _, entry := range previous.Files {
			if err := ctx.Err(); err != nil {
				return err
			}
			oldEntries[entry.Path] = entry
		}
	}
	for entryIndex, entry := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		dst := filepath.Join(stage, filepath.FromSlash(entry.Path))
		switch entry.Type {
		case "dir":
			continue
		case "symlink":
			if target, err := os.Readlink(dst); err == nil && target == entry.LinkTarget {
				continue
			}
			if err := os.RemoveAll(dst); err != nil {
				return stagingPathError("remove conflicting symlink entry", entry.Path, err)
			}
			if err := os.Symlink(entry.LinkTarget, dst); err != nil {
				return stagingPathError("create symlink", entry.Path, err)
			}
		case "file":
			progressFilesStarted.Add(1)
			matches, info, err := fileMatchesManifestChunksWithInfo(ctx, dst, entry)
			if err == nil && matches {
				if fileHasMultipleLinks(info) {
					cloned, copied, detachedInfo, detachErr := materializeBaselineFile(ctx, stage, entry.Path, info, dst, entry)
					if detachErr != nil {
						if ctx.Err() != nil {
							return ctx.Err()
						}
						matches = false
					} else {
						info = detachedInfo
						if cloned {
							stagedFilesCloned++
						} else {
							stagedFilesCopied++
							baselineCopyBytes += copied
						}
					}
				}
			}
			if err == nil && matches {
				captureIdentity(entryIndex, info)
				stagedFilesReused++
				progressFilesCompleted.Add(1)
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			old := oldEntries[entry.Path]
			if sameFile(entry, old) {
				sourceInfo, reusable := reusableWholeFileInfoBeneath(resolvedRoot, entry.Path, entry)
				if reusable {
					matches := !verifyLocal && old.LocalChangeID != "" && old.LocalChangeID == fileChangeID(sourceInfo)
					if !matches {
						var err error
						matches, sourceInfo, err = fileMatchesManifestChunksBeneath(ctx, resolvedRoot, entry.Path, entry)
						if err != nil && ctx.Err() != nil {
							return ctx.Err()
						}
					}
					if matches {
						cloned, copied, stagedInfo, materializeErr := materializeBaselineFile(ctx, resolvedRoot, entry.Path, sourceInfo, dst, entry)
						if materializeErr == nil {
							captureIdentity(entryIndex, stagedInfo)
							if cloned {
								sourceFilesCloned++
							} else {
								sourceFilesCopied++
								baselineCopyBytes += copied
							}
							progressFilesCompleted.Add(1)
							continue
						}
						if ctx.Err() != nil {
							return ctx.Err()
						}
					}
				}
			}
			if info, err := os.Lstat(dst); err == nil && info.IsDir() {
				if err := os.RemoveAll(dst); err != nil {
					return stagingPathError("remove conflicting file entry", entry.Path, err)
				}
			} else if err != nil && !os.IsNotExist(err) {
				return stagingPathError("inspect file destination", entry.Path, err)
			}
			tmp := filepath.Join(stageTempDir, strconv.Itoa(entryIndex))
			out, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(entry.Mode))
			if err != nil {
				return stagingPathError("create temporary file", entry.Path, err)
			}
			for _, chunk := range entry.Chunks {
				if chunkIsZero(chunk, inferredZeroHashes) {
					if err := ctx.Err(); err != nil {
						_ = out.Close()
						return err
					}
					if _, err := out.Seek(chunk.Size, io.SeekCurrent); err != nil {
						_ = out.Close()
						return stagingPathError("create sparse file extent", entry.Path, err)
					}
					zeroChunksSkipped++
					zeroBytesSkipped += chunk.Size
					progressChunksProcessed.Add(1)
					progressPayloadBytesProcessed.Add(chunk.Size)
					continue
				}
				var data []byte
				chunkSource := ""
				if cached := chunkMemoryCache.get(chunk.Hash, chunk.Size); cached != nil {
					data = cached
					chunkSource = "memory"
				} else {
					if cached, err := readCachedChunkForBatch(cacheDir, chunk.Hash, &cacheShards); err == nil {
						data = cached
						if data != nil {
							chunkSource = "cache"
						}
					} else if !errors.Is(err, os.ErrNotExist) {
						log.Debug("Cached replication chunk unavailable during staging; trying local or remote source: snapshot=%s hash=%s error=%v", manifest.ID, chunk.Hash, err)
					}
				}
				if data == nil {
					for _, location := range localChunkLocations[chunk.Hash] {
						localCandidateAttempts++
						data, err = readChunkFromRoot(root, resolvedRoot, location, chunk.Hash)
						if err == nil {
							chunkSource = "local"
							log.Debug("Reused indexed local replication chunk: snapshot=%s hash=%s path=%s", manifest.ID, chunk.Hash, location.Path)
							break
						}
						localCandidateFailures++
						log.Debug("Indexed local replication chunk candidate failed during staging: snapshot=%s hash=%s path=%s error=%v", manifest.ID, chunk.Hash, location.Path, err)
					}
				}
				if data == nil {
					if previous != nil && !oldChunksIndexed {
						oldChunks, oldChunkAlternates, err = indexManifestWithAlternatesContext(ctx, previous)
						if err != nil {
							_ = out.Close()
							return fmt.Errorf("index previous manifest chunks: %w", err)
						}
						oldChunksIndexed = true
					}
					if location, ok := oldChunks[chunk.Hash]; ok {
						previousManifestCandidateAttempts++
						var err error
						data, err = readChunkFromRoot(root, resolvedRoot, location, chunk.Hash)
						if err != nil {
							previousManifestCandidateFailures++
							failedPath := location.Path
							for _, alternate := range oldChunkAlternates[chunk.Hash] {
								previousManifestCandidateAttempts++
								data, err = readChunkFromRoot(root, resolvedRoot, alternate, chunk.Hash)
								if err == nil {
									log.Debug("Reused replication chunk from alternate local manifest location: snapshot=%s hash=%s path=%s", manifest.ID, chunk.Hash, alternate.Path)
									break
								}
								previousManifestCandidateFailures++
								failedPath = alternate.Path
							}
							if err != nil {
								log.Debug("Local replication chunk candidates failed during staging; fetching fallback: snapshot=%s hash=%s path=%s error=%v", manifest.ID, chunk.Hash, failedPath, err)
								data = nil
							}
						}
						if err == nil && data != nil {
							chunkSource = "local"
						}
					}
				}
				if data == nil {
					data, _, err = fetchChunkWithIntegrityRetry(ctx, manifest.ID, chunk.Hash, chunk.Size,
						func() ([]byte, int64, error) {
							data, err := fetch(chunk.Hash)
							return data, -1, err
						},
						func(data []byte) error {
							if err := storeChunkForBatch(cacheDir, chunk.Hash, data); err != nil {
								return err
							}
							cacheShards.LoadOrStore(chunk.Hash[:2], struct{}{})
							return nil
						},
					)
					if err == nil {
						chunkSource = "remote"
					}
					if err != nil {
						_ = out.Close()
						return fmt.Errorf("restore staging file %q from chunk %s: %w", entry.Path, chunk.Hash, err)
					}
				}
				if int64(len(data)) != chunk.Size {
					_ = out.Close()
					return fmt.Errorf("staging file %q chunk %s size mismatch: got %d want %d", entry.Path, chunk.Hash, len(data), chunk.Size)
				}
				switch chunkSource {
				case "memory":
					memoryCacheChunks++
					memoryCacheBytes += int64(len(data))
					progressMemoryCacheBytes.Add(int64(len(data)))
				case "cache":
					cacheChunks++
					cacheBytes += int64(len(data))
					progressCacheBytes.Add(int64(len(data)))
				case "local":
					localChunks++
					localBytes += int64(len(data))
					progressLocalBytes.Add(int64(len(data)))
				case "remote":
					fetchedChunks++
					fetchedBytes += int64(len(data))
					progressFetchedBytes.Add(int64(len(data)))
				}
				if chunkSource != "memory" {
					chunkMemoryCache.add(chunk.Hash, data)
				}
				// Current manifests identify zero chunks; only legacy manifests need a content scan.
				if manifest.FormatVersion < zeroChunkFormatVersion && isZeroChunk(data) {
					if _, err := out.Seek(int64(len(data)), io.SeekCurrent); err != nil {
						_ = out.Close()
						return stagingPathError("create sparse file extent", entry.Path, err)
					}
					zeroChunksSkipped++
					zeroBytesSkipped += int64(len(data))
				} else if _, err := out.Write(data); err != nil {
					_ = out.Close()
					return stagingPathError("write file data", entry.Path, err)
				}
				progressChunksProcessed.Add(1)
				progressPayloadBytesProcessed.Add(int64(len(data)))
			}
			if err := out.Truncate(entry.Size); err != nil {
				_ = out.Close()
				return stagingPathError("set file size", entry.Path, err)
			}
			if err := out.Chmod(os.FileMode(entry.Mode)); err != nil {
				_ = out.Close()
				return stagingPathError("restore file permissions", entry.Path, err)
			}
			mtime := time.Unix(0, entry.ModTimeNS)
			if err := os.Chtimes(tmp, mtime, mtime); err != nil {
				_ = out.Close()
				return stagingPathError("restore file timestamps", entry.Path, err)
			}
			if err := out.Sync(); err != nil {
				_ = out.Close()
				return stagingPathError("sync file", entry.Path, err)
			}
			if err := out.Close(); err != nil {
				return stagingPathError("close file", entry.Path, err)
			}
			if err := os.Rename(tmp, dst); err != nil {
				return stagingPathError("activate temporary file", entry.Path, err)
			}
			if info, err := os.Lstat(dst); err == nil {
				captureIdentity(entryIndex, info)
			}
			filesRebuilt++
			progressFilesCompleted.Add(1)
		}
	}
	if err := os.RemoveAll(stageTempDir); err != nil {
		return fmt.Errorf("remove incremental staging temporary directory %q: %w", filepath.Base(stageTempDir), err)
	}
	stageTempDirOwned = false
	for _, entry := range slices.Backward(directories) {
		dst := filepath.Join(stage, filepath.FromSlash(entry.Path))
		if err := os.Chmod(dst, os.FileMode(entry.Mode)); err != nil {
			return stagingPathError("restore directory permissions", entry.Path, err)
		}
		mtime := time.Unix(0, entry.ModTimeNS)
		if err := os.Chtimes(dst, mtime, mtime); err != nil {
			return stagingPathError("restore directory timestamps", entry.Path, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Chmod(stage, os.FileMode(manifest.RootMode)); err != nil {
		return fmt.Errorf("restore staging root permissions: %w", err)
	}
	stopProgress()
	if identitiesRecorded == fileCount {
		log.Info("Captured staging file identities: snapshot=%s files=%d", manifest.ID, identitiesRecorded)
	} else {
		log.Warn("Could not capture all staging file identities: snapshot=%s recorded=%d total=%d", manifest.ID, identitiesRecorded, fileCount)
	}
	log.Info("Built incremental staging tree: snapshot=%s files_stage_reused=%d stage_files_reflinked=%d stage_files_copied=%d files_reflinked_from_baseline=%d files_copied_from_baseline=%d baseline_copy_bytes=%d files_rebuilt=%d zero_chunks_skipped=%d zero_bytes_skipped=%d chunks_from_memory_cache=%d memory_cache_payload_bytes=%d chunks_from_cache=%d cache_payload_bytes=%d chunks_from_local_baseline=%d local_payload_bytes=%d indexed_local_candidate_attempts=%d indexed_local_candidate_failures=%d previous_manifest_candidate_attempts=%d previous_manifest_candidate_failures=%d chunks_fetched_on_demand=%d fetched_payload_bytes=%d duration=%s", manifest.ID, stagedFilesReused, stagedFilesCloned, stagedFilesCopied, sourceFilesCloned, sourceFilesCopied, baselineCopyBytes, filesRebuilt, zeroChunksSkipped, zeroBytesSkipped, memoryCacheChunks, memoryCacheBytes, cacheChunks, cacheBytes, localChunks, localBytes, localCandidateAttempts, localCandidateFailures, previousManifestCandidateAttempts, previousManifestCandidateFailures, fetchedChunks, fetchedBytes, time.Since(stageStarted))
	return nil
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

func verifyRestoredStandbyTree(ctx context.Context, root string, manifest *SnapshotManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	base := &SnapshotManifest{Files: manifest.Files, FullScanAt: manifest.FullScanAt}
	local, err := scanInstalledTreeAgainstManifest(ctx, root, base, manifest)
	if err != nil {
		return fmt.Errorf("verify restored standby tree: %w", err)
	}
	var files, identityMatches, localIdentities int
	for i := range manifest.Files {
		if manifest.Files[i].Type == "file" {
			files++
			if local.Files[i].ChangeID != "" {
				localIdentities++
			}
			if manifest.Files[i].LocalChangeID != "" && manifest.Files[i].LocalChangeID == local.Files[i].ChangeID {
				identityMatches++
			}
		}
		manifest.Files[i].LocalChangeID = local.Files[i].ChangeID
	}
	log.Info("Verified restored standby tree after readiness: snapshot=%s files=%d identity_matches=%d content_hashed=%d local_identities=%d", manifest.ID, files, identityMatches, files-identityMatches, localIdentities)
	return nil
}

func signRestoredStandbyManifest(ctx context.Context, manifest *SnapshotManifest, token string) error {
	manifest.State = "ready"
	manifest.Error = ""
	if err := signIncrementalManifestContext(ctx, manifest, token); err != nil {
		return err
	}
	if err := writeManifestJSONContext(ctx, io.Discard, manifest, false); errors.Is(err, errManifestTooLarge) {
		for i := range manifest.Files {
			manifest.Files[i].LocalChangeID = ""
		}
		log.Warn("Omit local standby file identities because the ready manifest exceeds its size limit: snapshot=%s", manifest.ID)
		if err := signIncrementalManifestContext(ctx, manifest, token); err != nil {
			return err
		}
		return writeManifestJSONContext(ctx, io.Discard, manifest, false)
	} else if err != nil {
		return err
	}
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
	cleanupCompletedInstallStage(cfg, manifest.ID)
	if err := removeStandbyFinalizeRequestCheckpoint(cfg.SnapshotDir); err != nil {
		log.Warn("Remove completed standby final request checkpoint: snapshot=%s error=%v", manifest.ID, err)
	}
	if err := removeStandbyPreflightRequestCheckpoint(cfg.SnapshotDir); err != nil {
		log.Warn("Remove completed standby preflight request checkpoint: snapshot=%s error=%v", manifest.ID, err)
	}
	if err := removeFileSynced(stageCheckpointPath(cfg)); err != nil {
		log.Warn("Remove completed staging checkpoint: snapshot=%s error=%v", manifest.ID, err)
	}
	if err := pruneFailedRestoreStages(cfg.SnapshotDir, 0); err != nil {
		log.Warn("Remove failed standby restore stages after successful sync: snapshot=%s error=%v", manifest.ID, err)
	}
	pruneManifestFiles(cfg.SnapshotDir, cfg.SnapshotRetention, cfg.ControlToken)
	if err := os.RemoveAll(cacheDir); err != nil {
		log.Warn("Remove completed incremental cache: snapshot=%s error=%v", manifest.ID, err)
	} else if err := syncDirectory(filepath.Dir(cacheDir)); err != nil {
		log.Warn("Persist completed incremental cache removal: snapshot=%s error=%v", manifest.ID, err)
	}
	return nil
}

func cleanupCompletedInstallStage(cfg *config, snapshotID string) {
	stage := installStagePath(cfg)
	if _, err := os.Lstat(stage); os.IsNotExist(err) {
		return
	} else if err != nil {
		log.Warn("Inspect completed standby install stage failed: snapshot=%s error=%v", snapshotID, err)
		return
	}
	if err := leaveStageWorkingDirectory(stage); err != nil {
		log.Warn("Cannot leave completed standby install stage; retaining backup: snapshot=%s stage=%s error=%v", snapshotID, filepath.Base(stage), err)
		return
	}
	cleanupErr := errors.Join(makeTreeRemovable(context.Background(), stage), cleanupBackup(stage), syncDirectory(cfg.SnapshotDir))
	if cleanupErr != nil {
		log.Warn("Remove completed standby install stage failed: snapshot=%s stage=%s error=%v", snapshotID, filepath.Base(stage), cleanupErr)
	} else {
		log.Info("Removed completed standby install stage: snapshot=%s", snapshotID)
	}
}

type stageCheckpoint struct {
	Version             int               `json:"version,omitempty"`
	SnapshotID          string            `json:"snapshot_id"`
	ManifestSHA         string            `json:"manifest_sha"`
	RenewalIDs          map[string]string `json:"renewal_ids,omitempty"`
	StandbyWasActive    *bool             `json:"standby_was_active,omitempty"`
	StandbyReady        bool              `json:"standby_ready,omitempty"`
	SwitchPrepared      bool              `json:"switch_prepared,omitempty"`
	RollbackKeysPending bool              `json:"rollback_keys_pending,omitempty"`
	RootDevice          uint64            `json:"root_device,omitempty"`
	RootInode           uint64            `json:"root_inode,omitempty"`
	StageDevice         uint64            `json:"stage_device,omitempty"`
	StageInode          uint64            `json:"stage_inode,omitempty"`
}

const (
	stageCheckpointVersion = 1
	maxStageCheckpointSize = 4 << 10
	stageCheckpointName    = ".install-stage.checkpoint"
)

func stageCheckpointPath(cfg *config) string {
	return filepath.Join(cfg.SnapshotDir, stageCheckpointName)
}

func readStageCheckpoint(path string) (stageCheckpoint, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return stageCheckpoint{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxStageCheckpointSize {
		return stageCheckpoint{}, errors.New("invalid staging checkpoint file")
	}
	file, _, err := openRegularFile(path, info)
	if err != nil {
		return stageCheckpoint{}, fmt.Errorf("open staging checkpoint: %w", err)
	}
	defer file.Close()
	var checkpoint stageCheckpoint
	if err := decodeBoundedJSON(file, maxStageCheckpointSize, &checkpoint); err != nil {
		return stageCheckpoint{}, err
	}
	if checkpoint.Version < 0 || checkpoint.Version > stageCheckpointVersion {
		return stageCheckpoint{}, fmt.Errorf("unsupported standby install checkpoint version %d", checkpoint.Version)
	}
	return checkpoint, nil
}

func persistedStageRenewalID(cfg *config, snapshot *Snapshot, phase string) (string, error) {
	switch phase {
	case "durable_staging", "before_service_start", "before_readiness":
	default:
		return "", fmt.Errorf("invalid final sync renewal phase %q", phase)
	}
	path := stageCheckpointPath(cfg)
	checkpoint, err := readStageCheckpoint(path)
	if err != nil {
		return "", fmt.Errorf("read final sync renewal checkpoint: %w", err)
	}
	if checkpoint.SnapshotID != snapshot.ID || checkpoint.ManifestSHA != snapshot.SHA256 {
		return "", errors.New("final sync renewal checkpoint does not match the snapshot")
	}
	seenIDs := make(map[string]string, len(checkpoint.RenewalIDs))
	for existingPhase, existingID := range checkpoint.RenewalIDs {
		switch existingPhase {
		case "durable_staging", "before_service_start", "before_readiness":
		default:
			return "", fmt.Errorf("invalid final sync renewal checkpoint phase %q", existingPhase)
		}
		if !validReplicationRequestID(existingID) {
			return "", fmt.Errorf("invalid final sync renewal checkpoint ID for phase %q", existingPhase)
		}
		if previousPhase := seenIDs[existingID]; previousPhase != "" {
			return "", fmt.Errorf("final sync renewal checkpoint reuses an ID for phases %q and %q", previousPhase, existingPhase)
		}
		seenIDs[existingID] = existingPhase
	}
	if renewalID := checkpoint.RenewalIDs[phase]; renewalID != "" {
		log.Debug("Reused persisted final sync session renewal ID: snapshot=%s phase=%s", snapshot.ID, phase)
		return renewalID, nil
	}
	renewalID, err := newSessionRenewalID()
	if err != nil {
		return "", err
	}
	if checkpoint.RenewalIDs == nil {
		checkpoint.RenewalIDs = make(map[string]string, 3)
	}
	checkpoint.RenewalIDs[phase] = renewalID
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return "", fmt.Errorf("encode final sync renewal checkpoint: %w", err)
	}
	if err := writeFileSynced(path, data, 0o600); err != nil {
		return "", fmt.Errorf("persist final sync renewal checkpoint: %w", err)
	}
	log.Debug("Persisted final sync session renewal ID: snapshot=%s phase=%s", snapshot.ID, phase)
	return renewalID, nil
}

func setStageCheckpointStandbyReady(cfg *config, snapshot *Snapshot, ready, required bool) error {
	path := stageCheckpointPath(cfg)
	checkpoint, err := readStageCheckpoint(path)
	if os.IsNotExist(err) {
		if required {
			return fmt.Errorf("standby install checkpoint %q is required: %w", path, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if checkpoint.SnapshotID != snapshot.ID || checkpoint.ManifestSHA != snapshot.SHA256 {
		return errors.New("staging checkpoint does not match the snapshot being installed")
	}
	if checkpoint.StandbyReady == ready {
		return nil
	}
	checkpoint.StandbyReady = ready
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return writeFileSynced(path, data, 0o600)
}

func setStageCheckpointStandbyWasActive(cfg *config, snapshot *Snapshot, wasActive, required bool) error {
	path := stageCheckpointPath(cfg)
	checkpoint, err := readStageCheckpoint(path)
	if os.IsNotExist(err) {
		if required {
			return fmt.Errorf("standby install checkpoint %q is required: %w", path, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if checkpoint.SnapshotID != snapshot.ID || checkpoint.ManifestSHA != snapshot.SHA256 {
		return errors.New("staging checkpoint does not match the snapshot being installed")
	}
	active := wasActive
	checkpoint.Version = stageCheckpointVersion
	checkpoint.StandbyWasActive = &active
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return writeFileSynced(path, data, 0o600)
}

func restoreCheckpointedStandbyService(ctx context.Context, cfg *config, shouldBeActive bool) error {
	serviceCtx, cancel := context.WithTimeout(ctx, cfg.ServiceTimeout)
	defer cancel()
	if err := ensureSocketActivationDisabled(serviceCtx, cfg.GiteaServiceName); err != nil {
		return fmt.Errorf("disable standby socket activation before service recovery: %w", err)
	}
	active, err := systemctlUnitActive(serviceCtx, cfg.GiteaServiceName)
	if err != nil {
		return fmt.Errorf("inspect standby service before recovery: %w", err)
	}
	if active == shouldBeActive {
		if shouldBeActive {
			if err := readinessCheck(serviceCtx, cfg.GiteaServiceName); err != nil {
				return fmt.Errorf("verify standby readiness after interrupted install: %w", err)
			}
		}
		return nil
	}
	if !shouldBeActive {
		if err := systemctl(serviceCtx, "stop", cfg.GiteaServiceName); err != nil {
			return fmt.Errorf("stop standby service after interrupted install: %w", err)
		}
		active, err = systemctlUnitActive(serviceCtx, cfg.GiteaServiceName)
		if err != nil {
			return fmt.Errorf("verify standby service stopped after interrupted install: %w", err)
		}
		if active {
			return errors.New("standby service remained active after interrupted install recovery")
		}
		log.Info("Restored standby service state after interrupted install: service=%s active=false", cfg.GiteaServiceName)
		return nil
	}
	if err := ensureStandbyService(serviceCtx, cfg); err != nil {
		return fmt.Errorf("restart standby service after interrupted install: %w", err)
	}
	log.Info("Restored standby service state after interrupted install: service=%s active=true", cfg.GiteaServiceName)
	return nil
}

func checkpointedStandbyActive(checkpoint stageCheckpoint, observed bool) bool {
	if checkpoint.StandbyWasActive != nil {
		return *checkpoint.StandbyWasActive
	}
	return observed
}

func setStageCheckpointSwitchPrepared(cfg *config, snapshot *Snapshot, root, stage string, required bool) error {
	path := stageCheckpointPath(cfg)
	checkpoint, err := readStageCheckpoint(path)
	if os.IsNotExist(err) {
		if required {
			return fmt.Errorf("standby install checkpoint %q is required: %w", path, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if checkpoint.SnapshotID != snapshot.ID || checkpoint.ManifestSHA != snapshot.SHA256 {
		return errors.New("staging checkpoint does not match the snapshot being installed")
	}
	rootDevice, rootInode, err := directoryIdentity(root)
	if err != nil {
		return fmt.Errorf("identify standby data root: %w", err)
	}
	stageDevice, stageInode, err := directoryIdentity(stage)
	if err != nil {
		return fmt.Errorf("identify install stage: %w", err)
	}
	checkpoint.SwitchPrepared = true
	checkpoint.RootDevice, checkpoint.RootInode = rootDevice, rootInode
	checkpoint.StageDevice, checkpoint.StageInode = stageDevice, stageInode
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return writeFileSynced(path, data, 0o600)
}

func setStageCheckpointRollbackKeysPending(cfg *config, snapshot *Snapshot, pending, required bool) error {
	path := stageCheckpointPath(cfg)
	checkpoint, err := readStageCheckpoint(path)
	if os.IsNotExist(err) {
		if required {
			return fmt.Errorf("standby install checkpoint %q is required: %w", path, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if checkpoint.SnapshotID != snapshot.ID || checkpoint.ManifestSHA != snapshot.SHA256 {
		return errors.New("staging checkpoint does not match the snapshot being recovered")
	}
	if checkpoint.RollbackKeysPending == pending {
		return nil
	}
	checkpoint.RollbackKeysPending = pending
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return writeFileSynced(path, data, 0o600)
}

type installSwitchState uint8

const (
	installSwitchUnknown installSwitchState = iota
	installSwitchPrepared
	installSwitchExchanged
)

func checkpointInstallSwitchState(checkpoint stageCheckpoint, root, stage string) (installSwitchState, error) {
	if !checkpoint.SwitchPrepared {
		return installSwitchUnknown, nil
	}
	if checkpoint.RootInode == 0 || checkpoint.StageInode == 0 {
		return installSwitchUnknown, errors.New("install checkpoint has incomplete directory identities")
	}
	rootIsOriginal := pathHasDirectoryIdentity(root, checkpoint.RootDevice, checkpoint.RootInode)
	rootIsStage := pathHasDirectoryIdentity(root, checkpoint.StageDevice, checkpoint.StageInode)
	stageIsOriginal := pathHasDirectoryIdentity(stage, checkpoint.RootDevice, checkpoint.RootInode)
	stageIsStage := pathHasDirectoryIdentity(stage, checkpoint.StageDevice, checkpoint.StageInode)
	if rootIsOriginal && stageIsStage {
		return installSwitchPrepared, nil
	}
	if rootIsStage && stageIsOriginal {
		return installSwitchExchanged, nil
	}
	return installSwitchUnknown, nil
}

func exchangeBackUnreadyInstall(ctx context.Context, cfg *config, stage string, checkpoint stageCheckpoint) (bool, os.FileInfo, error) {
	root := filepath.Clean(setting.AppWorkPath)
	snapshot := &Snapshot{ID: checkpoint.SnapshotID, SHA256: checkpoint.ManifestSHA}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return false, nil, fmt.Errorf("inspect installed standby root before recovery: %w", err)
	}
	stageInfo, err := os.Lstat(stage)
	if err != nil {
		return false, nil, fmt.Errorf("inspect standby backup before recovery: %w", err)
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 || !stageInfo.IsDir() || stageInfo.Mode()&os.ModeSymlink != 0 {
		return false, nil, errors.New("unready install recovery requires real root and backup directories")
	}
	checkpoint, err = readStageCheckpoint(stageCheckpointPath(cfg))
	if err != nil {
		return false, nil, fmt.Errorf("reload standby install checkpoint before recovery: %w", err)
	}
	if checkpoint.SnapshotID != snapshot.ID || checkpoint.ManifestSHA != snapshot.SHA256 {
		return false, nil, errors.New("standby install checkpoint changed before recovery")
	}
	if !checkpoint.SwitchPrepared {
		rootDevice, rootInode, err := directoryIdentity(stage)
		if err != nil {
			return false, nil, fmt.Errorf("identify previous standby root for legacy install recovery: %w", err)
		}
		stageDevice, stageInode, err := directoryIdentity(root)
		if err != nil {
			return false, nil, fmt.Errorf("identify unready standby root for legacy install recovery: %w", err)
		}
		checkpoint.SwitchPrepared = true
		checkpoint.RootDevice, checkpoint.RootInode = rootDevice, rootInode
		checkpoint.StageDevice, checkpoint.StageInode = stageDevice, stageInode
		checkpoint.RollbackKeysPending = true
		data, err := json.Marshal(checkpoint)
		if err != nil {
			return false, nil, err
		}
		if err := writeFileSynced(stageCheckpointPath(cfg), data, 0o600); err != nil {
			return false, nil, fmt.Errorf("persist legacy standby rollback identity checkpoint: %w", err)
		}
	} else if err := setStageCheckpointRollbackKeysPending(cfg, snapshot, true, true); err != nil {
		return false, nil, fmt.Errorf("persist standby rollback key recovery checkpoint: %w", err)
	}
	prepCtx, cancelPreparation := context.WithTimeout(ctx, cfg.ServiceTimeout)
	defer cancelPreparation()
	if err := ensureSocketActivationDisabled(prepCtx, cfg.GiteaServiceName); err != nil {
		return false, nil, fmt.Errorf("disable standby socket activation before install recovery: %w", err)
	}
	fence, err := acquireSnapshotFence(prepCtx)
	if err != nil {
		return false, nil, fmt.Errorf("acquire standby write fence for install recovery: %w", err)
	}
	defer func() {
		if err := fence.Release(); err != nil {
			log.Error("Release standby write fence after install recovery failed: %v", err)
		}
	}()
	active, err := systemctlUnitActive(prepCtx, cfg.GiteaServiceName)
	if err != nil {
		return false, nil, err
	}
	if active {
		if err := systemctl(prepCtx, "stop", cfg.GiteaServiceName); err != nil {
			stopErr := fmt.Errorf("stop standby service before install recovery: %w", err)
			if restartErr := ensureStandbyService(context.Background(), cfg); restartErr != nil {
				return false, nil, errors.Join(stopErr, fmt.Errorf("restart standby after install recovery stop failure: %w", restartErr))
			}
			return false, nil, stopErr
		}
	}
	if err := atomicSwitchWithTimeout(ctx, cfg.ServiceTimeout, cfg); err != nil {
		rootAfter, rootErr := os.Lstat(root)
		stageAfter, stageErr := os.Lstat(stage)
		exchanged := rootErr == nil && stageErr == nil && rootAfter.IsDir() && stageAfter.IsDir() &&
			os.SameFile(rootAfter, stageInfo) && os.SameFile(stageAfter, rootInfo)
		if !exchanged {
			if active {
				if restartErr := ensureStandbyService(context.Background(), cfg); restartErr != nil {
					return false, nil, errors.Join(fmt.Errorf("exchange unready standby install back: %w", err), fmt.Errorf("restart standby after install recovery failure: %w", restartErr))
				}
			}
			return false, nil, fmt.Errorf("exchange unready standby install back: %w", err)
		}
		if syncErr := errors.Join(syncDirectory(filepath.Dir(root)), syncDirectory(filepath.Dir(stage))); syncErr != nil {
			return false, nil, errors.Join(fmt.Errorf("exchange unready standby install back: %w", err), fmt.Errorf("persist recovered directory exchange: %w", syncErr))
		}
		log.Warn("Install recovery exchange reported an error after completing; parent directory sync confirmed recovery: error=%v", err)
	}
	rootAfter, rootErr := os.Lstat(root)
	stageAfter, stageErr := os.Lstat(stage)
	if rootErr != nil || stageErr != nil || !os.SameFile(rootAfter, stageInfo) || !os.SameFile(stageAfter, rootInfo) {
		return false, nil, errors.Join(errors.New("install recovery exchange did not restore the previous standby root"), rootErr, stageErr)
	}
	executable, err := executablePath()
	if err != nil {
		return false, nil, fmt.Errorf("locate Gitea executable to restore standby authorized keys: %w", err)
	}
	keysCtx, cancelKeys := context.WithTimeout(ctx, cfg.ServiceTimeout)
	keysErr := regenerateKeys(keysCtx, executable, setting.CustomConf)
	cancelKeys()
	if keysErr != nil {
		return false, nil, fmt.Errorf("regenerate authorized keys for restored standby root: %w", keysErr)
	}
	if err := setStageCheckpointRollbackKeysPending(cfg, snapshot, false, true); err != nil {
		return false, nil, fmt.Errorf("clear standby rollback key recovery checkpoint: %w", err)
	}
	log.Info("Regenerated standby authorized keys after restoring previous root: snapshot=%s", checkpoint.SnapshotID)
	log.Info("Restored previous standby root before retrying unready install: stage=%s", filepath.Base(stage))
	return active, stageInfo, nil
}

func recoverPendingStandbyRollbackKeys(ctx context.Context, cfg *config, checkpoint stageCheckpoint) error {
	if !checkpoint.RollbackKeysPending {
		return nil
	}
	root, stage := filepath.Clean(setting.AppWorkPath), installStagePath(cfg)
	switchState, err := checkpointInstallSwitchState(checkpoint, root, stage)
	if err != nil {
		return fmt.Errorf("inspect standby rollback key recovery state: %w", err)
	}
	if switchState == installSwitchExchanged {
		wasActive, _, err := exchangeBackUnreadyInstall(ctx, cfg, stage, checkpoint)
		if err != nil {
			return fmt.Errorf("restore previous standby root before recovering authorized keys: %w", err)
		}
		if err := restoreCheckpointedStandbyService(ctx, cfg, checkpointedStandbyActive(checkpoint, wasActive)); err != nil {
			return fmt.Errorf("restore previous standby service state after authorized key recovery: %w", err)
		}
		return nil
	}
	rootHasPreviousIdentity := checkpoint.SwitchPrepared && pathHasDirectoryIdentity(root, checkpoint.RootDevice, checkpoint.RootInode)
	if switchState != installSwitchPrepared && !rootHasPreviousIdentity {
		return fmt.Errorf("cannot safely recover authorized keys with ambiguous directory identities: snapshot=%s", checkpoint.SnapshotID)
	}
	prepCtx, cancelPreparation := context.WithTimeout(ctx, cfg.ServiceTimeout)
	defer cancelPreparation()
	if err := ensureSocketActivationDisabled(prepCtx, cfg.GiteaServiceName); err != nil {
		return fmt.Errorf("disable standby socket activation before key recovery: %w", err)
	}
	fence, err := acquireSnapshotFence(prepCtx)
	if err != nil {
		return fmt.Errorf("acquire standby write fence for key recovery: %w", err)
	}
	fenceReleased := false
	defer func() {
		if !fenceReleased {
			if err := fence.Release(); err != nil {
				log.Error("Release standby write fence after key recovery failed: %v", err)
			}
		}
	}()
	active, err := systemctlUnitActive(prepCtx, cfg.GiteaServiceName)
	if err != nil {
		return fmt.Errorf("inspect standby service before key recovery: %w", err)
	}
	shouldBeActive := checkpointedStandbyActive(checkpoint, active)
	if active {
		if err := systemctl(prepCtx, "stop", cfg.GiteaServiceName); err != nil {
			return fmt.Errorf("stop standby service before key recovery: %w", err)
		}
	}
	executable, err := executablePath()
	if err != nil {
		return fmt.Errorf("locate Gitea executable for standby key recovery: %w", err)
	}
	keysCtx, cancelKeys := context.WithTimeout(ctx, cfg.ServiceTimeout)
	keysErr := regenerateKeys(keysCtx, executable, setting.CustomConf)
	cancelKeys()
	if keysErr != nil {
		return fmt.Errorf("regenerate standby authorized keys from restored root: %w", keysErr)
	}
	snapshot := &Snapshot{ID: checkpoint.SnapshotID, SHA256: checkpoint.ManifestSHA}
	if err := setStageCheckpointRollbackKeysPending(cfg, snapshot, false, true); err != nil {
		return fmt.Errorf("clear standby rollback key recovery checkpoint: %w", err)
	}
	log.Info("Recovered standby authorized keys from previous root: snapshot=%s", checkpoint.SnapshotID)
	if err := fence.Release(); err != nil {
		return fmt.Errorf("release standby write fence after key recovery: %w", err)
	}
	fenceReleased = true
	if err := restoreCheckpointedStandbyService(ctx, cfg, shouldBeActive); err != nil {
		return fmt.Errorf("restore standby service state after key recovery: %w", err)
	}
	return nil
}

func prepareIncrementalStage(ctx context.Context, cfg *config, final *SnapshotManifest, stage string) (bool, error) {
	checkpointPath := stageCheckpointPath(cfg)
	checkpoint, checkpointErr := readStageCheckpoint(checkpointPath)
	stageInfo, stageErr := os.Lstat(stage)
	checkpointMatches := checkpointErr == nil && checkpoint.SnapshotID == final.ID && checkpoint.ManifestSHA == final.SHA256
	if checkpointMatches {
		if stageErr == nil {
			if stageInfo.IsDir() && stageInfo.Mode()&os.ModeSymlink == 0 {
				return true, nil
			}
			log.Info("Cannot resume incremental staging tree because its path is not a real directory: snapshot=%s stage=%s mode=%s", final.ID, filepath.Base(stage), stageInfo.Mode())
		} else {
			log.Info("Cannot resume incremental staging tree because its directory is unavailable: snapshot=%s stage=%s error=%v", final.ID, filepath.Base(stage), stageErr)
		}
	} else if checkpointErr != nil {
		if !os.IsNotExist(checkpointErr) {
			log.Warn("Cannot reuse incremental staging checkpoint: snapshot=%s error=%v", final.ID, checkpointErr)
		} else if stageErr == nil {
			log.Info("Discarding uncheckpointed incremental staging tree: snapshot=%s stage=%s", final.ID, filepath.Base(stage))
		}
	} else if checkpoint.SnapshotID != final.ID {
		log.Info("Discarding incremental staging progress for a different snapshot: requested_snapshot=%s checkpoint_snapshot=%s", final.ID, checkpoint.SnapshotID)
	} else {
		log.Info("Discarding incremental staging progress for a different manifest: snapshot=%s", final.ID)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if stageErr == nil {
		log.Info("Removing incremental staging tree before restart: snapshot=%s stage=%s", final.ID, filepath.Base(stage))
		if err := makeTreeRemovable(ctx, stage); err != nil {
			return false, fmt.Errorf("make previous staging tree removable: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
	} else if !os.IsNotExist(stageErr) {
		return false, stageErr
	}
	if err := os.RemoveAll(stage); err != nil {
		return false, fmt.Errorf("remove previous staging tree %q: %w", stage, err)
	}
	if err := removeFileSynced(checkpointPath); err != nil {
		return false, fmt.Errorf("remove previous staging checkpoint %q: %w", checkpointPath, err)
	}
	if err := os.Mkdir(stage, 0o700); err != nil {
		return false, fmt.Errorf("create staging tree %q: %w", stage, err)
	}
	data, err := json.Marshal(stageCheckpoint{Version: stageCheckpointVersion, SnapshotID: final.ID, ManifestSHA: final.SHA256})
	if err != nil {
		return false, fmt.Errorf("encode staging checkpoint for snapshot %s: %w", final.ID, err)
	}
	if err := writeFileSynced(checkpointPath, data, 0o600); err != nil {
		return false, fmt.Errorf("persist staging checkpoint %q: %w", checkpointPath, err)
	}
	return false, nil
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
	var chunkErr *chunkSourceChangedError
	if errors.As(err, &chunkErr) {
		return false
	}
	var statusErr *httpResponseStatusError
	if errors.As(err, &statusErr) {
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

func completeFinalSync(ctx context.Context, cfg *config, base string, client *http.Client, final, previous *SnapshotManifest, cacheDir, stage string, localChunkCandidates ...map[string][]chunkLocation) error {
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
	if err := pruneChunkCache(ctx, cacheDir, final.ID, final); err != nil {
		return fmt.Errorf("prune stale replication chunk cache: %w", err)
	}
	chunkPassStarted := time.Now()
	var knownLocalCandidates map[string][]chunkLocation
	if len(localChunkCandidates) > 0 {
		knownLocalCandidates = localChunkCandidates[0]
	}
	if err := fetchMissingChunks(ctx, client, base, cfg.ControlToken, final, previous, cacheDir, false, &knownLocalCandidates); err != nil {
		log.Error("Final chunk preparation failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(chunkPassStarted), err)
		if preserveFinalSession(err) {
			abortSession = false
			log.Warn("Final sync session %s is retained for retry after transient transport failure: %v", final.ID, err)
		}
		return err
	}
	log.Info("Final chunk preparation completed: snapshot=%s duration=%s", final.ID, time.Since(chunkPassStarted))
	resumedStage, err := prepareIncrementalStage(ctx, cfg, final, stage)
	if err != nil {
		log.Error("Cannot prepare incremental stage: snapshot=%s stage=%s error=%v", final.ID, stage, err)
		return err
	}
	if resumedStage {
		log.Info("Resuming incremental staging tree: snapshot=%s", final.ID)
	}
	root := filepath.Clean(setting.AppWorkPath)
	stageStarted := time.Now()
	var onDemandResponsesReceived, onDemandPayloadBytesReceived, onDemandEncodedPayloadBytes, onDemandEncodedBodyBytes, onDemandEncodedBodyMeasurements atomic.Int64
	fetch := func(hash string) ([]byte, error) {
		started := time.Now()
		data, encodedBytes, err := requestChunk(ctx, client, base, cfg.ControlToken, final.ID, hash)
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
	stageErr := buildIncrementalStage(ctx, root, stage, cacheDir, final, previous, fetch, knownLocalCandidates)
	if responses := onDemandResponsesReceived.Load(); responses > 0 {
		log.Info("On-demand final chunk transfer summary: snapshot=%s responses_received=%d payload_bytes_received=%d server_encoded_body_bytes=%d measured_responses=%d/%d", final.ID, responses, onDemandPayloadBytesReceived.Load(), onDemandEncodedBodyBytes.Load(), onDemandEncodedBodyMeasurements.Load(), responses)
		logChunkEncodingSummary("On-demand final", final.ID, onDemandEncodedPayloadBytes.Load(), onDemandEncodedBodyBytes.Load(), onDemandEncodedBodyMeasurements.Load())
	}
	if stageErr != nil {
		log.Error("Incremental stage build failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(stageStarted), stageErr)
		if preserveFinalSession(stageErr) {
			abortSession = false
			log.Warn("Final sync session %s is retained for retry after transient transport failure: %v", final.ID, stageErr)
		}
		return stageErr
	}
	log.Info("Prepared incremental stage: snapshot=%s duration=%s", final.ID, time.Since(stageStarted))
	log.Info("Activating incremental stage for snapshot %s on the standby", final.ID)
	activationStarted := time.Now()
	renewSession := func(phase, checkpointPhase string) error {
		renewStarted := time.Now()
		renewalID, err := persistedStageRenewalID(cfg, &final.Snapshot, checkpointPhase)
		if err != nil {
			return fmt.Errorf("prepare final sync renewal for %s: %w", phase, err)
		}
		if err := renewRemoteSession(ctx, client, base, cfg.ControlToken, final.ID, renewalID); err != nil {
			log.Error("Renew final sync session failed: snapshot=%s phase=%s duration=%s error=%v", final.ID, phase, time.Since(renewStarted), err)
			return err
		}
		log.Info("Renewed final sync session: snapshot=%s phase=%s duration=%s", final.ID, phase, time.Since(renewStarted))
		return nil
	}
	if err := installPreparedSnapshotWithVerifierAndHooks(ctx, stage, &final.Snapshot, cfg, func() error {
		return verifyRestoredStandbyTree(ctx, root, final)
	}, snapshotInstallHooks{
		requireCheckpoint: true,
		afterStageSync: func() error {
			return renewSession("durable standby staging", "durable_staging")
		},
		beforeStandbyStart: func() error {
			return renewSession("before starting standby", "before_service_start")
		},
		beforeReadiness: func() error {
			return renewSession("before standby readiness check", "before_readiness")
		},
		afterStandbyStopped: func() error {
			releaseStarted := time.Now()
			if err := finishRemoteSession(ctx, client, base, cfg.ControlToken, final.ID, "release"); err != nil {
				log.Error("Release primary after standby readiness and stop failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(releaseStarted), err)
				return err
			}
			log.Info("Primary released after standby readiness and stop: snapshot=%s duration=%s", final.ID, time.Since(releaseStarted))
			return nil
		},
	}); err != nil {
		var warning *cleanupWarning
		if !errors.As(err, &warning) {
			log.Error("Standby activation failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(activationStarted), err)
			return err
		}
		log.Warn("Standby activation completed with cleanup warning: snapshot=%s error=%v", final.ID, warning)
	}
	identityStarted := time.Now()
	if err := recordLocalChangeIDs(ctx, filepath.Clean(setting.AppWorkPath), final); err != nil {
		log.Error("Record standby file identities after activation failed: snapshot=%s duration=%s error=%v", final.ID, time.Since(identityStarted), err)
		return fmt.Errorf("record standby file identities after activation: %w", err)
	}
	log.Info("Standby activation completed: snapshot=%s duration=%s; marking remote session complete", final.ID, time.Since(activationStarted))
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

func resumeFinalSync(ctx context.Context, cfg *config, base string, client *http.Client, previous, final *SnapshotManifest, cacheDir, stage string) (bool, error) {
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
			if err := verifyRestoredStandbyTree(ctx, filepath.Clean(setting.AppWorkPath), ready); err != nil {
				if ctx.Err() != nil {
					return true, ctx.Err()
				}
				log.Warn("Local data does not match completed final sync checkpoint: snapshot=%s error=%v; starting a new sync", final.ID, err)
				return false, nil
			}
			identityStarted := time.Now()
			if err := recordLocalChangeIDs(ctx, filepath.Clean(setting.AppWorkPath), ready); err != nil {
				log.Error("Record standby file identities for recovered sync failed: snapshot=%s duration=%s error=%v", ready.ID, time.Since(identityStarted), err)
				return true, fmt.Errorf("record recovered standby file identities: %w", err)
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
		checkpoint, checkpointErr := readStageCheckpoint(stageCheckpointPath(cfg))
		checkpointMatches := checkpointErr == nil && checkpoint.SnapshotID == remote.ID && checkpoint.ManifestSHA == remote.SHA256
		if checkpointErr != nil {
			if os.IsNotExist(checkpointErr) {
				log.Info("No standby install checkpoint is available for active final sync recovery: snapshot=%s", remote.ID)
			} else {
				log.Warn("Cannot read standby install checkpoint for active final sync recovery: snapshot=%s error=%v", remote.ID, checkpointErr)
			}
		} else if !checkpointMatches {
			log.Warn("Standby install checkpoint does not match active final sync: snapshot=%s checkpoint_snapshot=%s checkpoint_manifest_sha256=%s remote_manifest_sha256=%s", remote.ID, checkpoint.SnapshotID, checkpoint.ManifestSHA, remote.SHA256)
		}
		if checkpointMatches {
			_, stageErr := os.Lstat(stage)
			if stageErr != nil && !os.IsNotExist(stageErr) {
				log.Warn("Cannot inspect incremental install stage during recovery: snapshot=%s error=%v", remote.ID, stageErr)
			}
			if !checkpoint.StandbyReady && checkpoint.Version == stageCheckpointVersion && checkpoint.StandbyWasActive != nil && !checkpoint.SwitchPrepared &&
				(stageErr == nil || os.IsNotExist(stageErr)) {
				if err := restoreCheckpointedStandbyService(ctx, cfg, *checkpoint.StandbyWasActive); err != nil {
					return true, fmt.Errorf("restore previous standby service state: %w", err)
				}
			}
			if !checkpoint.StandbyReady && checkpoint.SwitchPrepared && os.IsNotExist(stageErr) {
				if checkpoint.RootInode == 0 || checkpoint.StageInode == 0 {
					return true, fmt.Errorf("cannot recover standby install with incomplete directory identities: snapshot=%s", remote.ID)
				}
				root := filepath.Clean(setting.AppWorkPath)
				rootWasOriginal := pathHasDirectoryIdentity(root, checkpoint.RootDevice, checkpoint.RootInode)
				rootIsInstalled := pathHasDirectoryIdentity(root, checkpoint.StageDevice, checkpoint.StageInode)
				if rootWasOriginal == rootIsInstalled {
					return true, fmt.Errorf("cannot safely recover standby install with a missing stage and ambiguous root identity: snapshot=%s", remote.ID)
				}
				if rootIsInstalled {
					err := fmt.Errorf("standby install exchange completed but the previous root backup is missing: snapshot=%s", remote.ID)
					log.Error("Cannot safely continue standby install recovery: %v", err)
					return true, err
				}
				log.Warn("Standby install stage is missing but the original root is still active; rebuilding stage: snapshot=%s", remote.ID)
				if checkpoint.StandbyWasActive != nil {
					if err := restoreCheckpointedStandbyService(ctx, cfg, *checkpoint.StandbyWasActive); err != nil {
						return true, fmt.Errorf("restore previous standby service state: %w", err)
					}
				}
			}
			var restoredRootInfo os.FileInfo
			serviceWasActive := false
			exchangedBack := false
			if !checkpoint.StandbyReady && stageErr == nil {
				switchState, switchErr := checkpointInstallSwitchState(checkpoint, filepath.Clean(setting.AppWorkPath), stage)
				if switchErr != nil {
					return true, fmt.Errorf("inspect unready standby install recovery state: %w", switchErr)
				}
				if switchState == installSwitchExchanged {
					log.Warn("Unready standby install was exchanged before interruption; restoring the previous root before retry: snapshot=%s", remote.ID)
					stopRecoveryHeartbeats := startFinalSessionProgressHeartbeats(ctx, client, base, cfg.ControlToken, remote.ID, cfg.FinalSessionTimeout)
					serviceWasActive, restoredRootInfo, err = exchangeBackUnreadyInstall(ctx, cfg, stage, checkpoint)
					stopRecoveryHeartbeats()
					if err != nil {
						return true, fmt.Errorf("recover unready standby install: %w", err)
					}
					serviceWasActive = checkpointedStandbyActive(checkpoint, serviceWasActive)
					if err := restoreCheckpointedStandbyService(ctx, cfg, serviceWasActive); err != nil {
						return true, fmt.Errorf("restore previous standby service state: %w", err)
					}
					exchangedBack = true
				} else if checkpoint.SwitchPrepared && switchState == installSwitchUnknown {
					return true, fmt.Errorf("cannot safely resume standby install with ambiguous directory identities: snapshot=%s", remote.ID)
				} else if !checkpoint.SwitchPrepared && checkpoint.Version == 0 {
					// Checkpoints written before directory identities were introduced can still
					// identify the exchanged state by verifying the installed manifest.
					verifyStarted := time.Now()
					stopRecoveryHeartbeats := startFinalSessionProgressHeartbeats(ctx, client, base, cfg.ControlToken, remote.ID, cfg.FinalSessionTimeout)
					if verifyErr := verifyRestoredStandbyTree(ctx, filepath.Clean(setting.AppWorkPath), remote); verifyErr == nil {
						stopRecoveryHeartbeats()
						log.Warn("Legacy unready standby install checkpoint matches the active root; restoring its backup before retry: snapshot=%s verification_duration=%s", remote.ID, time.Since(verifyStarted))
						stopRecoveryHeartbeats = startFinalSessionProgressHeartbeats(ctx, client, base, cfg.ControlToken, remote.ID, cfg.FinalSessionTimeout)
						serviceWasActive, restoredRootInfo, err = exchangeBackUnreadyInstall(ctx, cfg, stage, checkpoint)
						stopRecoveryHeartbeats()
						if err != nil {
							return true, fmt.Errorf("recover legacy unready standby install: %w", err)
						}
						exchangedBack = true
					} else {
						stopRecoveryHeartbeats()
						if ctx.Err() != nil {
							return true, ctx.Err()
						}
						log.Info("Legacy unready standby install checkpoint does not match the active root; resuming staged install: snapshot=%s verification_duration=%s error=%v", remote.ID, time.Since(verifyStarted), verifyErr)
					}
				} else if switchState == installSwitchPrepared {
					log.Info("Standby install checkpoint confirms that directory exchange had not started: snapshot=%s", remote.ID)
					if checkpoint.StandbyWasActive != nil {
						if err := restoreCheckpointedStandbyService(ctx, cfg, *checkpoint.StandbyWasActive); err != nil {
							return true, fmt.Errorf("restore previous standby service state: %w", err)
						}
					}
				}
			}
			if checkpoint.StandbyReady {
				stopRecoveryHeartbeats := startFinalSessionProgressHeartbeats(ctx, client, base, cfg.ControlToken, remote.ID, cfg.FinalSessionTimeout)
				prepCtx, cancelPreparation := context.WithTimeout(ctx, cfg.ServiceTimeout)
				socketErr := ensureSocketActivationDisabled(prepCtx, cfg.GiteaServiceName)
				standbyActive := false
				var serviceErr error
				if socketErr == nil {
					standbyActive, serviceErr = systemctlUnitActive(prepCtx, cfg.GiteaServiceName)
				}
				cancelPreparation()
				stopRecoveryHeartbeats()
				if socketErr != nil {
					log.Warn("Skipping installed-tree recovery because standby socket activation is unavailable: snapshot=%s error=%v", remote.ID, socketErr)
				} else if serviceErr != nil {
					log.Warn("Skipping installed-tree recovery because standby service state is unavailable: snapshot=%s error=%v", remote.ID, serviceErr)
				} else if standbyActive {
					log.Info("Skipping installed-tree recovery because standby Gitea is active; rerunning activation to stop it: snapshot=%s", remote.ID)
				} else {
					verifyStarted := time.Now()
					var verifyErr, identityErr, finishErr error
					var remoteFinishStarted time.Time
					func() {
						stopProgressHeartbeats := startFinalSessionProgressHeartbeats(ctx, client, base, cfg.ControlToken, remote.ID, cfg.FinalSessionTimeout)
						defer stopProgressHeartbeats()
						verifyErr = verifyRestoredStandbyTree(ctx, filepath.Clean(setting.AppWorkPath), remote)
						if verifyErr == nil {
							log.Info("Recovered installed standby tree for active final sync: snapshot=%s verification_duration=%s", remote.ID, time.Since(verifyStarted))
							identityStarted := time.Now()
							identityErr = recordLocalChangeIDs(ctx, filepath.Clean(setting.AppWorkPath), remote)
							if identityErr != nil {
								log.Error("Record standby file identities for installed-tree recovery failed: snapshot=%s duration=%s error=%v", remote.ID, time.Since(identityStarted), identityErr)
								identityErr = fmt.Errorf("record installed standby file identities: %w", identityErr)
							}
							if identityErr == nil {
								remoteFinishStarted = time.Now()
								finishErr = finishRemoteSession(ctx, client, base, cfg.ControlToken, remote.ID, "complete")
							}
						}
					}()
					if verifyErr != nil && ctx.Err() != nil {
						return true, ctx.Err()
					}
					if verifyErr == nil {
						if identityErr != nil {
							if !preserveFinalSession(identityErr) {
								abortFinalSyncSession(cfg, client, base, remote.ID)
							}
							return true, identityErr
						}
						if finishErr != nil {
							log.Error("Remote final sync completion failed after recovering installed tree: snapshot=%s duration=%s error=%v", remote.ID, time.Since(remoteFinishStarted), finishErr)
							if !preserveFinalSession(finishErr) {
								abortFinalSyncSession(cfg, client, base, remote.ID)
							}
							return true, finishErr
						}
						if err := persistReadyStandbySync(ctx, cfg, remote, cacheDir); err != nil {
							log.Error("Persist recovered installed standby sync failed: snapshot=%s error=%v", remote.ID, err)
							return true, err
						}
						log.Info("Completed active final sync from already installed standby tree: snapshot=%s", remote.ID)
						return true, nil
					}
					if verifyErr != nil {
						log.Warn("Active final sync checkpoint does not match installed standby tree: snapshot=%s error=%v; rebuilding stage", remote.ID, verifyErr)
					}
				}
			} else if stageErr == nil {
				log.Info("Incremental install stage remains and standby readiness is not checkpointed: snapshot=%s; rebuilding and rerunning activation", remote.ID)
			} else if os.IsNotExist(stageErr) {
				log.Info("Incremental install stage is absent but standby readiness is not checkpointed: snapshot=%s; rebuilding and rerunning activation", remote.ID)
			}
			if exchangedBack {
				log.Info("Retrying final sync after restoring the previous standby root: snapshot=%s", remote.ID)
				err := completeFinalSync(ctx, cfg, base, client, remote, previous, cacheDir, stage)
				if err != nil && serviceWasActive {
					checkpointAfter, checkpointErr := readStageCheckpoint(stageCheckpointPath(cfg))
					rootAfter, rootErr := os.Lstat(filepath.Clean(setting.AppWorkPath))
					if checkpointErr == nil && checkpointAfter.SnapshotID == remote.ID && !checkpointAfter.StandbyReady &&
						rootErr == nil && restoredRootInfo != nil && os.SameFile(rootAfter, restoredRootInfo) {
						if restartErr := ensureStandbyService(context.Background(), cfg); restartErr != nil {
							err = errors.Join(err, fmt.Errorf("restart previous standby after unready install retry failed: %w", restartErr))
						} else {
							log.Info("Restarted previous standby service after unready install retry failed: snapshot=%s", remote.ID)
						}
					}
				}
				return true, err
			}
		}
		log.Info("Resuming active final sync session %s from verified local chunk cache", final.ID)
		return true, completeFinalSync(ctx, cfg, base, client, remote, previous, cacheDir, stage)
	}
	return true, fmt.Errorf("remote final sync state did not stabilize: %w", stateChangedErr)
}

func recoverStandbyInstallCheckpoint(ctx context.Context, cfg *config, base string, client *http.Client, cacheDir string, checkpoint stageCheckpoint) (*SnapshotManifest, bool, error) {
	if !validSnapshotID(checkpoint.SnapshotID) || len(checkpoint.ManifestSHA) != sha256.Size*2 || !isLowerHex(checkpoint.ManifestSHA) {
		return nil, false, errors.New("standby install checkpoint has invalid snapshot identity")
	}
	completeReady := func(ready *SnapshotManifest) (bool, error) {
		if !matchesStageCheckpoint(ready, checkpoint) {
			return false, errors.New("ready manifest does not match the standby install checkpoint")
		}
		if err := verifyRestoredStandbyTree(ctx, filepath.Clean(setting.AppWorkPath), ready); err != nil {
			return false, fmt.Errorf("verify standby tree from install checkpoint: %w", err)
		}
		if err := recordLocalChangeIDs(ctx, filepath.Clean(setting.AppWorkPath), ready); err != nil {
			return false, fmt.Errorf("record standby file identities from install checkpoint: %w", err)
		}
		if err := persistReadyStandbySync(ctx, cfg, ready, cacheDir); err != nil {
			return false, fmt.Errorf("persist recovered standby install checkpoint: %w", err)
		}
		log.Info("Recovered completed standby install from its checkpoint: snapshot=%s", ready.ID)
		return true, nil
	}

	ready, localErr := loadTrustedManifest(manifestPath(cfg.SnapshotDir, checkpoint.SnapshotID), cfg.ControlToken, "ready")
	if localErr == nil {
		completed, err := completeReady(ready)
		return nil, completed, err
	}
	if !os.IsNotExist(localErr) {
		log.Warn("Cannot recover standby install from local ready manifest: snapshot=%s error=%v", checkpoint.SnapshotID, localErr)
	}

	var inactive bool
	for attempt := 1; attempt <= requestRetryLimit; attempt++ {
		status, err := requestSnapshotStatus(ctx, client, base, cfg.ControlToken, checkpoint.SnapshotID)
		if errors.Is(err, errRemoteSnapshotUnavailable) {
			inactive = true
			break
		}
		if err != nil {
			return nil, false, fmt.Errorf("check remote standby install checkpoint %s: %w", checkpoint.SnapshotID, err)
		}
		if status.State == "failed" {
			inactive = true
			break
		}
		if status.State != "transferring" && status.State != "ready" {
			return nil, false, fmt.Errorf("remote standby install checkpoint has unexpected state %q: snapshot=%s", status.State, checkpoint.SnapshotID)
		}
		expectedState := status.State
		remote, err := requestManifestByID(ctx, client, base, cfg.ControlToken, checkpoint.SnapshotID, expectedState)
		if errors.Is(err, errRemoteManifestStateChanged) {
			continue
		}
		if err != nil {
			return nil, false, fmt.Errorf("load remote standby install checkpoint %s: %w", checkpoint.SnapshotID, err)
		}
		if expectedState == "transferring" {
			if remote.SHA256 != checkpoint.ManifestSHA {
				return nil, false, errors.New("remote final manifest does not match the standby install checkpoint")
			}
			if err := persistStandbyManifestContext(ctx, cfg.SnapshotDir, remote); err != nil {
				return nil, false, fmt.Errorf("persist recovered remote final manifest: %w", err)
			}
			log.Info("Recovered remote final manifest from standby install checkpoint: snapshot=%s", remote.ID)
			return remote, false, nil
		}
		completed, err := completeReady(remote)
		return nil, completed, err
	}
	if !inactive {
		return nil, false, fmt.Errorf("remote standby install checkpoint state did not stabilize: snapshot=%s", checkpoint.SnapshotID)
	}
	if checkpoint.StandbyReady {
		transfer, err := loadTrustedManifest(manifestPath(cfg.SnapshotDir, checkpoint.SnapshotID), cfg.ControlToken, "transferring")
		if err != nil {
			return nil, false, fmt.Errorf("load verified standby transfer manifest from install checkpoint: %w", err)
		}
		if transfer.SHA256 != checkpoint.ManifestSHA {
			return nil, false, errors.New("standby transfer manifest does not match the ready install checkpoint")
		}
		transfer.Snapshot.State, transfer.Snapshot.Error = "ready", ""
		completed, err := completeReady(transfer)
		if err != nil {
			return nil, false, err
		}
		log.Info("Recovered verified standby install after its remote session ended before local readiness was persisted: snapshot=%s", transfer.ID)
		return nil, completed, nil
	}
	if !checkpoint.SwitchPrepared {
		if checkpoint.Version == stageCheckpointVersion {
			if checkpoint.StandbyWasActive != nil {
				if err := restoreCheckpointedStandbyService(ctx, cfg, *checkpoint.StandbyWasActive); err != nil {
					return nil, false, fmt.Errorf("restore previous standby service state: %w", err)
				}
			}
			log.Info("Inactive standby install checkpoint confirms the previous root is active: snapshot=%s", checkpoint.SnapshotID)
			return nil, false, nil
		}
		return nil, false, errors.New("cannot safely recover an inactive standby install without directory identities")
	}

	root, stage := filepath.Clean(setting.AppWorkPath), installStagePath(cfg)
	switchState, err := checkpointInstallSwitchState(checkpoint, root, stage)
	if err != nil {
		return nil, false, fmt.Errorf("inspect inactive standby install checkpoint: %w", err)
	}
	switch switchState {
	case installSwitchPrepared:
		log.Info("Inactive standby install checkpoint confirms the previous root is still active: snapshot=%s", checkpoint.SnapshotID)
		if checkpoint.StandbyWasActive != nil {
			if err := restoreCheckpointedStandbyService(ctx, cfg, *checkpoint.StandbyWasActive); err != nil {
				return nil, false, fmt.Errorf("restore previous standby service state: %w", err)
			}
		}
		return nil, false, nil
	case installSwitchExchanged:
		log.Warn("Inactive standby install had exchanged directories; restoring the previous root before a new sync: snapshot=%s", checkpoint.SnapshotID)
		wasActive, _, err := exchangeBackUnreadyInstall(ctx, cfg, stage, checkpoint)
		if err != nil {
			return nil, false, fmt.Errorf("restore previous standby root from inactive install checkpoint: %w", err)
		}
		if err := restoreCheckpointedStandbyService(ctx, cfg, checkpointedStandbyActive(checkpoint, wasActive)); err != nil {
			return nil, false, fmt.Errorf("restore previous standby service state: %w", err)
		}
		return nil, false, nil
	default:
		rootWasOriginal := pathHasDirectoryIdentity(root, checkpoint.RootDevice, checkpoint.RootInode)
		_, stageErr := os.Lstat(stage)
		if rootWasOriginal && os.IsNotExist(stageErr) {
			log.Warn("Inactive standby install stage is missing but the previous root is intact: snapshot=%s", checkpoint.SnapshotID)
			if checkpoint.StandbyWasActive != nil {
				if err := restoreCheckpointedStandbyService(ctx, cfg, *checkpoint.StandbyWasActive); err != nil {
					return nil, false, fmt.Errorf("restore previous standby service state: %w", err)
				}
			}
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("cannot safely recover inactive standby install with ambiguous directory identities: snapshot=%s", checkpoint.SnapshotID)
	}
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
	checkpoint, checkpointErr := readStageCheckpoint(stageCheckpointPath(cfg))
	if checkpointErr != nil && !os.IsNotExist(checkpointErr) {
		return fmt.Errorf("read standby install checkpoint before cleanup: %w", checkpointErr)
	}
	if checkpointErr == nil {
		if !validSnapshotID(checkpoint.SnapshotID) || len(checkpoint.ManifestSHA) != sha256.Size*2 || !isLowerHex(checkpoint.ManifestSHA) {
			return errors.New("standby install checkpoint has invalid snapshot identity")
		}
		stopRecoveryHeartbeats = startFinalSessionProgressHeartbeats(ctx, client, base, cfg.ControlToken, checkpoint.SnapshotID, cfg.FinalSessionTimeout)
		if err := recoverPendingStandbyRollbackKeys(ctx, cfg, checkpoint); err != nil {
			return fmt.Errorf("recover standby authorized keys from install checkpoint: %w", err)
		}
		reloadedCheckpoint, err := readStageCheckpoint(stageCheckpointPath(cfg))
		if err != nil {
			return fmt.Errorf("reload standby install checkpoint after key recovery: %w", err)
		}
		checkpoint = reloadedCheckpoint
		recoveredFinal, completed, err := recoverStandbyInstallCheckpoint(ctx, cfg, base, client, cacheDir, checkpoint)
		if err != nil {
			return err
		}
		if completed {
			stopRecoveryHeartbeats()
			stopRecoveryHeartbeats = nil
			return nil
		}
		if recoveredFinal != nil {
			finalRecovery = recoveredFinal
		} else if finalRecovery != nil && finalRecovery.ID == checkpoint.SnapshotID {
			finalRecovery = nil
		}
		if finalRecovery == nil || finalRecovery.ID != checkpoint.SnapshotID {
			stopRecoveryHeartbeats()
			stopRecoveryHeartbeats = nil
		}
	}
	if finalRecovery != nil && stopRecoveryHeartbeats == nil {
		stopRecoveryHeartbeats = startFinalSessionProgressHeartbeats(ctx, client, base, cfg.ControlToken, finalRecovery.ID, cfg.FinalSessionTimeout)
	}
	pruneReplicationTemporaryFiles(cfg.SnapshotDir)
	pruneManifestFiles(cfg.SnapshotDir, cfg.SnapshotRetention, cfg.ControlToken)
	if err := pruneFailedRestoreStages(cfg.SnapshotDir, 1); err != nil {
		log.Error("Cannot prune older failed standby restore stages: error=%v", err)
		return err
	}
	stage := installStagePath(cfg)
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
	if resumed, err := resumeFinalSync(ctx, cfg, base, client, previous, finalRecovery, cacheDir, stage); resumed {
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
		if err := fetchMissingChunks(ctx, client, base, cfg.ControlToken, preflight, previous, cacheDir, true, &localChunkCandidates); err != nil {
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
		return completeFinalSync(ctx, cfg, base, client, final, previous, cacheDir, stage, localChunkCandidates)
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
