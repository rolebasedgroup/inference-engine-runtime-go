package rbg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	kcache "k8s.io/client-go/tools/cache"

	"github.com/rolebasedgroup/inference-engine-runtime-go/pkg/runtime"
)

// observation is the opaque snapshot token: the credential of exactly one
// observation of the target RoleInstance. The core never parses it.
type observation struct {
	uid             types.UID
	hookState       string
	resourceVersion string
	signals         map[runtime.Signal]bool
}

// errNotSynced reports that API-mode observation has not started or
// synced yet. It is distinct from fail-closed protocol errors, which are
// returned as soon as the object is observed.
var errNotSynced = errors.New("rbg: source observation has not started or synced yet")

// Source observes one RoleInstance in one namespace and acknowledges
// completed signals by removing only the labels this runtime executed.
type Source struct {
	client     dynamic.Interface
	namespace  string
	name       string
	labelFile  string
	storeMu    sync.RWMutex
	store      kcache.Store
	replayTTL  time.Duration
	suppressMu sync.Mutex
	suppressed map[runtime.Signal]time.Time
	pollEvery  time.Duration
	logger     *slog.Logger
}

// Option configures a Source.
type Option func(*Source)

// WithLogger sets the source logger. The default is slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(s *Source) {
		if logger != nil {
			s.logger = logger
		}
	}
}

// WithLabelFile switches the source to file observation: the snapshot is
// the projected Pod label list and the source performs no API call before
// an acknowledgement. The file must be a downwardAPI projection of the
// Pod's metadata.labels (the RoleInstance controller mirrors signalLabels
// keys onto member Pods). This removes LIST/WATCH and per-snapshot GET
// calls; see WithReplaySuppressTTL for the accepted trade-offs.
func WithLabelFile(path string) Option {
	return func(s *Source) {
		if path != "" {
			s.labelFile = path
		}
	}
}

// WithPollInterval sets the label-file polling interval (default 1s). It
// only applies when a label file is configured.
func WithPollInterval(interval time.Duration) Option {
	return func(s *Source) { s.pollEvery = interval }
}

// WithReplaySuppressTTL sets how long a signal stays hidden from file
// snapshots after it was acknowledged or observed as stale (default 30s).
// In file mode it must cover the RI-to-Pod mirror plus kubelet file
// propagation. In API mode it bounds re-examination of suppressed signals
// and missed watch updates. Acknowledgement always GETs the RoleInstance
// and validates hook-state, declaration, and label value before patching,
// so write-back safety is unchanged in both modes.
func WithReplaySuppressTTL(ttl time.Duration) Option {
	return func(s *Source) { s.replayTTL = ttl }
}

// New creates a source bound to a single RoleInstance. By default it
// observes through an informer watching only that object via a
// metadata.name field selector; the snapshot reads the watched object and
// performs no extra GET. With WithLabelFile the snapshot becomes the
// projected Pod label list instead. Acknowledgement always re-GETs and
// validates before patching.
func New(client dynamic.Interface, namespace string, name string, opts ...Option) (*Source, error) {
	if client == nil {
		return nil, errors.New("rbg: dynamic client must not be nil")
	}
	if namespace == "" {
		return nil, errors.New("rbg: RoleInstance namespace must not be empty")
	}
	if name == "" {
		return nil, errors.New("rbg: RoleInstance name must not be empty")
	}
	source := &Source{
		client:    client,
		namespace: namespace,
		name:      name,
		replayTTL: 30 * time.Second,
		pollEvery: time.Second,
		logger:    slog.Default(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(source)
		}
	}
	if source.labelFile != "" && source.pollEvery <= 0 {
		return nil, errors.New("rbg: label-file polling interval must be positive")
	}
	if source.replayTTL <= 0 {
		return nil, errors.New("rbg: replay suppression TTL must be positive")
	}
	return source, nil
}

// Run observes until ctx is done: file mode polls the label file, API mode
// watches the RoleInstance. Notifications are coalesced and never carry
// execution state. Run returns nil once ctx is canceled.
func (s *Source) Run(ctx context.Context, notify chan<- struct{}) error {
	if s.labelFile != "" {
		return s.runLabelFile(ctx, notify)
	}
	return s.runInformer(ctx, notify)
}

// runInformer observes the RoleInstance through an informer. The watched
// object in the informer store is the observation result: Snapshot reads
// it without another API call. A periodic fallback notification bounds how
// long replay-suppressed signals or missed watch updates stay unseen.
func (s *Source) runInformer(ctx context.Context, notify chan<- struct{}) error {
	tweak := func(options *metav1.ListOptions) {
		options.FieldSelector = fields.OneTermEqualSelector("metadata.name", s.name).String()
	}
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(s.client, 0, s.namespace, tweak)
	informer := factory.ForResource(RoleInstanceGVR).Informer()

	handler := kcache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { notifySource(notify) },
		UpdateFunc: func(_, _ any) { notifySource(notify) },
		DeleteFunc: func(any) { notifySource(notify) },
	}
	if _, err := informer.AddEventHandler(handler); err != nil {
		return fmt.Errorf("rbg: register informer handler: %w", err)
	}

	factory.Start(ctx.Done())
	if !kcache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return nil
	}
	s.setStore(informer.GetStore())

	ticker := time.NewTicker(s.replayTTL)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			notifySource(notify)
		}
	}
}

// runLabelFile ticks at the polling interval instead of watching the API
// server. Snapshot reads the file itself, so every tick is a complete
// wake-up: change detection would save nothing but code. Observation
// freshness is therefore bounded by the polling interval.
func (s *Source) runLabelFile(ctx context.Context, notify chan<- struct{}) error {
	ticker := time.NewTicker(s.pollEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			notifySource(notify)
		}
	}
}

func notifySource(notify chan<- struct{}) {
	select {
	case notify <- struct{}{}:
	default:
	}
}

func (s *Source) setStore(store kcache.Store) {
	s.storeMu.Lock()
	defer s.storeMu.Unlock()
	s.store = store
}

func (s *Source) currentStore() kcache.Store {
	s.storeMu.RLock()
	defer s.storeMu.RUnlock()
	return s.store
}

// Snapshot returns the last observed state without an extra API call: the
// informer store in API mode, or the label file in file mode. It fails
// closed on unknown hook-state values, malformed lifecycle snapshots, and
// keys declared by more than one hook. Acknowledgement is the only
// strongly consistent step: it re-GETs and validates before patching.
func (s *Source) Snapshot(ctx context.Context) (runtime.SourceSnapshot, error) {
	if s.labelFile != "" {
		return s.snapshotFromLabelFile()
	}
	return s.snapshotFromStore()
}

// snapshotFromStore reads the watched RoleInstance from the informer
// store. It errors until Run has synced the cache; a missing object means
// the RoleInstance is deleted, which is an empty observation.
func (s *Source) snapshotFromStore() (runtime.SourceSnapshot, error) {
	store := s.currentStore()
	if store == nil {
		return runtime.SourceSnapshot{}, errNotSynced
	}
	item, exists, err := store.GetByKey(s.namespace + "/" + s.name)
	if err != nil {
		return runtime.SourceSnapshot{}, fmt.Errorf("rbg: read observed RoleInstance %s/%s: %w", s.namespace, s.name, err)
	}
	if !exists {
		// Deleted object: no signal can be executable.
		return runtime.SourceSnapshot{Token: &observation{signals: map[runtime.Signal]bool{}}}, nil
	}
	object, ok := item.(*unstructured.Unstructured)
	if !ok {
		return runtime.SourceSnapshot{}, fmt.Errorf("rbg: unexpected observed object type %T", item)
	}
	return s.buildSnapshot(object)
}

// snapshotFromLabelFile builds the snapshot from the projected Pod labels.
// In this mode "executable signal" degrades to "label present with the
// declared value". The replay cache hides signals that were recently
// acknowledged or observed as stale while the mirrored file still shows
// them.
func (s *Source) snapshotFromLabelFile() (runtime.SourceSnapshot, error) {
	content, err := os.ReadFile(s.labelFile)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// A missing file is an empty observation: no signal is
			// executable, which is a valid state.
			return runtime.SourceSnapshot{Token: &observation{signals: map[runtime.Signal]bool{}}}, nil
		}
		return runtime.SourceSnapshot{}, fmt.Errorf("rbg: read label file %s: %w", s.labelFile, err)
	}

	labels, malformed := parseDownwardAPILabels(string(content))
	if len(malformed) > 0 {
		s.logger.Warn("skipping malformed label file lines", "path", s.labelFile, "count", len(malformed))
	}

	now := time.Now()
	token := &observation{signals: map[runtime.Signal]bool{}}
	signals := make([]runtime.Signal, 0, len(labels))
	for key, value := range labels {
		signal := runtime.Signal{Key: key, Value: value}
		if s.isSuppressed(signal, now) {
			continue
		}
		token.signals[signal] = true
		signals = append(signals, signal)
	}
	sort.Slice(signals, func(i, j int) bool {
		if signals[i].Key != signals[j].Key {
			return signals[i].Key < signals[j].Key
		}
		return signals[i].Value < signals[j].Value
	})
	return runtime.SourceSnapshot{Signals: signals, Token: token}, nil
}

// parseDownwardAPILabels parses the downwardAPI label file format: one
// `key="value"` pair per line. Malformed lines are skipped and returned so
// callers can log them; missing keys and invalid separators never panic.
func parseDownwardAPILabels(content string) (labels map[string]string, malformed []string) {
	labels = map[string]string{}
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		index := strings.Index(line, "=")
		if index <= 0 {
			malformed = append(malformed, line)
			continue
		}
		key := strings.TrimSpace(line[:index])
		value := strings.TrimSpace(line[index+1:])
		if unquoted, err := strconv.Unquote(value); err == nil {
			value = unquoted
		}
		if key == "" {
			malformed = append(malformed, line)
			continue
		}
		labels[key] = value
	}
	return labels, malformed
}

// suppressSignal hides a signal from file snapshots until the TTL expires,
// covering the mirror propagation window after an acknowledgement or a
// stale observation.
func (s *Source) suppressSignal(signal runtime.Signal) {
	s.suppressMu.Lock()
	defer s.suppressMu.Unlock()
	if s.suppressed == nil {
		s.suppressed = map[runtime.Signal]time.Time{}
	}
	s.suppressed[signal] = time.Now()
}

func (s *Source) isSuppressed(signal runtime.Signal, now time.Time) bool {
	s.suppressMu.Lock()
	defer s.suppressMu.Unlock()
	ackedAt, present := s.suppressed[signal]
	if !present {
		return false
	}
	if now.Sub(ackedAt) > s.replayTTL {
		delete(s.suppressed, signal)
		return false
	}
	return true
}

func (s *Source) get(ctx context.Context) (*unstructured.Unstructured, error) {
	object, err := s.client.Resource(RoleInstanceGVR).Namespace(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("rbg: get RoleInstance %s/%s: %w", s.namespace, s.name, err)
	}
	return object, nil
}

// buildSnapshot interprets one observed RoleInstance.
func (s *Source) buildSnapshot(object *unstructured.Unstructured) (runtime.SourceSnapshot, error) {
	labels := object.GetLabels()
	token := &observation{
		uid:             object.GetUID(),
		resourceVersion: object.GetResourceVersion(),
		signals:         map[runtime.Signal]bool{},
	}

	hookState, hasState := labels[HookStateLabel]
	if !hasState || hookState == "" {
		// No lifecycle operation is active: nothing is executable.
		return runtime.SourceSnapshot{Token: token}, nil
	}
	if !knownHookStates[hookState] {
		return runtime.SourceSnapshot{}, fmt.Errorf(
			"rbg: RoleInstance %s/%s has unknown hook-state %q; failing closed",
			s.namespace, s.name, hookState)
	}
	token.hookState = hookState

	hook, executable := executableHooks[hookState]
	if !executable {
		// Updating, Recovering, and Terminating are valid but not
		// executable.
		return runtime.SourceSnapshot{Token: token}, nil
	}

	parsed, err := parseLifecycle(object)
	if err != nil {
		return runtime.SourceSnapshot{}, err
	}
	hookConfig := parsed.hookFor(hook)
	if hookConfig == nil {
		return runtime.SourceSnapshot{Token: token}, nil
	}
	for key, wantValue := range hookConfig.SignalLabels {
		// A missing label never matches; a present label with an empty
		// value matches only an empty declared value.
		labelValue, present := labels[key]
		if present && labelValue == wantValue {
			signal := runtime.Signal{Key: key, Value: wantValue}
			token.signals[signal] = true
		}
	}

	// The replay cache hides recently acknowledged or stale signals so
	// observation lag cannot replay them into execution.
	signals := make([]runtime.Signal, 0, len(token.signals))
	now := time.Now()
	for signal := range token.signals {
		if !s.isSuppressed(signal, now) {
			signals = append(signals, signal)
		}
	}
	sort.Slice(signals, func(i, j int) bool {
		if signals[i].Key != signals[j].Key {
			return signals[i].Key < signals[j].Key
		}
		return signals[i].Value < signals[j].Value
	})
	return runtime.SourceSnapshot{Signals: signals, Token: token}, nil
}

// parseLifecycle strictly decodes spec.lifecycle and rejects keys declared
// by more than one hook.
func parseLifecycle(object *unstructured.Unstructured) (*lifecycle, error) {
	raw, found, err := unstructured.NestedMap(object.Object, "spec", "lifecycle")
	if err != nil {
		return nil, fmt.Errorf("rbg: invalid spec.lifecycle on RoleInstance %s/%s: %w",
			object.GetNamespace(), object.GetName(), err)
	}
	parsed := &lifecycle{}
	if !found || raw == nil {
		return parsed, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("rbg: encode spec.lifecycle: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(parsed); err != nil {
		return nil, fmt.Errorf("rbg: invalid spec.lifecycle on RoleInstance %s/%s: %w",
			object.GetNamespace(), object.GetName(), err)
	}

	seenKeys := map[string]string{}
	for hookName, hook := range map[string]*lifecycleHook{
		"preUpdate":  parsed.PreUpdate,
		"preRecover": parsed.PreRecover,
		"postReady":  parsed.PostReady,
		"preDelete":  parsed.PreDelete,
	} {
		if hook == nil {
			continue
		}
		for key := range hook.SignalLabels {
			if owner, exists := seenKeys[key]; exists {
				return nil, fmt.Errorf(
					"rbg: signal key %q declared by both %s and %s on RoleInstance %s/%s; failing closed",
					key, owner, hookName, object.GetNamespace(), object.GetName())
			}
			seenKeys[key] = hookName
		}
	}
	return parsed, nil
}

// Ack removes the label for one completed signal after revalidating the
// observation: the RoleInstance UID, hook-state, current lifecycle
// declaration, and label value must all be unchanged. File-based snapshots
// carry no original UID or hook-state (see WithFileSnapshot), so their
// acknowledgement validates the current state only. Stale observations
// never write. The write is a merge patch carrying the latest
// resourceVersion; conflicts are retried after a fresh GET.
func (s *Source) Ack(ctx context.Context, snapshot runtime.SourceSnapshot, signal runtime.Signal) (runtime.AckStatus, error) {
	token, ok := snapshot.Token.(*observation)
	if !ok || token == nil {
		return 0, errors.New("rbg: acknowledgement requires a snapshot from this source")
	}
	if !token.signals[signal] {
		return 0, fmt.Errorf("rbg: signal %s=%s was not part of the observed snapshot", signal.Key, signal.Value)
	}

	for attempt := 0; attempt < maxAckConflicts; attempt++ {
		object, err := s.get(ctx)
		if err != nil {
			// A deleted RoleInstance invalidates the observation
			// definitively; retrying would never succeed.
			if kerrors.IsNotFound(err) {
				s.suppressSignal(signal)
				return runtime.Stale, nil
			}
			return 0, err
		}
		stale, err := s.validateForAck(object, token, signal)
		if err != nil {
			return 0, err
		}
		if stale {
			s.suppressSignal(signal)
			return runtime.Stale, nil
		}

		patch, err := buildLabelRemovalPatch(object.GetResourceVersion(), signal.Key)
		if err != nil {
			return 0, err
		}
		_, err = s.client.Resource(RoleInstanceGVR).Namespace(s.namespace).Patch(
			ctx, s.name, types.MergePatchType, patch, metav1.PatchOptions{})
		if err == nil {
			s.suppressSignal(signal)
			return runtime.Acknowledged, nil
		}
		if kerrors.IsConflict(err) {
			s.logger.Warn("merge patch conflicted; revalidating", "roleInstance", s.namespace+"/"+s.name, "signalKey", signal.Key)
			continue
		}
		return 0, fmt.Errorf("rbg: patch RoleInstance %s/%s: %w", s.namespace, s.name, err)
	}
	return 0, fmt.Errorf("rbg: acknowledgement for signal %s hit too many conflicts", signal.Key)
}

// validateForAck returns stale=true when the current object no longer
// matches the original observation. It returns an error for protocol errors
// (unknown state, malformed lifecycle) rather than silently writing.
func (s *Source) validateForAck(object *unstructured.Unstructured, token *observation, signal runtime.Signal) (bool, error) {
	labels := object.GetLabels()
	hookState := labels[HookStateLabel]
	if token.uid != "" {
		// API-based observation: compare against the original.
		if object.GetUID() != token.uid {
			return true, nil
		}
		if hookState != token.hookState {
			return true, nil
		}
		// File-based observations carry no original UID or hook-state:
		// the current-state checks below are the whole validation.
	}
	hook, executable := executableHooks[hookState]
	if !executable {
		return true, nil
	}
	parsed, err := parseLifecycle(object)
	if err != nil {
		return false, err
	}
	hookConfig := parsed.hookFor(hook)
	if hookConfig == nil {
		return true, nil
	}
	if want, declared := hookConfig.SignalLabels[signal.Key]; !declared || want != signal.Value {
		return true, nil
	}
	labelValue, present := labels[signal.Key]
	if !present || labelValue != signal.Value {
		return true, nil
	}
	return false, nil
}

func buildLabelRemovalPatch(resourceVersion, key string) ([]byte, error) {
	patch := map[string]any{
		"metadata": map[string]any{
			"resourceVersion": resourceVersion,
			"labels": map[string]any{
				key: nil,
			},
		},
	}
	encoded, err := json.Marshal(patch)
	if err != nil {
		return nil, fmt.Errorf("rbg: build merge patch: %w", err)
	}
	return encoded, nil
}
