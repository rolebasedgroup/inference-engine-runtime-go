package rbg

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Wire names from the RoleInstance lifecycle-hook proposal. These values are
// normative for the protocol and intentionally centralized here.
const (
	// Group and Version identify the RoleInstance API surface used by the
	// lifecycle-hook protocol.
	Group   = "workloads.x-k8s.io"
	Version = "v1alpha2"
	// Resource is the plural resource name of RoleInstance.
	Resource = "roleinstances"

	// HookStateLabel is the exact hook-state label key on RoleInstance.
	HookStateLabel = "rbg.workloads.x-k8s.io/role-instance-hook-state"

	// maxAckConflicts bounds the retry loop after merge-patch conflicts.
	maxAckConflicts = 5
)

// RoleInstanceGVR is the GroupVersionResource watched by the source.
var RoleInstanceGVR = schema.GroupVersionResource{
	Group:    Group,
	Version:  Version,
	Resource: Resource,
}

// Hook-state values. The four pre/post states are executable; the three
// in-progress states are valid but produce no executable signal. A missing
// label means no lifecycle operation is active.
const (
	HookStatePreUpdate   = "PreUpdate"
	HookStateUpdating    = "Updating"
	HookStatePreRecover  = "PreRecover"
	HookStateRecovering  = "Recovering"
	HookStatePostReady   = "PostReady"
	HookStatePreDelete   = "PreDelete"
	HookStateTerminating = "Terminating"
)

// executableHooks maps executable hook-state values to their hook names in
// the RoleInstance.spec.lifecycle snapshot.
var executableHooks = map[string]string{
	HookStatePreUpdate:  "preUpdate",
	HookStatePreRecover: "preRecover",
	HookStatePostReady:  "postReady",
	HookStatePreDelete:  "preDelete",
}

// knownHookStates contains every valid hook-state value. Unknown values are
// invalid: the source must fail closed and expose diagnostics.
var knownHookStates = map[string]bool{
	HookStatePreUpdate:   true,
	HookStateUpdating:    true,
	HookStatePreRecover:  true,
	HookStateRecovering:  true,
	HookStatePostReady:   true,
	HookStatePreDelete:   true,
	HookStateTerminating: true,
}

// lifecycle is the strict decoding target for spec.lifecycle. Every hook
// contains only signalLabels.
type lifecycle struct {
	PreUpdate  *lifecycleHook `json:"preUpdate,omitempty"`
	PreRecover *lifecycleHook `json:"preRecover,omitempty"`
	PostReady  *lifecycleHook `json:"postReady,omitempty"`
	PreDelete  *lifecycleHook `json:"preDelete,omitempty"`
}

type lifecycleHook struct {
	SignalLabels map[string]string `json:"signalLabels"`
}

// hookFor returns the lifecycle hook entry for a hook name.
func (l *lifecycle) hookFor(hook string) *lifecycleHook {
	switch hook {
	case "preUpdate":
		return l.PreUpdate
	case "preRecover":
		return l.PreRecover
	case "postReady":
		return l.PostReady
	case "preDelete":
		return l.PreDelete
	default:
		return nil
	}
}
