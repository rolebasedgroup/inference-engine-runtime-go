// Package rbg adapts the experimental RoleInstance lifecycle-hook protocol
// (KEP-527, proposed in sgl-project/rbg PR #546) to the patio SignalSource
// contract.
//
// STATUS: EXPERIMENTAL. The upstream protocol is still a documentation
// proposal; no released RBG controller implements it. This package is
// validated against the test CRD fixture in test/integration and must not be
// treated as version-compatible with a real RBG cluster until the upstream
// implementation is fixed and re-verified.
//
// All protocol wire names, snapshot interpretation, and safety checks are
// concentrated here; the core patio API never imports Kubernetes types.
package rbg
