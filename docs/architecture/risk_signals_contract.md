# Risk Signals — Canonical Contract (v1)

## Purpose

This document defines the canonical representation of a Risk Signal
as a Gold-level analytical artifact.

Risk Signals are designed not as classification flags,
but as operational signals that:
- are measurable
- trigger downstream actions
- can be monitored and analyzed over time

## Logical Entity: Risk Signal

A Risk Signal represents a single detected risk condition
for a vacancy at a given point in time.

### Core Identifiers

- vacancy_id
- load_dt

### Signal Attributes

- risk_signal_name        (string, snake_case)
- risk_signal_type        (content_based / employer_based / behavior_based / technical)
- severity                (informational / warning / critical)
- blocking                (boolean)

### Signal Value

- signal_value            (boolean / numeric / categorical)
- risk_domain             (crypto / gambling / payments / baseline)

### Operational Semantics

- downstream_action       (monitor / analytics / alert / manual_review)

### Observability & Analysis

- false_positive_note     (free text, optional)

## Notes

- Not all fields are required to be implemented in Risk Signals v1.
- The initial implementation may aggregate signals into boolean flags
  at the vacancy level.
- This contract defines the long-term target model for Risk Signals
  as first-class operational entities.
