## Assumptions

This project describes a target-state data architecture for a Stripe-like payment platform.

Assumed scale:
- 10M+ transactions/day
- 100M+ semi-structured events/day
- near-real-time fraud scoring
- regulatory constraints: GDPR, PCI-DSS, CCPA

All performance numbers are architectural targets, not benchmark results.