# Action and KPI Rules After AMBRE Import

This document describes all rules that can set or change Action and/or KPI in the reconciliation workflow after an AMBRE import. It references concrete implementations in the codebase to serve as the single source of truth.

- Version: 2025-09-22
- Owner: RecoTool

---

## Source of Truth (Code References)

- Compute Auto Action: `RecoTool/Services/ReconciliationService.cs` → `ComputeAutoAction(...)`
- Cross-side overrides on insert: `RecoTool/Services/Ambre/AmbreReconciliationUpdater.cs` → `ApplyCrossSideActionRules(...)`
- KPI mapping (categorization): `RecoTool/Services/TransformationService.cs` → `DetermineTransactionType(...)`, `ApplyAutomaticCategorization(...)`
- Legacy/auxiliary rules (batch apply): `RecoTool/Services/Rules/ReconciliationRules.cs` (used by `ApplyAutomaticRulesAsync`)
- Auto-matching shortcut: `RecoTool/Services/ReconciliationService.cs` → `PerformAutomaticMatchingAsync(...)`

---

## When Rules Are Applied

- New reconciliation rows (created from new AMBRE rows):
  - KPI is computed via `TransformationService.ApplyAutomaticCategorization(...)`.
  - Action is computed via `ReconciliationService.ComputeAutoAction(...)` unless overridden by cross-side rules.
  - Cross-side overrides can force Actions when both Pivot and Receivable exist for the same BGI (DWINGS_InvoiceID).

- Updated reconciliation rows (from updated AMBRE rows):
  - User-maintained fields (Action, KPI, Comments, Assignee, etc.) are preserved.
  - DWINGS references can be backfilled if empty; existing links are not overwritten.
  - No automatic recompute of Action/KPI is performed by the import.

- Deleted AMBRE rows:
  - Matching reconciliation rows are archived (set `DeleteDate`).

- Optional routines (manual or scheduled):
  - `ApplyAutomaticRulesAsync(...)` can recompute Action/KPI using `ReconciliationRules` (legacy mapping per account side).
  - `PerformAutomaticMatchingAsync(...)` can set Action/KPI for pairs detected as matched by heuristics.

---

## ComputeAutoAction Rules (ReconciliationService.ComputeAutoAction)

Inputs:
- `transactionType` from `DetermineTransactionType(label, isPivot, category)`.
- `a` = AMBRE row (`DataAmbre`), `r` = Reconciliation row.
- `country` (to resolve Pivot vs Receivable), `paymentMethod` (DWINGS: `INCOMING_PAYMENT`, `OUTGOING_PAYMENT`, `DIRECT_DEBIT`, `MANUAL_OUTGOING`).
- `today` = business date (passed as `DateTime.Today`).

General guards:
- Never overrides a user-set Action: if `r.Action` already has a value, returns null (no change).
- `isPivot = a.IsPivotAccount(country.CNT_AmbrePivot)`; `isReceivable = !isPivot`.

Rules:
- COLLECTION and `r.TriggerDate` is blank → `Action = Trigger`.
- Pivot “transitory” movement (if `a.Reconciliation_Num` contains `BGPMT`) and `a.Operation_Date < today - 1` → `Action = Investigate`.
- If `r.TriggerDate <= today - 1` AND not matched AND no manual match set → `Action = Match`.
  - Matched is interpreted as `a.DeleteDate.HasValue`.
- If Receivable account:
  - Payment method = `MANUAL_OUTGOING` or `transactionType = MANUAL_OUTGOING` → `Action = Trigger`.
  - Payment method = `OUTGOING_PAYMENT` or `transactionType = OUTGOING_PAYMENT` → `Action = Execute`.
  - Payment method = `DIRECT_DEBIT` or `transactionType = DIRECT_DEBIT` → no auto action (null).
  - Payment method = `INCOMING_PAYMENT` or `transactionType = INCOMING_PAYMENT`:
    - If first request is blank (interpreted as `a.CreationDate` null or equals `today`) → `Action = Request`.
    - Else if `r.ToRemindDate <= today - 30` → `Action = Remind`.
- Else (no rule matched) → null (no automatic Action).

Notes:
- `ComputeAutoAction` is only applied for new rows during import (through `AmbreReconciliationUpdater`).

---

## Cross-Side Overrides on Insert (AmbreReconciliationUpdater.ApplyCrossSideActionRules)

For the batch of newly inserted reconciliation rows:
- Group by `DWINGS_InvoiceID` (BGI), case-insensitive.
- If both sides present for the same BGI:
  - Pivot side → `Action = Match`.
  - Receivable side → `Action = Trigger`.

These overrides are applied even if `ComputeAutoAction` produced a different action.

---

## KPI Mapping Rules (TransformationService)

Two steps:
1) Detect `TransactionType` via `DetermineTransactionType(label, isPivot, category)`.
   - Pivot side: if `category` is set from ATC mapping, it directly maps to a `TransactionType`.
   - Otherwise inferred from label tokens: `COLLECTION`, `PAYMENT`/`AUTOMATIC REFUND`, `ADJUSTMENT`, `XCL LOADER`, `TRIGGER`, else `TO_CATEGORIZE`.
   - Receivable side: inferred from label tokens: `INCOMING PAYMENT`, `DIRECT DEBIT`, `MANUAL OUTGOING`, `OUTGOING PAYMENT`, `EXTERNAL DEBIT PAYMENT`, else `TO_CATEGORIZE`.

2) Map to KPI via `ApplyAutomaticCategorization(transactionType, signedAmount, isPivot, guaranteeType)`:

- COLLECTION
  - Credit → KPI = `PaidButNotReconciled`
  - Debit → KPI = `CorrespondentChargesToBeInvoiced`
- PAYMENT
  - Debit → KPI = `CorrespondentChargesToBeInvoiced`
  - Credit → KPI = `ITIssues`
- ADJUSTMENT
  - Any sign → KPI = `PaidButNotReconciled`
- XCL_LOADER
  - Credit → KPI = `PaidButNotReconciled`
  - Debit → KPI = `UnderInvestigation`
- TRIGGER
  - Credit → KPI = `CorrespondentChargesToBeInvoiced`
  - Debit → KPI = `UnderInvestigation`
- INCOMING_PAYMENT → KPI = `NotClaimed`
- DIRECT_DEBIT → KPI = `ITIssues`
- MANUAL_OUTGOING → KPI = `CorrespondentChargesPendingTrigger`
- OUTGOING_PAYMENT → KPI = `CorrespondentChargesPendingTrigger`
- EXTERNAL_DEBIT_PAYMENT → KPI = `NotClaimed`
- TO_CATEGORIZE or null → KPI = `ITIssues`

Notes:
- During import, only the KPI result from this mapping is persisted. The Action from this mapping is not used for import (Action comes from `ComputeAutoAction` and cross-side rules).

---

## Legacy / Auxiliary Rule Sets

- Batch Apply (optional): `ReconciliationService.ApplyAutomaticRulesAsync(...)`
  - Uses `ReconciliationRules.ApplyPivotRules`/`ApplyReceivableRules` to set Action & KPI based on transaction type and (for receivable) a simple `guaranteeType` token extracted from label.

- Auto-Matching Shortcut: `ReconciliationService.PerformAutomaticMatchingAsync(...)`
  - When a receivable line’s invoice reference matches pivot lines:
    - Sets `Action = Match` and `KPI = PaidButNotReconciled` on all involved lines.
    - Adds a comment describing the auto-match.

---

## Precedence & Preservation

- **User override protection**: `ComputeAutoAction` will not set an action if `r.Action` already has a value.
- **Cross-side overrides**: On insert, cross-side rules can enforce Actions regardless of earlier computation.
- **Updates preserve user fields**: Import updates do not recompute Action/KPI and do not overwrite user-maintained fields.
- **DWINGS backfill is non-destructive**: Only fills empty `DWINGS_*` fields on updates; no overwrite of existing values.

---

## Inputs Driving the Rules

- `TransactionType` from `DetermineTransactionType(label, isPivot, category)` (category from ATC mapping for Pivot).
- Account side (Pivot/Receivable) from country config (`Country.CNT_AmbrePivot`).
- Dates and flags: `a.Operation_Date`, `r.TriggerDate`, `a.DeleteDate` (treated as matched), `a.CreationDate`, `r.ToRemindDate`.
- Payment method (DWINGS): `INCOMING_PAYMENT`, `OUTGOING_PAYMENT`, `DIRECT_DEBIT`, `MANUAL_OUTGOING`.
- Amount sign for KPI mapping (`SignedAmount > 0` → credit).
- Optional `guaranteeType` token in label (legacy rules).

---

## Practical UI Notes

- In `ReconciliationView.xaml`, Action, KPI, and related fields can be edited by users. Clearing selections is supported for Action/KPI/Incident Type in the grid (blank option persists null).
- Newly added columns (Receiver, MT Status, MT Error message) aid context but do not affect Action/KPI computation.

---

## Glossary (Enums)

- `ActionType`: `Match`, `Trigger`, `Execute`, `Request`, `Remind`, `Investigate`, `DoPricing`, `ToClaim`, `Adjust`, `ToDoSDD`, `NA`, ...
- `KPIType`: `PaidButNotReconciled`, `CorrespondentChargesToBeInvoiced`, `CorrespondentChargesPendingTrigger`, `NotClaimed`, `ClaimedButNotPaid`, `UnderInvestigation`, `ITIssues`, ...
- `TransactionType` (pivot+receivable unified): `COLLECTION`, `PAYMENT`, `ADJUSTMENT`, `XCL_LOADER`, `TRIGGER`, `INCOMING_PAYMENT`, `DIRECT_DEBIT`, `MANUAL_OUTGOING`, `OUTGOING_PAYMENT`, `EXTERNAL_DEBIT_PAYMENT`, `TO_CATEGORIZE`.

---

## Change Log of this Document

- 2025-09-22: Initial version created from code references listed above.
