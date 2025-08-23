# Cleanup Report

This document tracks unused/legacy elements found in the solution, with precise deletion guidance.

Format per entry:
- File path
- Element to remove (signature/name and, if applicable, exact lines)
- Concept/Reason
- Notes/Risks

---

## 1) RecoTool/Windows/ReconciliationPage.xaml.cs

- Element to remove: Legacy page-level reconciliation data pipeline
  - Removed members:
    - Property: `ReconciliationData` (ObservableCollection<ReconciliationViewData>)
    - Private fields: `_reconciliationData`, `_viewSource`
    - Method: `SetupDataGrid()`
    - Method: `FilterReconciliationItems(object item)`
    - Handlers: `DataGrid_SelectionChanged(...)`, `DataGrid_CellEditEnding(...)`
    - Calls to `_viewSource?.View?.Refresh()` in setters
    - Data load of page-level collection in `LoadDataAsync()`
  - Concept/Reason: Unused with the current architecture; each `ReconciliationView` manages its own data, filtering, and grid. No `ReconciliationDataGrid` exists in `ReconciliationPage.xaml`.
  - Notes/Risks: None identified. Page now only manages top filters (Accounts/Status), saved filters/views, and dynamic view containers.

- References verified as absent:
  - `ReconciliationDataGrid` not present in `RecoTool/Windows/ReconciliationPage.xaml`
  - No remaining references to `_viewSource`, `ReconciliationData`, `SetupDataGrid`, `FilterReconciliationItems`, `DataGrid_SelectionChanged`, `DataGrid_CellEditEnding` across the solution.

---

## Pending audit items

- Scan for unused classes, methods, and XAML resources across:
  - `RecoTool/`
  - `OfflineFirstAccess/`
- Special attention:
  - Unreferenced converters, helpers, and services
  - XAML styles/resources not used
  - Public methods never called (be mindful of reflection/bindings)
  - Files included in repo but not in .csproj

Findings will be appended here with precise file paths and elements to delete.

---

## 2) Temporary wpftmp project files (safe to delete)

- Files:
  - `RecoTool/RecoTool_1dlabqyo_wpftmp.csproj`
  - `RecoTool/RecoTool_df1ycdhu_wpftmp.csproj`
  - `RecoTool/RecoTool_fxxlui2f_wpftmp.csproj`
  - `RecoTool/RecoTool_i5xyowds_wpftmp.csproj`
  - `RecoTool/RecoTool_vetvtxbm_wpftmp.csproj`
- Concept/Reason: Temporary WPF designer project files created by Visual Studio. Not referenced by `RecoTool.sln` or needed for build/runtime.
- Notes/Risks: None. Deleting these does not affect the solution.

---

## 3) Candidate unused service (verify then delete)

- File: `RecoTool/Services/UserViewService.cs`
  - Element to remove: Entire file (`class UserViewService`)
  - Concept/Reason: No references found across the solution (only in its own file). `FilterPickerWindow` uses `UserFilterService` instead. Likely an older approach to saved views.
  - Notes/Risks: If a future "Saved Views" picker reuses this, keep. Otherwise safe to remove.

---

## 4) Candidate unused DTOs (verify then delete)

- File: `RecoTool/Services/DTOs/UISettings.cs`
  - Element: Entire file (`class UISettings`)
  - Reason: No usages found (only referenced in project include). Not read from config.
  - Risks: None known.

- File: `RecoTool/Services/DTOs/ImportDefaultSettings.cs`
  - Element: Entire file (`class ImportDefaultSettings`)
  - Reason: No usages found.
  - Risks: None known.

- File: `RecoTool/Services/DTOs/ExportSettings.cs`
  - Element: Entire file (`class ExportSettings`)
  - Reason: No usages found. `ExportService` does not reference it.
  - Risks: None known.

- File: `RecoTool/Services/DTOs/SyncSettings.cs`
  - Element: Entire file (`class SyncSettings`)
  - Reason: No usages found.
  - Risks: None known.

- File: `RecoTool/Services/DTOs/DataChanges.cs`
  - Element: Entire file (`internal class DataChanges`)
  - Reason: No usages found outside defining file. Import logic uses other models.
  - Risks: None known.

- File: `RecoTool/Services/DTOs/DataConflict.cs`
  - Element: Entire file (`class DataConflict`)
  - Reason: No usages found.
  - Risks: None known.

---

## 5) OfflineFirstAccess helper candidates (verify carefully)

- File: `OfflineFirstAccess/Helpers/EntityConverter.cs`
  - Element: Entire file
  - Reason: No references found outside the file/csproj.
  - Risks: Low within current solution; verify no runtime reflection.

- File: `OfflineFirstAccess/Helpers/ConfigConstants.cs`
  - Element: Entire file
  - Reason: No references found outside the file/csproj.
  - Risks: Low; appears unused.
