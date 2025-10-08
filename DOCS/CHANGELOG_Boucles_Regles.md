# CHANGELOG - Prévention des Boucles Infinies dans les Règles

**Date:** 2025-10-08  
**Version:** RT v2  
**Auteur:** Cascade AI

---

## 🎯 Objectif

1. Empêcher les règles automatiques de se réappliquer en boucle lors de l'édition manuelle
2. **Empêcher les règles de se réappliquer lors de l'import Ambre quotidien** (écrasant les modifications manuelles)
3. Améliorer la gestion de l'ActionDate et de l'ActionStatus

---

## ✅ Changements Implémentés

### **1. Solution 5: Forcer ActionDate lors du changement d'Action**

#### Fichier: `ReconciliationService.cs`
**Méthode:** `EnsureActionDefaults(Reconciliation r)`

**AVANT:**
```csharp
else
{
    if (!r.ActionStatus.HasValue) r.ActionStatus = false; // PENDING
    if (!r.ActionDate.HasValue) r.ActionDate = DateTime.Now;
}
```

**APRÈS:**
```csharp
else
{
    if (!r.ActionStatus.HasValue) r.ActionStatus = false; // PENDING
    // FIX: ALWAYS update ActionDate when Action changes
    r.ActionDate = DateTime.Now;
}
```

**Impact:** L'ActionDate est maintenant **toujours** mise à jour quand une Action est modifiée, même si elle avait déjà une valeur.

---

### **2. Solution 4: N/A auto-DONE (ActionStatus=true)**

#### Fichier: `ReconciliationService.cs`
**Méthode:** `EnsureActionDefaults(Reconciliation r)`

**AVANT:**
```csharp
if (isNa)
{
    r.ActionStatus = null;
    r.ActionDate = null;
}
```

**APRÈS:**
```csharp
if (isNa)
{
    // FIX: N/A action should be marked as DONE, not null
    r.ActionStatus = true;
    r.ActionDate = DateTime.Now;
}
```

**Impact:** Quand Action = N/A, l'ActionStatus est automatiquement mis à `DONE` (true) au lieu de `null`.

---

#### Fichier: `AmbreReconciliationUpdater.cs`
**Méthode:** `ApplyTruthTableRulesAsync()` - Finalize section

**AVANT:**
```csharp
if (isNa)
{
    rec.ActionStatus = null;
    rec.ActionDate = null;
}
else
{
    if (!rec.ActionStatus.HasValue) rec.ActionStatus = false;
    if (!rec.ActionDate.HasValue) rec.ActionDate = nowLocal;
}
```

**APRÈS:**
```csharp
if (isNa)
{
    // FIX: N/A action should be marked as DONE, not null
    rec.ActionStatus = true;
    rec.ActionDate = nowLocal;
}
else
{
    if (!rec.ActionStatus.HasValue) rec.ActionStatus = false;
    // FIX: ALWAYS set ActionDate when Action is set
    rec.ActionDate = nowLocal;
}
```

**Impact:** Même correction pour l'import Ambre - N/A = DONE et ActionDate toujours forcée.

---

### **3. Solution 2: Changer Scope des règles à Import**

#### Fichier: `TruthTableRepository.cs`
**Méthode:** `SeedDefaultRulesAsync()`

**AVANT:**
```csharp
var rules = new List<TruthRule>
{
    new TruthRule { RuleId = "LEGACY_P_COLLECTION_C_GROUP", Scope = RuleScope.Both, ... },
    new TruthRule { RuleId = "LEGACY_R_INCOMING_PAYMENT_REISSUANCE_ACK", Scope = RuleScope.Both, ... },
    // ... toutes les règles avec Scope = Both
}
```

**APRÈS:**
```csharp
var rules = new List<TruthRule>
{
    // NOTE: Scope=Import to prevent infinite loops during manual edits
    new TruthRule { RuleId = "LEGACY_P_COLLECTION_C_GROUP", Scope = RuleScope.Import, ... },
    new TruthRule { RuleId = "LEGACY_R_INCOMING_PAYMENT_REISSUANCE_ACK", Scope = RuleScope.Import, ... },
    // ... toutes les règles avec Scope = Import
}
```

**Impact:** Les règles automatiques ne se déclenchent plus lors de l'édition manuelle (`RuleScope.Edit`), uniquement lors de l'import Ambre (`RuleScope.Import`).

**Règles modifiées (toutes passées à `Scope = RuleScope.Import`):** 26 règles

---

### **4. Solution 6: Ajouter conditions pour éviter réapplication lors de l'import quotidien**

#### Fichier: `TruthTableRepository.cs`
**Méthode:** `SeedDefaultRulesAsync()`

**Problème:** Lors de l'import Ambre quotidien, certaines règles se réappliquent **même si l'utilisateur a déjà traité la ligne**, écrasant ses modifications.

**Solution A: Ajouter `CurrentActionId = null` pour les règles Pivot et Receivable stables**

**AVANT:**
```csharp
new TruthRule { RuleId = "LEGACY_P_COLLECTION_C_NOTGROUP", Scope = RuleScope.Import, AccountSide = "P", TransactionType = "COLLECTION", Sign = "C", OutputActionId = 7, ... }
```

**APRÈS:**
```csharp
new TruthRule { RuleId = "LEGACY_P_COLLECTION_C_NOTGROUP", Scope = RuleScope.Import, AccountSide = "P", TransactionType = "COLLECTION", Sign = "C", CurrentActionId = null, OutputActionId = 7, ... }
```

**Impact:** La règle ne s'applique que si `Action` n'a pas encore été définie (null). Si l'utilisateur a déjà mis une Action, la règle ne se réapplique pas.

**Règles modifiées (10 règles Pivot):**
- `LEGACY_P_COLLECTION_C_NOTGROUP`
- `LEGACY_P_COLLECTION_D`
- `LEGACY_P_PAYMENT_D`
- `LEGACY_P_PAYMENT_C`
- `LEGACY_P_ADJUSTMENT`
- `LEGACY_P_XCL_LOADER_C`
- `LEGACY_P_XCL_LOADER_D`
- `LEGACY_P_TRIGGER_C`
- `LEGACY_P_TRIGGER_D`
- `LEGACY_P_MANUAL_OUTGOING`

**Règles modifiées (4 règles Receivable autres):**
- `LEGACY_R_DIRECT_DEBIT`
- `LEGACY_R_OUTGOING_PAYMENT_NOTINI`
- `LEGACY_R_EXTERNAL_DEBIT_PAYMENT`
- `LEGACY_R_OUTGOING_PAYMENT_INIT`

---

**Solution B: Ajouter `IsFirstRequest = true` pour les règles Receivable INCOMING_PAYMENT**

**AVANT:**
```csharp
new TruthRule { RuleId = "LEGACY_R_INCOMING_PAYMENT_REISSUANCE_ACK", Scope = RuleScope.Import, AccountSide = "R", GuaranteeType = "REISSUANCE", TransactionType = "INCOMING_PAYMENT", MTStatusAcked = true, OutputActionId = 1, ... }
```

**APRÈS:**
```csharp
new TruthRule { RuleId = "LEGACY_R_INCOMING_PAYMENT_REISSUANCE_ACK", Scope = RuleScope.Import, AccountSide = "R", GuaranteeType = "REISSUANCE", TransactionType = "INCOMING_PAYMENT", MTStatusAcked = true, IsFirstRequest = true, OutputActionId = 1, ... }
```

**Impact:** La règle ne s'applique que lors de la **première apparition** de la ligne dans Ambre (IsFirstRequest=true). Les imports suivants ne réappliquent pas la règle.

**Règles modifiées (6 règles Receivable INCOMING_PAYMENT):**
- `LEGACY_R_INCOMING_PAYMENT_REISSUANCE_ACK`
- `LEGACY_R_INCOMING_PAYMENT_ISSUANCE_EMAIL`
- `LEGACY_R_INCOMING_PAYMENT_ADVISING_ACK`
- `LEGACY_R_INCOMING_PAYMENT_REISSUANCE_NACK`
- `LEGACY_R_INCOMING_PAYMENT_ADVISING_NACK`
- `LEGACY_R_INCOMING_PAYMENT_ISSUANCE_NOEMAIL`

**Exception:** `LEGACY_R_INCOMING_PAYMENT_OTHER` - Pas d'output, donc pas de risque de réapplication

---

### **5. Solution 7: Ajouter Messages aux Règles Critiques**

#### Fichier: `TruthTableRepository.cs`
**Méthode:** `SeedDefaultRulesAsync()`

**Problème:** Les messages des règles n'étaient pas ajoutés aux Comments car la plupart des règles n'avaient pas de `Message` défini.

**Solution:** Ajouter des messages explicatifs aux règles critiques (règles qui nécessitent une action utilisateur ou une notification importante).

**Règles avec Messages Ajoutés (8 règles):**

| RuleId | Message |
|--------|---------|
| `HEUR_R_INCOMING_FIRST_REQUEST` | "First claim request - automatic action assigned" |
| `HEUR_R_INCOMING_REMIND_30D_REISSUANCE_NOTACK` | "⚠️ Reminder required - MT791 not acknowledged (30+ days)" |
| `LEGACY_P_COLLECTION_C_NOTGROUP` | "⚠️ Collection Credit without amount match - investigation required" |
| `LEGACY_R_INCOMING_PAYMENT_ISSUANCE_EMAIL` | "First claim email sent - awaiting response" |
| `LEGACY_R_INCOMING_PAYMENT_REISSUANCE_NACK` | "⚠️ MT791 not acknowledged - manual follow-up required" |
| `LEGACY_R_INCOMING_PAYMENT_ADVISING_NACK` | "⚠️ MT791 not acknowledged - manual follow-up required" |
| `LEGACY_R_INCOMING_PAYMENT_ISSUANCE_NOEMAIL` | "⚠️ No email communication ID - manual claim required" |

**Règles avec Messages Existants (4 règles):**
- `HEUR_R_INCOMING_REMIND_30D_ISSUANCE`
- `HEUR_R_INCOMING_REMIND_30D_REISSUANCE_ACK`
- `LEGACY_R_INCOMING_PAYMENT_REISSUANCE_ACK`
- `LEGACY_R_INCOMING_PAYMENT_ADVISING_ACK`

**Impact:** Quand une règle critique se déclenche, un message explicatif est automatiquement ajouté dans le champ `Comments` avec le format:
```
[2025-10-08 12:23] username: [Rule LEGACY_R_INCOMING_PAYMENT_REISSUANCE_NACK] ⚠️ MT791 not acknowledged - manual follow-up required
```

**Note:** Les messages avec ⚠️ indiquent des situations nécessitant une attention particulière de l'utilisateur.

---

## 📊 Résumé des Bénéfices

| Problème | Solution | Statut |
|----------|----------|--------|
| **Boucles infinies lors de l'édition** | Scope=Import pour toutes les règles auto | ✅ Résolu |
| **Réapplication lors de l'import quotidien** | CurrentActionId=null + IsFirstRequest=true | ✅ Résolu |
| **Messages des règles non ajoutés** | Ajouter Message aux règles critiques | ✅ Résolu |
| **ActionDate pas mise à jour** | Forcer ActionDate = DateTime.Now | ✅ Résolu |
| **N/A avec ActionStatus=null** | N/A → ActionStatus=true (DONE) | ✅ Résolu |

---

## 🧪 Tests Recommandés

### Test 1: Boucle infinie (édition manuelle)
```
1. Import Ambre → Règle applique Action = 1 (PENDING)
2. User change manuellement ActionStatus = DONE
3. Sauvegarder
4. ✅ Vérifier: ActionStatus reste DONE (règle ne se réapplique pas)
```

### Test 2: Réapplication lors de l'import quotidien (Pivot)
```
1. Import Ambre J1 → Règle LEGACY_P_COLLECTION_C_NOTGROUP applique Action = 7
2. User change manuellement Action = 4
3. Import Ambre J2 (même ligne réimportée)
4. ✅ Vérifier: Action reste 4 (règle ne se réapplique pas car CurrentActionId != null)
```

### Test 3: Réapplication lors de l'import quotidien (Receivable INCOMING_PAYMENT)
```
1. Import Ambre J1 → Règle LEGACY_R_INCOMING_PAYMENT_REISSUANCE_ACK applique Action = 1
2. User change manuellement Action = 5
3. Import Ambre J2 (même ligne réimportée)
4. ✅ Vérifier: Action reste 5 (règle ne se réapplique pas car IsFirstRequest=false)
```

### Test 4: Action N/A
```
1. Sélectionner Action = N/A
2. ✅ Vérifier: ActionStatus = DONE automatiquement
3. ✅ Vérifier: ActionDate = maintenant
```

### Test 5: ActionDate forcée
```
1. Changer Action (manuellement ou par règle)
2. ✅ Vérifier: ActionDate = maintenant (même si elle avait déjà une valeur)
```

### Test 6: Messages des règles critiques
```
1. Import Ambre avec une ligne qui déclenche LEGACY_R_INCOMING_PAYMENT_REISSUANCE_NACK
2. ✅ Vérifier: Comments contient "[Rule LEGACY_R_INCOMING_PAYMENT_REISSUANCE_NACK] ⚠️ MT791 not acknowledged - manual follow-up required"
3. ✅ Vérifier: Le message est horodaté avec [YYYY-MM-DD HH:mm] username:
```

---

## 📝 Notes Techniques

### Scope des règles
- **`RuleScope.Import`**: Règle s'applique uniquement lors de l'import Ambre
- **`RuleScope.Edit`**: Règle s'applique uniquement lors de l'édition manuelle
- **`RuleScope.Both`**: Règle s'applique dans les deux cas (⚠️ risque de boucle)

### Comportement ActionStatus
- **Action = N/A**: `ActionStatus = true` (DONE)
- **Action != N/A**: `ActionStatus = false` (PENDING) par défaut
- **ActionDate**: Toujours forcée à `DateTime.Now` quand Action change

---

## 🔄 Migration

**Aucune migration nécessaire.**

Les règles existantes dans la base de données seront automatiquement mises à jour lors du prochain appel à `SeedDefaultRulesAsync()` (via `EnsureRulesTableAsync()`).

Les utilisateurs peuvent manuellement changer le Scope des règles via l'interface Rules Admin si besoin.

---

## 📚 Documentation Associée

- **`Solutions_Boucles_Regles.md`**: Document détaillé avec toutes les solutions proposées (y compris celles non implémentées)
- **`TruthRule.cs`**: Définition des règles et du RuleContext
- **`RulesEngine.cs`**: Moteur d'évaluation des règles

---

**Fin du CHANGELOG**
