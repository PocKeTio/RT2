# Analyse - Règles qui se Réappliquent à l'Import Quotidien

**Date:** 2025-10-08  
**Problème:** Lors de l'import Ambre quotidien, certaines règles écrasent les modifications manuelles

---

## 🔴 Règles Problématiques Identifiées

### **Catégorie 1: Règles SANS condition de "première fois"**

Ces règles s'appliquent **TOUJOURS** lors de l'import, même si l'utilisateur a déjà traité la ligne.

| RuleId | Conditions | Output | Problème |
|--------|-----------|--------|----------|
| **LEGACY_P_COLLECTION_C_GROUP** | TransactionType=COLLECTION, IsAmountMatch=true, Sign=C | Action=4, KPI=18 | ✅ OK - Condition IsAmountMatch change rarement |
| **LEGACY_P_COLLECTION_C_NOTGROUP** | TransactionType=COLLECTION, Sign=C | Action=7, KPI=18 | ⚠️ **PROBLÈME** - Aucune condition temporelle |
| **LEGACY_P_COLLECTION_D** | TransactionType=COLLECTION, Sign=D | Action=1, KPI=19 | ⚠️ **PROBLÈME** - Aucune condition temporelle |
| **LEGACY_P_PAYMENT_D** | TransactionType=PAYMENT, Sign=D | Action=13, KPI=21 | ⚠️ **PROBLÈME** - Aucune condition temporelle |
| **LEGACY_P_PAYMENT_C** | TransactionType=PAYMENT, Sign=C | Action=7, KPI=22 | ⚠️ **PROBLÈME** - Aucune condition temporelle |
| **LEGACY_P_ADJUSTMENT** | TransactionType=ADJUSTMENT | Action=1, KPI=18 | ⚠️ **PROBLÈME** - Aucune condition temporelle |
| **LEGACY_P_XCL_LOADER_C** | TransactionType=XCL_LOADER, Sign=C | Action=6, KPI=18 | ⚠️ **PROBLÈME** - Aucune condition temporelle |
| **LEGACY_P_XCL_LOADER_D** | TransactionType=XCL_LOADER, Sign=D | Action=6, KPI=18 | ⚠️ **PROBLÈME** - Aucune condition temporelle |
| **LEGACY_P_TRIGGER_C** | TransactionType=TRIGGER, Sign=C | Action=6, KPI=18 | ⚠️ **PROBLÈME** - Aucune condition temporelle |
| **LEGACY_P_TRIGGER_D** | TransactionType=TRIGGER, Sign=D | Action=6, KPI=18 | ⚠️ **PROBLÈME** - Aucune condition temporelle |
| **LEGACY_P_MANUAL_OUTGOING** | TransactionType=MANUAL_OUTGOING | Action=4, KPI=15 | ⚠️ **PROBLÈME** - Aucune condition temporelle |

### **Catégorie 2: Règles Receivable SANS condition de "première fois"**

| RuleId | Conditions | Output | Problème |
|--------|-----------|--------|----------|
| **LEGACY_R_INCOMING_PAYMENT_REISSUANCE_ACK** | GuaranteeType=REISSUANCE, MTStatusAcked=true | Action=1, KPI=16 | ⚠️ **PROBLÈME** - Se réapplique chaque jour |
| **LEGACY_R_INCOMING_PAYMENT_ISSUANCE_EMAIL** | GuaranteeType=ISSUANCE, CommIdEmail=true | Action=1, KPI=16 | ⚠️ **PROBLÈME** - Se réapplique chaque jour |
| **LEGACY_R_INCOMING_PAYMENT_ADVISING_ACK** | GuaranteeType=ADVISING, MTStatusAcked=true | Action=1, KPI=16 | ⚠️ **PROBLÈME** - Se réapplique chaque jour |
| **LEGACY_R_INCOMING_PAYMENT_OTHER** | TransactionType=INCOMING_PAYMENT | (aucun output) | ✅ OK - Pas d'output |
| **LEGACY_R_INCOMING_PAYMENT_REISSUANCE_NACK** | GuaranteeType=REISSUANCE, MTStatusAcked=false | Action=2, KPI=17 | ⚠️ **PROBLÈME** - Se réapplique chaque jour |
| **LEGACY_R_INCOMING_PAYMENT_ADVISING_NACK** | GuaranteeType=ADVISING, MTStatusAcked=false | Action=2, KPI=17 | ⚠️ **PROBLÈME** - Se réapplique chaque jour |
| **LEGACY_R_INCOMING_PAYMENT_ISSUANCE_NOEMAIL** | GuaranteeType=ISSUANCE, CommIdEmail=false | Action=2, KPI=17 | ⚠️ **PROBLÈME** - Se réapplique chaque jour |
| **LEGACY_R_DIRECT_DEBIT** | TransactionType=DIRECT_DEBIT | Action=7, KPI=19 | ⚠️ **PROBLÈME** - Aucune condition temporelle |
| **LEGACY_R_OUTGOING_PAYMENT_NOTINI** | TransactionType=OUTGOING_PAYMENT, BgiStatusInitiated=false | Action=7, KPI=22 | ⚠️ **PROBLÈME** - Se réapplique chaque jour |
| **LEGACY_R_EXTERNAL_DEBIT_PAYMENT** | TransactionType=EXTERNAL_DEBIT_PAYMENT | Action=10, KPI=17 | ⚠️ **PROBLÈME** - Aucune condition temporelle |
| **LEGACY_R_OUTGOING_PAYMENT_INIT** | TransactionType=OUTGOING_PAYMENT, BgiStatusInitiated=true | Action=5, KPI=15 | ⚠️ **PROBLÈME** - Se réapplique chaque jour |

### **Catégorie 3: Règles OK (avec conditions temporelles)**

| RuleId | Conditions | Output | Pourquoi OK |
|--------|-----------|--------|-------------|
| **HEUR_R_INCOMING_FIRST_REQUEST** | IsFirstRequest=true | Action=1, KPI=16 | ✅ IsFirstRequest ne se déclenche qu'une fois |
| **HEUR_R_INCOMING_REMIND_30D_ISSUANCE** | DaysSinceReminderMin=30 | Action=3, KPI=16 | ✅ Condition temporelle (30 jours) |
| **HEUR_R_INCOMING_REMIND_30D_REISSUANCE_ACK** | DaysSinceReminderMin=30 | Action=1, KPI=16 | ✅ Condition temporelle (30 jours) |
| **HEUR_R_INCOMING_REMIND_30D_REISSUANCE_NOTACK** | DaysSinceReminderMin=30 | Action=7, KPI=17 | ✅ Condition temporelle (30 jours) |

---

## 💡 Solutions Proposées

### **Solution A: Ajouter `CurrentActionId = null` (Action pas encore traitée)**

**Principe:** La règle ne s'applique que si l'Action n'a pas encore été définie (null).

**Avantages:**
- ✅ Empêche la réapplication si l'utilisateur a déjà mis une Action
- ✅ Simple à implémenter

**Inconvénients:**
- ❌ Si l'utilisateur met Action=null, la règle se réapplique

**Règles concernées:** TOUTES les règles LEGACY (Pivot et Receivable)

---

### **Solution B: Ajouter `ActionStatus = null` (Pas encore traité)**

**Principe:** La règle ne s'applique que si ActionStatus est null (pas encore traité par l'utilisateur).

**Avantages:**
- ✅ Empêche la réapplication si l'utilisateur a traité la ligne (PENDING ou DONE)
- ✅ Plus robuste que Solution A

**Inconvénients:**
- ❌ Nécessite d'ajouter le champ `CurrentActionStatus` dans le schema (pas encore implémenté)

**Règles concernées:** TOUTES les règles LEGACY

---

### **Solution C: Ajouter `IsFirstRequest = true` (Première fois)**

**Principe:** La règle ne s'applique que lors de la première apparition de la ligne dans Ambre.

**Avantages:**
- ✅ Garantit que la règle ne s'applique qu'une seule fois
- ✅ Déjà implémenté dans le système

**Inconvénients:**
- ❌ Ne permet pas de réévaluer si les conditions DWINGS changent

**Règles concernées:** Toutes les règles Receivable INCOMING_PAYMENT

---

### **Solution D: Désactiver `AutoApply` (Confirmation manuelle)**

**Principe:** La règle propose l'Action mais ne l'applique pas automatiquement.

**Avantages:**
- ✅ L'utilisateur garde le contrôle total
- ✅ Pas de réapplication automatique

**Inconvénients:**
- ❌ Perd l'automatisation (l'utilisateur doit confirmer chaque fois)

**Règles concernées:** Règles critiques uniquement

---

## 🎯 Recommandation Finale

### **Approche Hybride:**

1. **Règles Pivot (LEGACY_P_*)**: Ajouter `CurrentActionId = null`
   - Ces règles sont basées sur des conditions stables (TransactionType, Sign)
   - Si Action est déjà définie, ne pas réappliquer

2. **Règles Receivable DWINGS (LEGACY_R_INCOMING_PAYMENT_*)**: Ajouter `IsFirstRequest = true`
   - Ces règles dépendent de conditions DWINGS qui peuvent changer
   - Mais on veut qu'elles s'appliquent uniquement la première fois

3. **Règles Receivable autres (LEGACY_R_DIRECT_DEBIT, etc.)**: Ajouter `CurrentActionId = null`
   - Conditions stables, pas besoin de réévaluation

---

## 📝 Implémentation Proposée

### **Étape 1: Règles Pivot (11 règles)**

```csharp
// AVANT
new TruthRule { RuleId = "LEGACY_P_COLLECTION_C_NOTGROUP", Scope = RuleScope.Import, AccountSide = "P", TransactionType = "COLLECTION", Sign = "C", OutputActionId = 7, OutputKpiId = 18, ... }

// APRÈS
new TruthRule { RuleId = "LEGACY_P_COLLECTION_C_NOTGROUP", Scope = RuleScope.Import, AccountSide = "P", TransactionType = "COLLECTION", Sign = "C", CurrentActionId = null, OutputActionId = 7, OutputKpiId = 18, ... }
```

**Règles à modifier:**
- LEGACY_P_COLLECTION_C_NOTGROUP
- LEGACY_P_COLLECTION_D
- LEGACY_P_PAYMENT_D
- LEGACY_P_PAYMENT_C
- LEGACY_P_ADJUSTMENT
- LEGACY_P_XCL_LOADER_C
- LEGACY_P_XCL_LOADER_D
- LEGACY_P_TRIGGER_C
- LEGACY_P_TRIGGER_D
- LEGACY_P_MANUAL_OUTGOING

**Exception:** `LEGACY_P_COLLECTION_C_GROUP` - OK car condition `IsAmountMatch=true` change rarement

---

### **Étape 2: Règles Receivable INCOMING_PAYMENT (6 règles)**

```csharp
// AVANT
new TruthRule { RuleId = "LEGACY_R_INCOMING_PAYMENT_REISSUANCE_ACK", Scope = RuleScope.Import, AccountSide = "R", GuaranteeType = "REISSUANCE", TransactionType = "INCOMING_PAYMENT", MTStatusAcked = true, OutputActionId = 1, ... }

// APRÈS
new TruthRule { RuleId = "LEGACY_R_INCOMING_PAYMENT_REISSUANCE_ACK", Scope = RuleScope.Import, AccountSide = "R", GuaranteeType = "REISSUANCE", TransactionType = "INCOMING_PAYMENT", MTStatusAcked = true, IsFirstRequest = true, OutputActionId = 1, ... }
```

**Règles à modifier:**
- LEGACY_R_INCOMING_PAYMENT_REISSUANCE_ACK
- LEGACY_R_INCOMING_PAYMENT_ISSUANCE_EMAIL
- LEGACY_R_INCOMING_PAYMENT_ADVISING_ACK
- LEGACY_R_INCOMING_PAYMENT_REISSUANCE_NACK
- LEGACY_R_INCOMING_PAYMENT_ADVISING_NACK
- LEGACY_R_INCOMING_PAYMENT_ISSUANCE_NOEMAIL

**Exception:** `LEGACY_R_INCOMING_PAYMENT_OTHER` - Pas d'output, donc OK

---

### **Étape 3: Règles Receivable autres (4 règles)**

```csharp
// AVANT
new TruthRule { RuleId = "LEGACY_R_DIRECT_DEBIT", Scope = RuleScope.Import, AccountSide = "R", TransactionType = "DIRECT_DEBIT", OutputActionId = 7, ... }

// APRÈS
new TruthRule { RuleId = "LEGACY_R_DIRECT_DEBIT", Scope = RuleScope.Import, AccountSide = "R", TransactionType = "DIRECT_DEBIT", CurrentActionId = null, OutputActionId = 7, ... }
```

**Règles à modifier:**
- LEGACY_R_DIRECT_DEBIT
- LEGACY_R_OUTGOING_PAYMENT_NOTINI
- LEGACY_R_EXTERNAL_DEBIT_PAYMENT
- LEGACY_R_OUTGOING_PAYMENT_INIT

---

## 📊 Résumé

| Catégorie | Nombre | Solution | Condition Ajoutée |
|-----------|--------|----------|-------------------|
| **Pivot** | 10 | CurrentActionId = null | Action pas encore définie |
| **Receivable INCOMING_PAYMENT** | 6 | IsFirstRequest = true | Première apparition uniquement |
| **Receivable autres** | 4 | CurrentActionId = null | Action pas encore définie |
| **Heuristiques** | 4 | ✅ Déjà OK | Conditions temporelles |
| **TOTAL** | 24 | **20 règles à corriger** | - |

---

**Fin de l'analyse**
