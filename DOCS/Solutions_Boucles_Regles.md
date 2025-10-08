# Solutions - Prévention des Boucles Infinies dans les Règles

## 🔴 Problèmes Identifiés

### 1. Boucles infinies lors de l'édition manuelle
**Scénario problématique:**
```
User met Action = DONE (ActionStatus = true)
→ Règle se déclenche (Scope=Edit ou Both)
→ Règle force Action = PENDING (ActionStatus = false)
→ User ne peut jamais mettre DONE
```

### 2. Action N/A sans ActionStatus automatique
**Problème:** Quand Action = N/A, l'ActionStatus devrait être automatiquement DONE

### 3. ActionDate non forcée
**Problème:** Quand une règle force une Action, l'ActionDate n'est pas mise à jour

---

## ✅ Solutions Proposées

### **Solution 1: Ajouter `CurrentActionStatus` comme condition**

#### Concept
Permettre aux règles de filtrer sur l'ActionStatus actuel pour éviter de se réappliquer en boucle.

#### Implémentation

**A. Ajouter le champ dans `TruthRule.cs`:**
```csharp
// Current state conditions
public int? CurrentActionId { get; set; }
public bool? CurrentActionStatus { get; set; }  // NEW: null/true/false
```

**B. Ajouter dans `RuleContext`:**
```csharp
public int? CurrentActionId { get; set; }
public bool? CurrentActionStatus { get; set; }  // NEW
```

**C. Modifier les règles par défaut:**
```csharp
// AVANT (problématique)
new TruthRule { 
    RuleId = "LEGACY_R_INCOMING_PAYMENT_REISSUANCE_ACK", 
    Scope = RuleScope.Both,  // ← Se déclenche aussi en Edit!
    AccountSide = "R", 
    GuaranteeType = "REISSUANCE", 
    TransactionType = "INCOMING_PAYMENT", 
    MTStatusAcked = true, 
    OutputActionId = 1,  // Force Action = 1
    AutoApply = true 
}

// APRÈS (corrigé)
new TruthRule { 
    RuleId = "LEGACY_R_INCOMING_PAYMENT_REISSUANCE_ACK", 
    Scope = RuleScope.Both,
    AccountSide = "R", 
    GuaranteeType = "REISSUANCE", 
    TransactionType = "INCOMING_PAYMENT", 
    MTStatusAcked = true,
    CurrentActionStatus = null,  // ← Seulement si pas encore traité (PENDING ou null)
    OutputActionId = 1,
    AutoApply = true 
}
```

**Logique:**
- `CurrentActionStatus = null` → Règle s'applique uniquement si ActionStatus n'est pas DONE
- `CurrentActionStatus = false` → Règle s'applique uniquement si ActionStatus = PENDING
- `CurrentActionStatus = true` → Règle s'applique uniquement si ActionStatus = DONE
- Pas de `CurrentActionStatus` → Règle s'applique toujours (comportement actuel)

---

### **Solution 2: Scope plus restrictif par défaut**

#### Concept
Changer le scope des règles automatiques de `Both` à `Import` pour éviter qu'elles se déclenchent lors de l'édition manuelle.

#### Implémentation

**Modifier les règles par défaut:**
```csharp
// AVANT
new TruthRule { 
    RuleId = "LEGACY_R_INCOMING_PAYMENT_REISSUANCE_ACK", 
    Scope = RuleScope.Both,  // ← Problématique
    ...
}

// APRÈS
new TruthRule { 
    RuleId = "LEGACY_R_INCOMING_PAYMENT_REISSUANCE_ACK", 
    Scope = RuleScope.Import,  // ← Seulement à l'import
    ...
}
```

**Avantages:**
- ✅ Empêche les boucles lors de l'édition manuelle
- ✅ Les règles s'appliquent uniquement lors de l'import Ambre

**Inconvénients:**
- ❌ Les règles ne se réappliquent pas si les conditions DWINGS changent après l'import
- ❌ Moins flexible pour les cas où on veut une réévaluation automatique

---

### **Solution 3: Ajouter `OutputActionStatus` dans les règles**

#### Concept
Permettre aux règles de forcer explicitement l'ActionStatus en plus de l'Action.

#### Implémentation

**A. Ajouter dans `TruthRule.cs`:**
```csharp
// Outputs
public int? OutputActionId { get; set; }
public bool? OutputActionStatus { get; set; }  // NEW: Force ActionStatus
public int? OutputKpiId { get; set; }
```

**B. Ajouter dans `RuleEvaluationResult`:**
```csharp
public int? NewActionIdSelf { get; set; }
public bool? NewActionStatusSelf { get; set; }  // NEW
public int? NewKpiIdSelf { get; set; }
```

**C. Appliquer dans le code de sauvegarde:**
```csharp
if (result.NewActionStatusSelf.HasValue)
{
    reconciliation.ActionStatus = result.NewActionStatusSelf.Value;
    reconciliation.ActionDate = DateTime.Now;  // ← Toujours mettre à jour la date
}
```

---

### **Solution 4: Gestion spéciale de l'Action N/A**

#### Concept
Détecter automatiquement quand Action = N/A et forcer ActionStatus = DONE.

#### Implémentation

**A. Dans `ReconciliationService.SaveReconciliationsAsync()`:**
```csharp
// After applying rule result
if (result != null && result.NewActionIdSelf.HasValue)
{
    reconciliation.Action = result.NewActionIdSelf.Value;
    reconciliation.ActionDate = DateTime.Now;  // ← TOUJOURS forcer la date
    
    // Special case: N/A action should be marked as DONE
    var actionField = _offlineFirstService?.UserFields?
        .FirstOrDefault(f => f.USR_Category == "Action" && f.USR_ID == result.NewActionIdSelf.Value);
    
    if (actionField != null && 
        (actionField.USR_FieldName?.Contains("N/A") == true || 
         actionField.USR_FieldDescription?.Contains("Not Applicable") == true))
    {
        reconciliation.ActionStatus = true;  // Auto-mark as DONE
    }
}
```

**B. Alternative: Dans l'UI lors de la sélection manuelle:**
```csharp
// In ReconciliationView\Editing.cs or similar
private void OnActionChanged(object sender, SelectionChangedEventArgs e)
{
    if (e.AddedItems.Count > 0 && e.AddedItems[0] is UserField actionField)
    {
        var row = GetSelectedRow();
        if (row != null)
        {
            row.Action = actionField.USR_ID;
            row.ActionDate = DateTime.Now;  // ← TOUJOURS forcer la date
            
            // Auto-mark N/A as DONE
            if (actionField.USR_FieldName?.Contains("N/A") == true || 
                actionField.USR_FieldDescription?.Contains("Not Applicable") == true)
            {
                row.ActionStatus = true;
            }
        }
    }
}
```

---

### **Solution 5: Toujours forcer ActionDate lors du changement d'Action**

#### Concept
Garantir que chaque fois qu'une Action est modifiée (manuellement ou par règle), l'ActionDate est mise à jour.

#### Implémentation

**A. Dans `ReconciliationService.SaveReconciliationsAsync()`:**
```csharp
// Check if Action changed
bool actionChanged = false;
if (dto.Action.HasValue)
{
    var oldAction = existing.Action;
    if (oldAction != dto.Action.Value)
    {
        actionChanged = true;
    }
}

// Apply changes
if (dto.Action.HasValue)
{
    existing.Action = dto.Action.Value;
    
    // ALWAYS update ActionDate when Action changes
    if (actionChanged)
    {
        existing.ActionDate = DateTime.Now;
    }
}

// Apply rule result
if (result != null && result.NewActionIdSelf.HasValue)
{
    existing.Action = result.NewActionIdSelf.Value;
    existing.ActionDate = DateTime.Now;  // ← FORCE date
}
```

**B. Dans `AmbreReconciliationUpdater.ApplyRuleOutputs()`:**
```csharp
if (result.NewActionIdSelf.HasValue)
{
    staging.Action = result.NewActionIdSelf.Value;
    staging.ActionDate = DateTime.Now;  // ← FORCE date
}
```

---

## 📋 Plan d'Implémentation Recommandé

### **Phase 1: Fixes Critiques (Immédiat)**

1. ✅ **Forcer ActionDate lors du changement d'Action** (Solution 5)
   - Modifier `ReconciliationService.SaveReconciliationsAsync()`
   - Modifier `AmbreReconciliationUpdater.ApplyRuleOutputs()`

2. ✅ **Gestion automatique Action N/A** (Solution 4)
   - Détecter N/A et forcer ActionStatus = DONE

### **Phase 2: Prévention des Boucles (Court terme)**

3. ✅ **Ajouter CurrentActionStatus** (Solution 1)
   - Ajouter le champ dans schema/repository/engine
   - Modifier les règles par défaut pour utiliser `CurrentActionStatus = null`

4. ✅ **Changer Scope des règles automatiques** (Solution 2)
   - Passer de `Both` à `Import` pour les règles auto-apply

### **Phase 3: Amélioration Avancée (Optionnel)**

5. ⚠️ **Ajouter OutputActionStatus** (Solution 3)
   - Permet un contrôle fin de l'ActionStatus par les règles

---

## 🧪 Tests Recommandés

### Test 1: Boucle infinie
```
1. Import Ambre → Règle applique Action = 1 (PENDING)
2. User change manuellement Action = DONE
3. Sauvegarder
4. Vérifier: Action reste DONE (pas de réapplication de règle)
```

### Test 2: Action N/A
```
1. Sélectionner Action = N/A
2. Vérifier: ActionStatus passe automatiquement à DONE
3. Vérifier: ActionDate = maintenant
```

### Test 3: ActionDate forcée
```
1. Changer Action (manuellement ou par règle)
2. Vérifier: ActionDate = maintenant
```

### Test 4: Règle avec CurrentActionStatus
```
1. Créer règle avec CurrentActionStatus = null
2. Import → Règle s'applique (Action = 1, ActionStatus = null)
3. User met ActionStatus = DONE
4. Rééditer la ligne
5. Vérifier: Règle ne se réapplique PAS (CurrentActionStatus != null)
```

---

## 📊 Tableau Récapitulatif

| Solution | Priorité | Complexité | Impact | Risque |
|----------|----------|------------|--------|--------|
| **Solution 1: CurrentActionStatus** | 🔴 Haute | Moyenne | Élevé | Faible |
| **Solution 2: Scope Import** | 🔴 Haute | Faible | Moyen | Très faible |
| **Solution 3: OutputActionStatus** | 🟡 Moyenne | Moyenne | Moyen | Faible |
| **Solution 4: N/A auto-DONE** | 🔴 Haute | Faible | Élevé | Très faible |
| **Solution 5: Force ActionDate** | 🔴 Haute | Faible | Élevé | Très faible |

---

## 🎯 Recommandation Finale

**Implémenter dans cet ordre:**

1. **Solution 5** (Force ActionDate) - Quick win, 0 risque
2. **Solution 4** (N/A auto-DONE) - Quick win, améliore UX
3. **Solution 2** (Scope Import) - Empêche 90% des boucles
4. **Solution 1** (CurrentActionStatus) - Protection complète contre les boucles
5. **Solution 3** (OutputActionStatus) - Optionnel, pour cas avancés

**Effort total estimé:** 4-6 heures  
**Impact:** Résolution complète des boucles infinies + amélioration UX

---

**Date:** 2025-10-08  
**Version:** RT v2
