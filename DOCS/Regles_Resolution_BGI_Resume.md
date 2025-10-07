# Résumé - Règles de Résolution BGI/BGPMT/Guarantee

## 🎯 Règles Simplifiées

### 1️⃣ Si BGI ou BGPMT trouvé

```
┌─────────────────────────────────────────┐
│  1 seule invoice en base DWINGS ?       │
├─────────────────────────────────────────┤
│  OUI  → Retour immédiat                 │
│         (pas de vérification montant)   │
│                                          │
│  NON  → Prendre celle avec le           │
│         REQUESTED_AMOUNT correspondant  │
│         (sinon BILLING_AMOUNT)          │
└─────────────────────────────────────────┘
```

**Exemple:**
```
BGI202401ABC trouvé dans Reconciliation_Num
→ 1 seule invoice BGI202401ABC existe
→ ✅ Lien automatique (même si montant différent)

BGI202401XYZ trouvé dans Reconciliation_Num, Ambre Amount = 1500
→ 3 invoices BGI202401XYZ existent:
   - Invoice A: REQUESTED_AMOUNT = 1500 ✅ WINNER
   - Invoice B: REQUESTED_AMOUNT = 1600
   - Invoice C: REQUESTED_AMOUNT = null, BILLING_AMOUNT = 1500
→ ✅ Lien vers Invoice A
```

---

### 2️⃣ Si uniquement Guarantee ID trouvé

```
┌─────────────────────────────────────────┐
│  Recherche BGI/BGPMT liés à la garantie │
├─────────────────────────────────────────┤
│  ⚠️  FILTRE OBLIGATOIRE:                │
│      T_INVOICE_STATUS = 'GENERATED'     │
│                                          │
│  Puis vérifier:                          │
│  - REQUESTED_AMOUNT correspondant       │
│  - Date la plus proche                   │
└─────────────────────────────────────────┘
```

**Exemple:**
```
G1234FR123456789 trouvé dans Reconciliation_Num
Ambre: Date = 2024-01-15, Amount = 1500

DWINGS Invoices avec BUSINESS_CASE_REF = G1234FR123456789:
1. BGI202401XXX - Status=GENERATED - Date=2024-01-10 - ReqAmt=1500 ✅
2. BGI202402YYY - Status=GENERATED - Date=2024-02-01 - ReqAmt=1500
3. BGI202401ZZZ - Status=DRAFT     - Date=2024-01-10 - ReqAmt=1500 ❌

→ Invoice 3 filtrée (statut DRAFT)
→ Invoice 1 sélectionnée (date + montant optimal)
→ ✅ Lien vers BGI202401XXX
```

---

## 📊 Tableau Récapitulatif

| Cas | Nombre d'invoices | Action | Vérification montant | Filtre statut |
|-----|-------------------|--------|---------------------|---------------|
| **BGI direct** | 1 | Retour immédiat | ❌ Non | ❌ Non |
| **BGI direct** | >1 | Match REQUESTED_AMOUNT | ✅ Oui | ❌ Non |
| **BGPMT direct** | 1 | Retour immédiat | ❌ Non | ❌ Non |
| **BGPMT direct** | >1 | Match REQUESTED_AMOUNT | ✅ Oui | ❌ Non |
| **Guarantee ID** | 1+ | Match date + montant | ✅ Oui (REQUESTED_AMOUNT) | ✅ Oui (GENERATED) |

---

## 🔍 Priorités de Matching

### Pour BGI/BGPMT (plusieurs invoices)
1. ✅ **REQUESTED_AMOUNT exact** (tolérance ±0.01)
2. ✅ **BILLING_AMOUNT exact** (tolérance ±0.01)
3. 📊 Plus proche **REQUESTED_AMOUNT**
4. 📊 Plus proche **BILLING_AMOUNT**

### Pour Guarantee ID
1. 🚫 **Filtre**: T_INVOICE_STATUS = 'GENERATED'
2. 📅 **Date**: Proximité avec Operation_Date/Value_Date
3. 💰 **Montant**: MIN(|REQUESTED_AMOUNT - Ambre|, |BILLING_AMOUNT - Ambre|)

---

## ⚠️ Points Critiques

### ✅ À FAIRE
- Utiliser le **BGI complet** dans les libellés Ambre
- Remplir **REQUESTED_AMOUNT** dans DWINGS (critère principal)
- Mettre les invoices au statut **GENERATED** quand elles sont prêtes
- Vérifier la **cohérence des dates** entre Ambre et DWINGS

### ❌ À ÉVITER
- Ne pas laisser plusieurs invoices avec même BGI/BGPMT sans REQUESTED_AMOUNT
- Ne pas utiliser de Guarantee ID trop court (risque de match partiel)
- Ne pas oublier de passer les invoices en GENERATED

---

## 🧪 Tests Recommandés

### Test 1: BGI unique
```
Ambre: BGI202401ABC, Amount = 1000
DWINGS: 1 invoice BGI202401ABC, REQUESTED_AMOUNT = 1500
→ Attendu: Lien créé (montant ignoré car unique)
```

### Test 2: BGI multiple
```
Ambre: BGI202401ABC, Amount = 1000
DWINGS: 
  - Invoice A: BGI202401ABC, REQUESTED_AMOUNT = 1000
  - Invoice B: BGI202401ABC, REQUESTED_AMOUNT = 1500
→ Attendu: Lien vers Invoice A (montant exact)
```

### Test 3: Guarantee avec statuts mixtes
```
Ambre: G1234FR123456789, Amount = 1000, Date = 2024-01-15
DWINGS:
  - Invoice A: Status=GENERATED, Date=2024-01-10, ReqAmt=1000
  - Invoice B: Status=DRAFT, Date=2024-01-10, ReqAmt=1000
→ Attendu: Lien vers Invoice A (B filtré par statut)
```

### Test 4: Guarantee sans GENERATED
```
Ambre: G1234FR123456789, Amount = 1000
DWINGS:
  - Invoice A: Status=DRAFT, ReqAmt=1000
  - Invoice B: Status=PENDING, ReqAmt=1000
→ Attendu: Aucun lien (pas de GENERATED)
```

---

**Version**: RT v2  
**Date**: 2025-10-08  
**Fichiers source**: `DwingsReferenceResolver.cs`, `DwingsLinkingHelper.cs`
