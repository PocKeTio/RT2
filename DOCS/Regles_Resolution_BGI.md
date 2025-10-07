# Règles de Gestion - Résolution BGI (Bank Guarantee Invoice)

## Vue d'ensemble
Lorsqu'on a uniquement une **référence de garantie** (Guarantee ID), l'outil recherche automatiquement le bon BGI (invoice) en appliquant une cascade de règles de résolution.

---

## 1. Extraction des Tokens

### 1.1 Formats reconnus

#### **BGI (Bank Guarantee Invoice)**
Formats acceptés:
- `BGI` + `YYYYMM` (6 chiffres) + 7 caractères (0-9 ou A-F)
  - Exemple: `BGI202401A1B2C3D`
- `BGI` + `YYMM` (4 chiffres) + Code Pays (2 lettres) + 7 caractères (0-9 ou A-F)
  - Exemple: `BGI2401FR1234567`

**Regex**: `(?:^|[^A-Za-z0-9])(BGI(?:\d{6}[A-F0-9]{7}|\d{4}[A-Za-z]{2}[A-F0-9]{7}))(?![A-Za-z0-9])`

#### **BGPMT (Bank Guarantee Payment)**
- `BGPMT` + 8 à 20 caractères alphanumériques
  - Exemple: `BGPMT12345678`

**Regex**: `(?:^|[^A-Za-z0-9])(BGPMT[A-Za-z0-9]{8,20})(?![A-Za-z0-9])`

#### **Guarantee ID**
- Format: `G` + 4 chiffres + 2 lettres + 9 chiffres
  - Exemple: `G1234FR123456789`

**Regex**: `(?:^|[^A-Za-z0-9])(G\d{4}[A-Za-z]{2}\d{9})(?![A-Za-z0-9])`

### 1.2 Sources d'extraction (ordre de priorité)
Les tokens sont extraits des champs suivants:

**Pour BGI:**
1. `RawLabel` (libellé brut)
2. `Reconciliation_Num`
3. `ReconciliationOrigin_Num`

**Pour BGPMT:**
1. `Reconciliation_Num`
2. `ReconciliationOrigin_Num`
3. `RawLabel`

**Pour Guarantee ID:**
1. `Reconciliation_Num`
2. `RawLabel`

---

## 2. Cascade de Résolution

### 2.1 Différence Receivable vs Pivot

#### **Receivable (Créances)**
- **UNIQUEMENT** `Reconciliation_Num` est utilisé
- Pas de fallback sur d'autres champs
- Source: `dataAmbre.Receivable_InvoiceFromAmbre?.Trim() ?? ExtractBgiToken(Reconciliation_Num)`

#### **Pivot**
- Utilise la cascade complète: `RawLabel → Reconciliation_Num → ReconciliationOrigin_Num`

### 2.2 Ordre de résolution (cascade complète)

#### **Étape 1: Résolution par BGI**
```
IF BGI extrait THEN
    Rechercher invoice(s) où INVOICE_ID = BGI (exact match, case-insensitive)
    
    IF 1 seul résultat THEN
        Retourner cette invoice (peu importe le montant)
    
    ELSE IF plusieurs résultats THEN
        Prioriser:
        1. Match exact REQUESTED_AMOUNT (tolérance ±0.01)
        2. Match exact BILLING_AMOUNT (tolérance ±0.01)
        3. Plus proche REQUESTED_AMOUNT
        4. Plus proche BILLING_AMOUNT
    END IF
END IF
```

**Critères de matching:**
- Comparaison exacte sur `INVOICE_ID`
- **Si 1 seule invoice**: retour immédiat sans vérification de montant
- **Si plusieurs invoices**: raffinement par montant (REQUESTED_AMOUNT prioritaire)

#### **Étape 2: Résolution par BGPMT**
```
IF pas de résultat ET BGPMT extrait THEN
    Rechercher invoice(s) où BGPMT = BGPMT extrait (exact match)
    
    IF 1 seul résultat THEN
        Retourner cette invoice (peu importe le montant)
    
    ELSE IF plusieurs résultats THEN
        Prioriser:
        1. Match exact REQUESTED_AMOUNT (tolérance ±0.01)
        2. Match exact BILLING_AMOUNT (tolérance ±0.01)
        3. Plus proche REQUESTED_AMOUNT
        4. Plus proche BILLING_AMOUNT
    END IF
END IF
```

#### **Étape 3: Résolution par OfficialRef (SENDER_REFERENCE)**
```
IF pas de résultat THEN
    Extraire tous les tokens alphanumériques de Reconciliation_Num et ReconciliationOrigin_Num
    Rechercher invoices où SENDER_REFERENCE IN (tokens extraits)
    IF plusieurs résultats THEN
        Classer par:
        1. Proximité de date (START_DATE ou END_DATE vs Operation_Date/Value_Date)
        2. Proximité de montant (BILLING_AMOUNT vs SignedAmount)
    END IF
    Retourner meilleur résultat
END IF
```

#### **Étape 4: Résolution par Guarantee ID** ⭐
```
IF pas de résultat ET Guarantee ID extrait THEN
    Rechercher invoices où:
        - BUSINESS_CASE_REFERENCE = Guarantee ID (exact match, prioritaire)
        - OU BUSINESS_CASE_ID = Guarantee ID (exact match, prioritaire)
        - OU BUSINESS_CASE_REFERENCE CONTAINS Guarantee ID (fallback)
        - OU BUSINESS_CASE_ID CONTAINS Guarantee ID (fallback)
    
    ⚠️ FILTRER OBLIGATOIREMENT: T_INVOICE_STATUS = 'GENERATED'
    
    Classer les résultats par:
        1. Match exact > Match partiel (contains)
        2. Proximité de date: |START_DATE ou END_DATE - Ambre Date|
        3. Proximité de montant: MIN(|REQUESTED_AMOUNT - Ambre|, |BILLING_AMOUNT - Ambre|)
    
    Retourner top 1 (ou top N si demandé)
END IF
```

**Détails de l'algorithme Guarantee:**
```csharp
// Matching exact (prioritaire)
MatchEq = BUSINESS_CASE_REFERENCE == guaranteeId 
       OR BUSINESS_CASE_ID == guaranteeId

// Matching partiel (fallback si aucun exact)
MatchContains = BUSINESS_CASE_REFERENCE CONTAINS guaranteeId
             OR BUSINESS_CASE_ID CONTAINS guaranteeId

// ⚠️ FILTRE CRITIQUE: Statut GENERATED uniquement
StatusFilter = T_INVOICE_STATUS == 'GENERATED'

// Scoring
DateScore = ABS(InvoiceDate - AmbreDate) en jours
AmountScore = MIN(
    ABS(REQUESTED_AMOUNT - AmbreAmount),
    ABS(BILLING_AMOUNT - AmbreAmount)
)

// Classement final
ORDER BY DateScore ASC, AmountScore ASC
TAKE 1
```

#### **Étape 5: Suggestions (fallback final)**
```
IF toujours pas de résultat THEN
    Appliquer SuggestInvoicesForAmbre():
        1. Essayer BGI (avec priorité Reconciliation_Num > ReconciliationOrigin_Num > RawLabel)
        2. Essayer BGPMT
        3. Essayer Guarantee ID avec ranking
    Retourner top 1 suggestion
END IF
```

---

## 3. Règles de Matching

### 3.1 Normalisation
Toutes les comparaisons sont:
- **Case-insensitive** (majuscules/minuscules ignorées)
- **Trimmed** (espaces supprimés)
- **UpperInvariant** pour stockage

### 3.2 Tolérance de montant
- **Exact match**: `|Ambre Amount - DW Amount| ≤ 0.01`
- Si pas d'exact match: sélection du montant le plus proche

### 3.3 Scoring de date
- Calcul: `ABS((InvoiceDate - AmbreDate).TotalDays)`
- Date invoice: `START_DATE` en priorité, sinon `END_DATE`
- Date Ambre: `Operation_Date` en priorité, sinon `Value_Date`

---

## 4. Cas d'usage: Guarantee ID uniquement

### Scénario
```
Ambre line:
- Reconciliation_Num = "G1234FR123456789"
- SignedAmount = 1500.00
- Operation_Date = 2024-01-15
```

### Processus de résolution

1. **Extraction**: `G1234FR123456789` détecté comme Guarantee ID
2. **Étapes 1-3**: Aucun BGI/BGPMT/OfficialRef trouvé → skip
3. **Étape 4 - Guarantee**:
   ```sql
   SELECT * FROM DWINGS_Invoices
   WHERE (BUSINESS_CASE_REFERENCE = 'G1234FR123456789'
      OR BUSINESS_CASE_ID = 'G1234FR123456789')
      AND T_INVOICE_STATUS = 'GENERATED'  -- ⚠️ CRITIQUE
   ORDER BY 
       ABS(DATEDIFF(day, START_DATE, '2024-01-15')),
       LEAST(ABS(REQUESTED_AMOUNT - 1500.00), ABS(BILLING_AMOUNT - 1500.00))
   LIMIT 1
   ```

4. **Résultat**:
   - Si 1 invoice GENERATED trouvée → lien automatique
   - Si plusieurs GENERATED → sélection de la plus proche en date puis montant
   - Si 0 GENERATED → pas de lien automatique (même si d'autres statuts existent)

---

## 5. Backfilling des références

Une fois l'invoice résolue, l'outil remplit automatiquement:

```csharp
references.InvoiceId = hit?.INVOICE_ID;
references.CommissionId = tokens.Bgpmt ?? hit?.BGPMT;
references.GuaranteeId = tokens.GuaranteeId ?? hit?.BUSINESS_CASE_REFERENCE ?? hit?.BUSINESS_CASE_ID;
```

**Logique:**
- **InvoiceId**: uniquement si résolution réussie
- **CommissionId**: token extrait en priorité, sinon BGPMT de l'invoice
- **GuaranteeId**: token extrait en priorité, sinon BUSINESS_CASE_REFERENCE/ID de l'invoice

---

## 6. Fichiers source

### Fichiers principaux
- **`DwingsReferenceResolver.cs`**: Orchestration de la résolution
- **`DwingsLinkingHelper.cs`**: Algorithmes de matching et extraction

### Méthodes clés
- `ResolveReferencesAsync()`: Point d'entrée principal
- `ResolveInvoicesByGuarantee()`: Résolution par Guarantee ID
- `ExtractGuaranteeId()`: Extraction regex du Guarantee ID
- `SuggestInvoicesForAmbre()`: Suggestions fallback

---

## 7. Exemples de résolution

### Exemple 1: BGI direct (unique)
```
Input: Reconciliation_Num = "BGI202401A1B2C3D"
→ Extraction: BGI202401A1B2C3D
→ Match: 1 seule invoice avec INVOICE_ID = "BGI202401A1B2C3D"
→ Résultat: Lien direct (pas de vérification de montant)
```

### Exemple 1b: BGI avec plusieurs invoices
```
Input: Reconciliation_Num = "BGI202401A1B2C3D", Amount = 1500
→ Extraction: BGI202401A1B2C3D
→ Match: 3 invoices avec INVOICE_ID = "BGI202401A1B2C3D"
   1. REQUESTED_AMOUNT = 1500 ✅ WINNER (exact match)
   2. REQUESTED_AMOUNT = 1600
   3. REQUESTED_AMOUNT = null, BILLING_AMOUNT = 1500
→ Résultat: Invoice #1 (REQUESTED_AMOUNT exact)
```

### Exemple 2: Guarantee avec plusieurs invoices
```
Input: Reconciliation_Num = "G1234FR123456789"
Ambre: Date=2024-01-15, Amount=1500

DWINGS Invoices (toutes avec BUSINESS_CASE_REF = G1234FR123456789):
1. BGI202401XXX - Status=GENERATED - Date=2024-01-10 - ReqAmt=1500 ✅
2. BGI202402YYY - Status=GENERATED - Date=2024-02-01 - ReqAmt=1500
3. BGI202401ZZZ - Status=GENERATED - Date=2024-01-12 - ReqAmt=1600
4. BGI202401AAA - Status=DRAFT     - Date=2024-01-10 - ReqAmt=1500 ❌ (filtré)

→ Filtre: Seules les invoices GENERATED sont considérées (1, 2, 3)

→ Scoring:
   1. DateScore=5j, AmountScore=0   → Score: (5, 0) ✅ WINNER
   2. DateScore=17j, AmountScore=0  → Score: (17, 0)
   3. DateScore=3j, AmountScore=100 → Score: (3, 100)

→ Résultat: BGI202401XXX (meilleur date+amount, statut GENERATED)
```

### Exemple 3: Guarantee partiel (contains)
```
Input: Reconciliation_Num = "G1234FR"
DWINGS: BUSINESS_CASE_REFERENCE = "G1234FR123456789"

→ Pas de match exact
→ Match partiel (contains) trouvé
→ Résultat: Lien suggéré
```

---

## 8. Limitations et edge cases

### Limitations
1. **Pas de fuzzy matching**: Les tokens doivent respecter les formats exacts
2. **Guarantee partiel risqué**: Un ID trop court peut matcher plusieurs garanties
3. **Filtre GENERATED uniquement pour Guarantee**: Les résolutions BGI/BGPMT directes ne filtrent pas par statut
4. **Pas de validation métier avancée**: L'outil ne vérifie pas la cohérence business (ex: devise, contrepartie)

### Edge cases gérés
- **Plusieurs invoices même BGI**: Raffinement par montant
- **Plusieurs invoices même Guarantee**: Ranking date + montant
- **Aucune correspondance**: Retourne null (pas de lien forcé)
- **Tokens multiples**: Priorité BGI > BGPMT > Guarantee

---

## 9. Recommandations

### Pour améliorer la résolution
1. **Toujours privilégier le BGI complet** dans les libellés
2. **Formater les Guarantee IDs correctement**: `G####AA#########`
3. **Vérifier les montants**: Utiliser `REQUESTED_AMOUNT` en priorité (critère principal)
4. **Dates cohérentes**: Les dates Ambre doivent être proches des dates DWINGS
5. **Statut GENERATED**: Pour les résolutions par Guarantee, s'assurer que les invoices sont au statut GENERATED

### Pour déboguer
1. Vérifier les logs: `LogManager.Warning` en cas d'échec
2. Tester l'extraction: Appeler `ExtractGuaranteeId()` sur le champ
3. Vérifier DWINGS: S'assurer que `BUSINESS_CASE_REFERENCE` est rempli
4. Comparer manuellement: Date et montant sont-ils cohérents?

---

**Dernière mise à jour**: 2025-10-08  
**Version**: RT v2
