using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using RecoTool.Models;
using RecoTool.Helpers;

namespace RecoTool.Services
{
    /// <summary>
    /// Service pour les transformations de données Ambre
    /// Implémente les fonctions définies dans AMB_TransformationFunction
    /// </summary>
    public class TransformationService
    {
        private readonly Dictionary<string, Country> _countries;

        public TransformationService(IEnumerable<Country> countries)
        {
            _countries = countries?.ToDictionary(c => c.CNT_Id, c => c) ?? new Dictionary<string, Country>();
        }

        /// <summary>
        /// Applique une transformation sur les données d'une ligne
        /// </summary>
        /// <param name="sourceData">Données source</param>
        /// <param name="transform">Configuration de transformation</param>
        /// <returns>Valeur transformée</returns>
        public string ApplyTransformation(Dictionary<string, object> sourceData, AmbreTransform transform)
        {
            var source = GetSourceValue(sourceData, transform?.AMB_Source);

            if (transform == null || string.IsNullOrEmpty(transform.AMB_TransformationFunction))
                return source;

            switch (transform.AMB_TransformationFunction.ToUpper())
            {
                case "GETBOOKINGNAMEFROMID":
                    return GetBookingNameFromID(source);

                case "GETMBAWIDFROMLABEL":
                    return GetMbawIDFromLabel(source);

                case "GETCODESFROMLABEL":
                    return GetCodesFromLabel(source);

                case "GETTRNFROMLABEL":
                case "GETTRN FROMLABEL": // support legacy with space
                    return GetTRNFromLabel(source);

                case "EXTRACTFORRECEIVABLE":
                    return ExtractForReceivable(source);

                case "REMOVEZEROSFROMSTART":
                    return RemoveZerosFromStart(source);

                default:
                    // Si la fonction n'est pas reconnue, retourne la valeur source
                    return source;
            }
        }

        /// <summary>
        /// Récupère la valeur source en gérant les concaténations
        /// Exemple: [Account_ID]&[Event_Num]&[ReconciliationOrigin_Num]&[RawLabel]
        /// </summary>
        private string GetSourceValue(Dictionary<string, object> sourceData, string sourceExpression)
        {
            if (string.IsNullOrEmpty(sourceExpression))
                return string.Empty;

            // Si c'est une simple référence de champ
            // Back-compat: 'Label' legacy => map to RawLabel if needed
            if (string.Equals(sourceExpression, "Label", StringComparison.OrdinalIgnoreCase))
            {
                var raw = sourceData.ContainsKey("RawLabel") ? sourceData["RawLabel"]?.ToString() : null;
                if (!string.IsNullOrEmpty(raw)) return raw;
            }

            if (sourceData.ContainsKey(sourceExpression))
            {
                return sourceData[sourceExpression]?.ToString() ?? string.Empty;
            }

            // Si c'est une expression avec des champs entre crochets
            var result = sourceExpression;
            var fieldMatches = Regex.Matches(sourceExpression, @"\[([^\]]+)\]");

            foreach (Match match in fieldMatches)
            {
                var fieldName = match.Groups[1].Value;
                string fieldValue;
                if (sourceData.ContainsKey(fieldName))
                {
                    fieldValue = sourceData[fieldName]?.ToString() ?? string.Empty;
                }
                else if (string.Equals(fieldName, "Label", StringComparison.OrdinalIgnoreCase) && sourceData.ContainsKey("RawLabel"))
                {
                    // Back-compat: replace [Label] by RawLabel if present
                    fieldValue = sourceData["RawLabel"]?.ToString() ?? string.Empty;
                }
                else
                {
                    fieldValue = string.Empty;
                }
                
                result = result.Replace(match.Value, fieldValue);
            }

            return result;
        }

        /// <summary>
        /// Obtient le nom du booking depuis l'ID pays
        /// </summary>
        /// <param name="countryId">Code pays Ambre</param>
        /// <returns>Booking ID 2 lettres</returns>
        public string GetBookingNameFromID(string countryId)
        {
            if (string.IsNullOrEmpty(countryId))
                return string.Empty;

            var country = _countries.Values.FirstOrDefault(c => 
                string.Equals(c.CNT_AmbrePivotCountryId.ToString(), countryId, StringComparison.OrdinalIgnoreCase));

            return country?.CNT_Id ?? countryId;
        }

        /// <summary>
        /// Extrait l'ID MBAW depuis un libellé
        /// Pattern: recherche des références de type MBAW suivies de chiffres
        /// </summary>
        /// <param name="label">Libellé à analyser</param>
        /// <returns>ID MBAW extrait</returns>
        public string GetMbawIDFromLabel(string label)
        {
            if (string.IsNullOrEmpty(label))
                return string.Empty;

            // Pattern pour MBAW suivi de chiffres et éventuellement de lettres
            var match = Regex.Match(label, @"MBAW[A-Z0-9]+", RegexOptions.IgnoreCase);
            return match.Success ? match.Value.ToUpper() : string.Empty;
        }

        /// <summary>
        /// Extrait les codes de transaction depuis un libellé
        /// </summary>
        /// <param name="label">Libellé à analyser</param>
        /// <returns>Codes extraits</returns>
        public string GetCodesFromLabel(string label)
        {
            if (string.IsNullOrEmpty(label))
                return string.Empty;

            var trimmed = label.Trim();
            if (trimmed.Length <= 13)
                return trimmed;

            return trimmed.Substring(trimmed.Length - 13);
        }

        /// <summary>
        /// Extrait le numéro TRN (Transaction Reference Number) depuis un libellé
        /// </summary>
        /// <param name="label">Libellé à analyser</param>
        /// <returns>TRN extrait</returns>
        public string GetTRNFromLabel(string label)
        {
            if (string.IsNullOrEmpty(label))
                return string.Empty;

            // Start at character 43 (1-based) => index 42 (0-based), take 10 characters
            int startIndex = 42;
            if (label.Length <= startIndex)
                return string.Empty;

            int len = Math.Min(10, label.Length - startIndex);
            return label.Substring(startIndex, len);
        }

        /// <summary>
        /// Extrait les informations spécifiques au compte Receivable
        /// Principalement l'ID DW 
        /// </summary>
        /// <param name="label">Libellé à analyser</param>
        /// <returns>Information extraite pour Receivable</returns>
        public string ExtractForReceivable(string label)
        {
            if (string.IsNullOrEmpty(label))
                return string.Empty;

            // Utiliser les extracteurs centralisés pour garantir des patterns uniformes
            var bgi = DwingsLinkingHelper.ExtractBgiToken(label);
            if (!string.IsNullOrWhiteSpace(bgi))
                return bgi.ToUpperInvariant();

            var guaranteeId = DwingsLinkingHelper.ExtractGuaranteeId(label);
            return string.IsNullOrWhiteSpace(guaranteeId) ? string.Empty : guaranteeId.ToUpperInvariant();
        }

        /// <summary>
        /// Supprime les zéros en début de chaîne
        /// </summary>
        /// <param name="value">Valeur à traiter</param>
        /// <returns>Valeur sans zéros initiaux</returns>
        public string RemoveZerosFromStart(string value)
        {
            if (string.IsNullOrEmpty(value))
                return string.Empty;

            return value.TrimStart('0');
        }

        /// <summary>
        /// Détermine le type de transaction pour la catégorisation automatique
        /// </summary>
        /// <param name="label">Libellé de la transaction</param>
        /// <param name="isPivot">True si c'est le compte Pivot</param>
        /// <param name="category">Indice de type de transaction (enum TransactionType) pour Pivot</param>
        /// <returns>Type de transaction détecté</returns>
        public TransactionType? DetermineTransactionType(string label, bool isPivot, int? category = null)
        {
            // Normalize label once
            var upperLabel = (label ?? string.Empty).ToUpperInvariant();

            // If explicitly uncategorized or empty
            if (string.IsNullOrWhiteSpace(label) && !category.HasValue)
                return TransactionType.TO_CATEGORIZE;
            if (upperLabel.Contains("TO CATEGORIZE"))
                return TransactionType.TO_CATEGORIZE;

            if (isPivot)
            {
                if (category.HasValue)
                {
                    // Désormais, category encode directement un TransactionType
                    return (TransactionType)category.Value;
                }

                if (upperLabel.Contains("COLLECTION")) return TransactionType.COLLECTION;
                if (upperLabel.Contains("AUTOMATIC REFUND") || upperLabel.Contains("PAYMENT")) return TransactionType.PAYMENT;
                if (upperLabel.Contains("ADJUSTMENT")) return TransactionType.ADJUSTMENT;
                if (upperLabel.Contains("XCL LOADER")) return TransactionType.XCL_LOADER;
                if (upperLabel.Contains("TRIGGER")) return TransactionType.TRIGGER;
                return TransactionType.TO_CATEGORIZE;
            }
            else
            {
                if (upperLabel.Contains("INCOMING PAYMENT")) return TransactionType.INCOMING_PAYMENT;
                if (upperLabel.Contains("DIRECT DEBIT")) return TransactionType.DIRECT_DEBIT;
                if (upperLabel.Contains("MANUAL OUTGOING")) return TransactionType.MANUAL_OUTGOING;
                if (upperLabel.Contains("OUTGOING PAYMENT")) return TransactionType.OUTGOING_PAYMENT;
                if (upperLabel.Contains("EXTERNAL DEBIT PAYMENT")) return TransactionType.EXTERNAL_DEBIT_PAYMENT;
                return TransactionType.TO_CATEGORIZE;
            }
        }

        /// <summary>
        /// Détermine si la transaction est crédit ou débit
        /// </summary>
        /// <param name="amount">Montant de la transaction</param>
        /// <returns>"CREDIT" ou "DEBIT"</returns>
        public string DetermineCreditDebit(decimal amount)
        {
            return amount >= 0 ? "CREDIT" : "DEBIT";
        }

        /// <summary>
        /// Applique la catégorisation automatique selon les règles métier
        /// </summary>
        /// <param name="transactionType">Type de transaction</param>
        /// <param name="signedAmount">Montant de la transaction avec signe</param>
        /// <param name="isPivot">True si compte Pivot</param>
        /// <param name="guaranteeType">Type de garantie (pour Receivable)</param>
        /// <returns>Tuple (Action, KPI)</returns>
        public (ActionType action, KPIType kpi) ApplyAutomaticCategorization(
            TransactionType? transactionType, 
            decimal signedAmount, 
            bool isPivot,
            string guaranteeType = null)
        {
            bool isCredit = signedAmount > 0;

            switch (transactionType.Value)
            {
                case TransactionType.COLLECTION:
                    if (isCredit)
                    {
                        // Primary: Match / Paid but not reconciled
                        return (ActionType.Match, KPIType.PaidButNotReconciled);
                        // Alternative from table: Investigate / IT Issues
                    }
                    else
                    {
                        // Primary: To Claim / Correspondent charges to be invoiced
                        return (ActionType.ToClaim, KPIType.CorrespondentChargesToBeInvoiced);
                        // Alternatives from table: Investigate / Under Investigation; Do Pricing / IT Issues
                    }

                case TransactionType.PAYMENT:
                    // Table specifies Debit only; treat credit as IT Issues default
                    if (!isCredit)
                    {
                        // Prefer To Claim / Correspondent charges to be invoiced; fallback N/A / IT Issues
                        return (ActionType.ToClaim, KPIType.CorrespondentChargesToBeInvoiced);
                    }
                    return (ActionType.NA, KPIType.ITIssues);

                case TransactionType.ADJUSTMENT:
                    // Both credit and debit map to Adjust / Paid but not reconciled
                    return (ActionType.Adjust, KPIType.PaidButNotReconciled);

                case TransactionType.XCL_LOADER:
                    if (isCredit)
                    {
                        // Primary: Match / Paid but not reconciled; alternative: Investigate / Under Investigation
                        return (ActionType.Match, KPIType.PaidButNotReconciled);
                    }
                    return (ActionType.Investigate, KPIType.UnderInvestigation);

                case TransactionType.TRIGGER:
                    if (isCredit)
                    {
                        // Do Pricing / Correspondent charges to be invoiced
                        return (ActionType.DoPricing, KPIType.CorrespondentChargesToBeInvoiced);
                    }
                    else
                    {
                        // Primary: To Claim / Under Investigation; alternative: N/A / IT Issues
                        return (ActionType.ToClaim, KPIType.UnderInvestigation);
                    }

                case TransactionType.INCOMING_PAYMENT:
                    // No credit/debit in table; choose a primary deterministic mapping.
                    // Primary: Request / Not Claimed. Other listed: N/A / Claimed but not paid; Trigger / Paid but not reconciled; Investigate / IT Issues; Remind / Claimed but not paid
                    return (ActionType.Request, KPIType.NotClaimed);

                case TransactionType.DIRECT_DEBIT:
                    // Investigate / IT Issues
                    return (ActionType.Investigate, KPIType.ITIssues);

                case TransactionType.MANUAL_OUTGOING:
                    // Primary: Trigger / Correspondent charges pending trigger; alternative: Investigate / IT Issues
                    return (ActionType.Trigger, KPIType.CorrespondentChargesPendingTrigger);

                case TransactionType.OUTGOING_PAYMENT:
                    // Primary: Trigger / Correspondent charges pending trigger; alternatives: Execute / Not Claimed; Investigate / IT Issues
                    return (ActionType.Trigger, KPIType.CorrespondentChargesPendingTrigger);

                case TransactionType.EXTERNAL_DEBIT_PAYMENT:
                    // To do SDD / Not Claimed
                    return (ActionType.ToDoSDD, KPIType.NotClaimed);

                case TransactionType.TO_CATEGORIZE:
                default:
                    return (ActionType.Investigate, KPIType.ITIssues);
            }
        }

    }
}