using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using RecoTool.Models;

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
            if (transform == null || string.IsNullOrEmpty(transform.AMB_TransformationFunction))
                return GetSourceValue(sourceData, transform?.AMB_Source);

            switch (transform.AMB_TransformationFunction.ToUpper())
            {
                case "GETBOOKINGNAMEFROMID":
                    return GetBookingNameFromID(GetSourceValue(sourceData, transform.AMB_Source));

                case "GETMBAWIDFROMLABEL":
                    return GetMbawIDFromLabel(GetSourceValue(sourceData, transform.AMB_Source));

                case "GETCODESFROMLABEL":
                    return GetCodesFromLabel(GetSourceValue(sourceData, transform.AMB_Source));

                case "GETTRNFROMLABEL":
                case "GETTRN FROMLABEL": // support legacy with space
                    return GetTRNFromLabel(GetSourceValue(sourceData, transform.AMB_Source));

                case "EXTRACTFORRECEIVABLE":
                    return ExtractForReceivable(GetSourceValue(sourceData, transform.AMB_Source));

                case "REMOVEZEROSFROMSTART":
                    return RemoveZerosFromStart(GetSourceValue(sourceData, transform.AMB_Source));

                default:
                    // Si la fonction n'est pas reconnue, retourne la valeur source
                    return GetSourceValue(sourceData, transform.AMB_Source);
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
                var fieldValue = sourceData.ContainsKey(fieldName) 
                    ? sourceData[fieldName]?.ToString() ?? string.Empty 
                    : string.Empty;
                
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

            var codes = new List<string>();

            // Recherche de patterns communs pour les codes de transaction
            var patterns = new[]
            {
                @"\b(COLLECTION|PAYMENT|ADJUSTMENT|TRIGGER|XCL LOADER)\b",
                @"\b(INCOMING|OUTGOING|DIRECT DEBIT|MANUAL|EXTERNAL)\b",
                @"\b(AUTOMATIC REFUND|DEBIT PAYMENT)\b"
            };

            foreach (var pattern in patterns)
            {
                var matches = Regex.Matches(label, pattern, RegexOptions.IgnoreCase);
                foreach (Match match in matches)
                {
                    if (!codes.Contains(match.Value.ToUpper()))
                        codes.Add(match.Value.ToUpper());
                }
            }

            return string.Join("|", codes);
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

            // Pattern pour TRN suivi de chiffres
            var match = Regex.Match(label, @"TRN[:\s]*([A-Z0-9]+)", RegexOptions.IgnoreCase);
            if (match.Success)
                return match.Groups[1].Value.ToUpper();

            // Pattern alternatif pour numéros de transaction
            match = Regex.Match(label, @"\b\d{8,}\b");
            return match.Success ? match.Value : string.Empty;
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

            // Extraction de l'ID d'invoice (format BGIYYYYMMXXXXXXX)
            var invoiceMatch = Regex.Match(label, @"BGI\d{6,}", RegexOptions.IgnoreCase);
            if (invoiceMatch.Success)
                return invoiceMatch.Value.ToUpper();

            // Extraction d'autres références utiles pour Receivable
            var referenceMatch = Regex.Match(label, @"\bG\d{4}[A-Z]{2}\d{8}\b");
            return referenceMatch.Success ? referenceMatch.Value.ToUpper() : string.Empty;
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
        /// <returns>Type de transaction détecté</returns>
        public string DetermineTransactionType(string label, bool isPivot)
        {
            if (string.IsNullOrEmpty(label))
                return "UNKNOWN";

            var upperLabel = label.ToUpper();

            if (isPivot)
            {
                if (upperLabel.Contains("COLLECTION"))
                    return "COLLECTION";
                if (upperLabel.Contains("PAYMENT") || upperLabel.Contains("AUTOMATIC REFUND"))
                    return "PAYMENT/AUTOMATIC REFUND";
                if (upperLabel.Contains("ADJUSTMENT"))
                    return "ADJUSTMENT";
                if (upperLabel.Contains("XCL LOADER"))
                    return "XCL LOADER";
                if (upperLabel.Contains("TRIGGER"))
                    return "TRIGGER";
            }
            else // Receivable
            {
                if (upperLabel.Contains("INCOMING PAYMENT"))
                    return "INCOMING PAYMENT";
                if (upperLabel.Contains("DIRECT DEBIT"))
                    return "DIRECT DEBIT";
                if (upperLabel.Contains("MANUAL OUTGOING"))
                    return "MANUAL OUTGOING";
                if (upperLabel.Contains("OUTGOING PAYMENT"))
                    return "OUTGOING PAYMENT";
                if (upperLabel.Contains("EXTERNAL DEBIT PAYMENT"))
                    return "EXTERNAL DEBIT PAYMENT";
            }

            return "OTHER";
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
        /// <param name="creditDebit">CREDIT ou DEBIT</param>
        /// <param name="isPivot">True si compte Pivot</param>
        /// <param name="guaranteeType">Type de garantie (pour Receivable)</param>
        /// <returns>Tuple (Action, KPI)</returns>
        public (ActionType? action, KPIType? kpi) ApplyAutomaticCategorization(
            string transactionType, string creditDebit, bool isPivot, string guaranteeType = null)
        {
            if (isPivot)
            {
                return ApplyPivotRules(transactionType, creditDebit);
            }
            else
            {
                return ApplyReceivableRules(transactionType, guaranteeType);
            }
        }

        private (ActionType? action, KPIType? kpi) ApplyPivotRules(string transactionType, string creditDebit)
        {
            switch (transactionType.ToUpper())
            {
                case "COLLECTION" when creditDebit == "CREDIT":
                    return (ActionType.Match, KPIType.PaidButNotReconciled);
                
                case "PAYMENT/AUTOMATIC REFUND" when creditDebit == "DEBIT":
                    return (ActionType.DoPricing, KPIType.CorrespondentChargesPendingTrigger);
                
                case "ADJUSTMENT":
                    return (ActionType.Adjust, KPIType.PaidButNotReconciled);
                
                case "XCL LOADER" when creditDebit == "CREDIT":
                    return (ActionType.Match, KPIType.PaidButNotReconciled);
                
                case "TRIGGER" when creditDebit == "CREDIT":
                    return (ActionType.Investigate, KPIType.UnderInvestigation);
                
                case "TRIGGER" when creditDebit == "DEBIT":
                    return (ActionType.ToClaim, KPIType.UnderInvestigation);
                
                default:
                    return (ActionType.NA, KPIType.ITIssues);
            }
        }

        private (ActionType? action, KPIType? kpi) ApplyReceivableRules(string transactionType, string guaranteeType)
        {
            switch (transactionType.ToUpper())
            {
                case "INCOMING PAYMENT" when guaranteeType == "REISSUANCE":
                    return (ActionType.Request, KPIType.NotClaimed);
                
                case "INCOMING PAYMENT" when guaranteeType == "ISSUANCE":
                    return (ActionType.NA, KPIType.ClaimedButNotPaid);
                
                case "INCOMING PAYMENT" when guaranteeType == "ADVISING":
                    return (ActionType.Trigger, KPIType.PaidButNotReconciled);
                
                case "DIRECT DEBIT":
                    return (ActionType.Investigate, KPIType.ITIssues);
                
                case "MANUAL OUTGOING":
                case "OUTGOING PAYMENT":
                    return (ActionType.Trigger, KPIType.CorrespondentChargesPendingTrigger);
                
                case "EXTERNAL DEBIT PAYMENT":
                    return (ActionType.Execute, KPIType.NotClaimed);
                
                default:
                    return (ActionType.Investigate, KPIType.ITIssues);
            }
        }
    }
}
