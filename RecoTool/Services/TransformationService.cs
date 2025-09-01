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
        public TransactionType? DetermineTransactionType(string label, bool isPivot)
        {
            if (string.IsNullOrEmpty(label))
                return null;

            var upperLabel = label.ToUpper();

            if (isPivot)
            {
                if (upperLabel.Contains("COLLECTION"))
                    return TransactionType.COLLECTION;
                if (upperLabel.Contains("PAYMENT") || upperLabel.Contains("AUTOMATIC REFUND"))
                    return TransactionType.PAYMENT;
                if (upperLabel.Contains("ADJUSTMENT"))
                    return TransactionType.ADJUSTMENT;
                if (upperLabel.Contains("XCL LOADER"))
                    return TransactionType.XCL_LOADER;
                if (upperLabel.Contains("TRIGGER"))
                    return TransactionType.TRIGGER;
            }
            else // Receivable
            {
                if (upperLabel.Contains("INCOMING PAYMENT"))
                    return TransactionType.INCOMING_PAYMENT;
                if (upperLabel.Contains("DIRECT DEBIT"))
                    return TransactionType.DIRECT_DEBIT;
                if (upperLabel.Contains("MANUAL OUTGOING"))
                    return TransactionType.MANUAL_OUTGOING;
                if (upperLabel.Contains("OUTGOING PAYMENT"))
                    return TransactionType.OUTGOING_PAYMENT;
                if (upperLabel.Contains("EXTERNAL DEBIT PAYMENT"))
                    return TransactionType.EXTERNAL_DEBIT_PAYMENT;
            }

            return null;
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
            
            if (isPivot)
            {
                return (transactionType, isCredit) switch
                {
                    (TransactionType.COLLECTION, true) => 
                        (ActionType.Match, KPIType.PaidButNotReconciled),
                    (TransactionType.COLLECTION, false) => 
                        (ActionType.NA, KPIType.ITIssues),
                    (TransactionType.PAYMENT, true) => 
                        (ActionType.NA, KPIType.ITIssues),
                    (TransactionType.PAYMENT, false) => 
                        (ActionType.DoPricing, KPIType.CorrespondentChargesToBeInvoiced),
                    (TransactionType.ADJUSTMENT, true) => 
                        (ActionType.Adjust, KPIType.PaidButNotReconciled),
                    (TransactionType.ADJUSTMENT, false) => 
                        (ActionType.Adjust, KPIType.PaidButNotReconciled),
                    (TransactionType.XCL_LOADER, true) => 
                        (ActionType.Match, KPIType.PaidButNotReconciled),
                    (TransactionType.TRIGGER, true) => 
                        (ActionType.Investigate, KPIType.UnderInvestigation),
                    (TransactionType.TRIGGER, false) => 
                        (ActionType.ToClaim, KPIType.UnderInvestigation),
                    _ => (ActionType.NA, KPIType.ITIssues)
                };
            }
            else
            {
                return (guaranteeType, transactionType) switch
                {
                    ("REISSUANCE", TransactionType.INCOMING_PAYMENT) => 
                        (ActionType.Request, KPIType.NotClaimed),
                    ("ISSUANCE", TransactionType.INCOMING_PAYMENT) => 
                        (ActionType.NA, KPIType.ClaimedButNotPaid),
                    ("ADVISING", TransactionType.INCOMING_PAYMENT) => 
                        (ActionType.Trigger, KPIType.PaidButNotReconciled),
                    (_, TransactionType.DIRECT_DEBIT) => 
                        (ActionType.Investigate, KPIType.ITIssues),
                    (_, TransactionType.MANUAL_OUTGOING) => 
                        (ActionType.Trigger, KPIType.CorrespondentChargesPendingTrigger),
                    (_, TransactionType.OUTGOING_PAYMENT) => 
                        (ActionType.Trigger, KPIType.CorrespondentChargesPendingTrigger),
                    (_, TransactionType.EXTERNAL_DEBIT_PAYMENT) => 
                        (ActionType.Execute, KPIType.NotClaimed),
                    _ => (ActionType.NA, KPIType.ITIssues)
                };
            }
        }

        private (ActionType action, KPIType kpi) ApplyPivotRules(TransactionType transactionType, decimal signedAmount)
        {
            bool isCredit = signedAmount > 0;
            
            switch (transactionType)
            {
                case TransactionType.COLLECTION when isCredit:
                    return (ActionType.Match, KPIType.PaidButNotReconciled);
                
                case TransactionType.PAYMENT when !isCredit:
                    return (ActionType.DoPricing, KPIType.CorrespondentChargesPendingTrigger);
                
                case TransactionType.ADJUSTMENT:
                    return (ActionType.Adjust, KPIType.PaidButNotReconciled);
                
                case TransactionType.XCL_LOADER when isCredit:
                    return (ActionType.Match, KPIType.PaidButNotReconciled);
                
                case TransactionType.TRIGGER when isCredit:
                    return (ActionType.Investigate, KPIType.UnderInvestigation);
                
                case TransactionType.TRIGGER when !isCredit:
                    return (ActionType.ToClaim, KPIType.UnderInvestigation);
                
                default:
                    return (ActionType.NA, KPIType.ITIssues);
            }
        }

        private (ActionType action, KPIType kpi) ApplyReceivableRules(TransactionType transactionType, string guaranteeType)
        {
            switch (transactionType)
            {
                case TransactionType.INCOMING_PAYMENT when guaranteeType == "REISSUANCE":
                    return (ActionType.Request, KPIType.NotClaimed);
                
                case TransactionType.INCOMING_PAYMENT when guaranteeType == "ISSUANCE":
                    return (ActionType.NA, KPIType.ClaimedButNotPaid);
                
                case TransactionType.INCOMING_PAYMENT when guaranteeType == "ADVISING":
                    return (ActionType.Trigger, KPIType.PaidButNotReconciled);
                
                case TransactionType.DIRECT_DEBIT:
                    return (ActionType.Investigate, KPIType.ITIssues);
                
                case TransactionType.MANUAL_OUTGOING:
                case TransactionType.OUTGOING_PAYMENT:
                    return (ActionType.Trigger, KPIType.CorrespondentChargesPendingTrigger);
                
                case TransactionType.EXTERNAL_DEBIT_PAYMENT:
                    return (ActionType.Execute, KPIType.NotClaimed);
                
                default:
                    return (ActionType.Investigate, KPIType.ITIssues);
            }
        }
    }
}