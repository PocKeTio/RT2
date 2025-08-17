OfflineFirstAccess est une bibliothèque .NET conçue pour faciliter le développement d'applications qui doivent fonctionner de manière fiable avec des bases de données Microsoft Access, même en cas de connexion réseau intermittente ou inexistante. Elle fournit un mécanisme complet pour la synchronisation des données entre une base de données locale (sur le poste client) et une base de données distante (sur un partage réseau).

Concepts Clés
La bibliothèque est construite autour de plusieurs concepts fondamentaux pour assurer la robustesse et la flexibilité.

Offline-First
L'application interagit principalement avec une copie locale de la base de données.  Cela garantit une performance et une réactivité maximales, car les opérations ne sont pas bloquées par la latence du réseau. La synchronisation des données avec le serveur distant s'effectue en arrière-plan.

Suivi des Modifications (Change Tracking)
Toute modification (ajout, mise à jour, suppression) effectuée sur la base de données locale est enregistrée dans une table de journalisation dédiée (

_ChangeLog).  Ce journal permet à la bibliothèque de savoir exactement quelles données doivent être envoyées au serveur lors de la prochaine synchronisation.

Synchronisation Bi-directionnelle (Push/Pull)
Le processus de synchronisation se déroule en deux phases :


PUSH : Les modifications locales enregistrées dans le _ChangeLog sont envoyées et appliquées à la base de données distante. 



PULL : Les modifications de la base de données distante (depuis la dernière synchronisation) sont récupérées et appliquées à la base de données locale. 

Détection de Conflits Manuelle
La bibliothèque adopte une approche de sécurité maximale pour les conflits. Si une même donnée a été modifiée à la fois localement et à distance, la bibliothèque ne prend pas de décision automatique (ce qui pourrait causer une perte de données). Elle 


détecte le conflit et le signale à l'application appelante.  C'est à l'application de décider comment résoudre ce conflit (par exemple, en demandant à l'utilisateur de choisir quelle version conserver).

Gestion de Schéma par Programmation
Grâce aux 

DatabaseTemplateBuilder et DatabaseTemplateFactory, vous pouvez définir la structure de vos bases de données directement en C#, créant ainsi des templates réutilisables et garantissant que toutes les bases de données (locales et distantes) ont une structure cohérente. 




Guide de Démarrage Rapide
1. Prérequis
Assurez-vous que le Microsoft Access Database Engine (pilote ACE OLEDB) est installé sur les postes où l'application s'exécutera.

2. Configuration
Tout commence par la création d'une SyncConfiguration. Cet objet définit les tables à synchroniser et les chemins vers les bases de données.

C#

var config = new SyncConfiguration
{
    LocalDatabasePath = @"C:\Temp\Local_Data.accdb",
    RemoteDatabasePath = @"\\MonServeur\Partage\Remote_Data.accdb",
    TablesToSync = new List<string> { "T_Data_Ambre", "T_Reconciliation" },

    // Noms des colonnes de métadonnées (peuvent être laissés par défaut)
    PrimaryKeyGuidColumn = "RowGuid",
    LastModifiedColumn = "LastModifiedUTC",
    IsDeletedColumn = "IsDeleted"
};
3. Création de la Base de Données
Utilisez la DatabaseTemplateFactory pour créer les fichiers de base de données avec le bon schéma.

C#

// Action pour définir la structure de vos tables
Action<DatabaseTemplateBuilder> configureSchema = builder =>
{
    builder.AddTable("T_Data_Ambre")
           .WithPrimaryKey("ID", typeof(int), true)
           .WithColumn("RowGuid", typeof(string), false) // Important pour la synchro
           .WithColumn("LastModifiedUTC", typeof(DateTime), false) // Important pour la synchro
           .WithColumn("IsDeleted", typeof(bool), false) // Important pour la synchro
           .WithColumn("NomClient", typeof(string))
           .WithColumn("Montant", typeof(double));

    builder.AddTable("T_Reconciliation")
           .WithPrimaryKey("RecoID", typeof(int), true)
           // ... autres colonnes
           .EndTable(); // Termine la définition de la table
};

// Créer les bases de données locale et distante
await DatabaseTemplateFactory.CreateCustomTemplateAsync(config.LocalDatabasePath, configureSchema);
await DatabaseTemplateFactory.CreateCustomTemplateAsync(config.RemoteDatabasePath, configureSchema);
4. Initialisation du Service
Instanciez et initialisez le service de synchronisation.

C#

ISynchronizationService syncService = new SynchronizationService();
await syncService.InitializeAsync(config);
5. Lancer la Synchronisation
Appelez la méthode SynchronizeAsync et gérez le résultat.

C#

// Définir une action pour suivre la progression (optionnel)
Action<int, string> onProgress = (percent, message) =>
{
    Console.WriteLine($"[{percent}%] {message}");
};

SyncResult result = await syncService.SynchronizeAsync(onProgress);

if (result.Success)
{
    Console.WriteLine("Synchronisation terminée avec succès !");

    // POINT CRUCIAL : Gérer les conflits non résolus
    if (result.UnresolvedConflicts.Any())
    {
        Console.WriteLine($"Attention : {result.UnresolvedConflicts.Count} conflit(s) détecté(s).");
        foreach (var conflict in result.UnresolvedConflicts)
        {
            // Ici, implémentez votre logique :
            // 1. Afficher les versions locale et distante à l'utilisateur.
            // 2. Lui demander de choisir la version à conserver.
            // 3. Appliquer la résolution en base de données.
            Console.WriteLine($"Conflit sur la table {conflict.TableName} pour l'enregistrement {conflict.RowGuid}");
        }
    }
}
else
{
    Console.WriteLine($"Échec de la synchronisation : {result.Message}");
    Console.WriteLine(result.Error?.ToString());
}
6. Enregistrer les Modifications
Pour que la synchronisation fonctionne, vous devez manuellement enregistrer chaque modification de données dans votre application à l'aide d'un ChangeTracker.

C#

// Après avoir initialisé le service, créez une instance du ChangeTracker
IChangeTracker changeTracker = new ChangeTracker(config.LocalDatabasePath);

// EXEMPLE : Après avoir mis à jour un enregistrement
string rowGuid = "GUID_DE_LIGNE_MODIFIEE";
await changeTracker.RecordChangeAsync("T_Data_Ambre", rowGuid, "UPDATE");

// EXEMPLE : Après avoir inséré un nouvel enregistrement
string newRowGuid = "GUID_DE_LA_NOUVELLE_LIGNE";
await changeTracker.RecordChangeAsync("T_Data_Ambre", newRowGuid, "INSERT");
Fonctionnement Détaillé
Structure des Composants
SynchronizationService: Façade simple pour l'application. Elle configure et expose le 

SyncOrchestrator. 



SyncOrchestrator: Le moteur interne qui exécute la logique de PUSH et de PULL. 



IDataProvider (AccessDataProvider): Implémente les opérations de bas niveau sur une base de données Access (lire, écrire, etc.). La bibliothèque en utilise deux instances : une pour le local, une pour le distant. 



IChangeTracker (ChangeTracker): Gère la lecture et l'écriture dans la table _ChangeLog. 



IConflictResolver (ManualConflictResolver): Détecte les conflits mais ne les résout pas, garantissant la sécurité des données. 


Helpers: Contient des classes utilitaires puissantes comme :

DatabaseTemplateBuilder / Factory pour la gestion de schéma.

RetryHelper pour la résilience réseau.

LogManager pour une journalisation asynchrone.

Résilience du Processus
Le processus de synchronisation est conçu pour être résilient. Notamment, l'horodatage de la dernière synchronisation est mis à jour 

après chaque table traitée lors de la phase de PULL. Si le processus est interrompu, il reprendra intelligemment là où il s'est arrêté, sans avoir à tout retraiter. 