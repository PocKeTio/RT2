# Test du Système Multi-Utilisateur TodoList

## 🔍 Diagnostic Initial

**Problème** : Aucune ligne dans `T_TodoList_Sessions` après ouverture/modification de TodoList.

**Cause** : Le système était créé mais **jamais appelé** car votre UI TodoList n'utilise pas de dialog séparé.

**Solution** : Intégration directe dans `HomePage.OpenTodoCard_Click()`.

## ✅ Corrections Appliquées

### 1. Correction de la Connection String
- **Avant** : `GetLockDatabasePath()` → retournait un chemin
- **Après** : `GetControlConnectionString()` → retourne une connection string complète
- **Fichier** : `HomePage.xaml.cs` ligne 2368

### 2. Tracking dans OpenTodoCard_Click
- **Ajouté** : `RegisterViewingAsync()` quand on clique sur une TodoCard
- **Ajouté** : `UnregisterViewingAsync()` de la TodoList précédente
- **Fichier** : `HomePage.xaml.cs` lignes 235-250

### 3. Tracking de la TodoList Active
- **Ajouté** : `_currentlyViewingTodoId` pour savoir quelle TodoList est active
- **Fichier** : `HomePage.xaml.cs` ligne 37

### 4. Cleanup Amélioré
- **Ajouté** : Désenregistrement dans `CleanupTodoSessionTracker()`
- **Fichier** : `HomePage.xaml.cs` lignes 2405-2408

## 🧪 Tests à Effectuer

### Test 1 : Vérification de Base (1 utilisateur)

1. **Ouvrir l'application**
2. **Sélectionner un country** (ex: FR)
3. **Cliquer sur une TodoCard** (ex: "Invoices to review")
4. **Vérifier la base de données** :

```sql
-- Ouvrir la Lock DB du country (ex: DB_FR_lock.accdb)
SELECT * FROM T_TodoList_Sessions;
```

**Résultat attendu** :
```
SessionId | CountryId | TodoId | UserId        | UserName      | SessionStart        | LastHeartbeat       | IsEditing
1         | FR        | 5      | gianni        | gianni        | 2025-10-05 02:00:00 | 2025-10-05 02:00:10 | False
```

### Test 2 : Heartbeat (1 utilisateur)

1. **Laisser l'application ouverte** avec une TodoCard active
2. **Attendre 15 secondes**
3. **Vérifier la base de données** :

```sql
SELECT SessionId, UserId, LastHeartbeat, IsEditing 
FROM T_TodoList_Sessions 
WHERE CountryId = 'FR';
```

**Résultat attendu** :
- `LastHeartbeat` doit être mis à jour toutes les **10 secondes**

### Test 3 : Changement de TodoCard

1. **Cliquer sur TodoCard #1** (ex: "Invoices to review")
2. **Vérifier DB** → 1 ligne avec TodoId = 1
3. **Cliquer sur TodoCard #2** (ex: "Missing invoices")
4. **Vérifier DB** → 1 ligne avec TodoId = 2 (l'ancienne est supprimée)

**Résultat attendu** :
- Une seule session active à la fois par utilisateur

### Test 4 : Multi-Utilisateur (2 PCs)

**PC 1 (User A)** :
1. Ouvrir l'app, sélectionner country FR
2. Cliquer sur TodoCard "Invoices to review" (TodoId = 5)

**PC 2 (User B)** :
1. Ouvrir l'app, sélectionner country FR
2. Cliquer sur la même TodoCard "Invoices to review" (TodoId = 5)
3. **Vérifier DB** :

```sql
SELECT UserId, UserName, LastHeartbeat, IsEditing 
FROM T_TodoList_Sessions 
WHERE CountryId = 'FR' AND TodoId = 5;
```

**Résultat attendu** :
```
UserId  | UserName | LastHeartbeat       | IsEditing
userA   | User A   | 2025-10-05 02:00:10 | False
userB   | User B   | 2025-10-05 02:00:15 | False
```

### Test 5 : Expiration de Session (Crash)

1. **User A** : Ouvrir TodoCard
2. **Vérifier DB** → Session active
3. **User A** : Tuer le processus (Task Manager → End Task)
4. **Attendre 35 secondes** (timeout = 30s + marge)
5. **User B** : Ouvrir la même TodoCard
6. **Vérifier DB** :

**Résultat attendu** :
- La session de User A est **automatiquement supprimée** (cleanup des sessions expirées)
- Seule la session de User B reste

### Test 6 : Changement de Country

1. **User A** : Ouvrir TodoCard sur country FR
2. **Vérifier DB FR** → Session active
3. **User A** : Changer de country vers BE
4. **Vérifier DB FR** → Session supprimée
5. **Vérifier DB BE** → Aucune session (normal, pas de TodoCard ouverte)

## 🐛 Dépannage

### Problème : Toujours aucune ligne dans T_TodoList_Sessions

**Solutions** :

1. **Vérifier que la table existe** :
   ```sql
   -- Dans Lock DB (ex: DB_FR_lock.accdb)
   SELECT * FROM MSysObjects WHERE Name = 'T_TodoList_Sessions';
   ```
   - Si la table n'existe pas, elle devrait être créée automatiquement au premier `RegisterViewingAsync()`

2. **Vérifier les logs** :
   - Ouvrir Visual Studio Output window
   - Chercher les erreurs de type "Failed to initialize TodoList session tracker"

3. **Vérifier que _todoSessionTracker n'est pas null** :
   - Mettre un breakpoint dans `OpenTodoCard_Click` ligne 242
   - Vérifier `_todoSessionTracker != null`
   - Vérifier `card.Item.TDL_id > 0`

4. **Vérifier la connection string** :
   - Breakpoint dans `InitializeTodoSessionTracker()` ligne 2368
   - Vérifier que `lockDbConnString` contient bien un chemin valide
   - Exemple attendu : `Provider=Microsoft.ACE.OLEDB.16.0;Data Source=\\server\share\DB_FR_lock.accdb;`

### Problème : Exception "Could not find file"

**Cause** : Le chemin de la Lock DB est incorrect.

**Solution** :
1. Vérifier dans `App.config` ou `Settings` :
   - `CountryDatabaseDirectory` pointe vers le bon dossier réseau
   - `ControlDatabasePrefix` ou `CountryDatabasePrefix` est correct (ex: "DB_")

2. Vérifier manuellement que le fichier existe :
   - Exemple : `\\server\share\DB_FR_lock.accdb`

### Problème : Sessions ne se nettoient pas

**Cause** : Le Dispose() n'est pas appelé.

**Solution** :
1. Ajouter un event handler `Unloaded` dans HomePage.xaml :
   ```xaml
   <UserControl Unloaded="HomePage_Unloaded">
   ```

2. Dans HomePage.xaml.cs :
   ```csharp
   private void HomePage_Unloaded(object sender, RoutedEventArgs e)
   {
       CleanupTodoSessionTracker();
   }
   ```

### Problème : Heartbeat ne fonctionne pas

**Vérification** :
1. Breakpoint dans `TodoListSessionTracker.HeartbeatCallback()` ligne 279
2. Vérifier que le timer s'exécute toutes les 10 secondes
3. Vérifier que `_trackedTodoIds` contient bien le TodoId actif

## 📊 Requêtes SQL Utiles

### Voir toutes les sessions actives
```sql
SELECT 
    SessionId,
    CountryId,
    TodoId,
    UserId,
    UserName,
    SessionStart,
    LastHeartbeat,
    IIF(IsEditing, 'Editing', 'Viewing') AS Status,
    DateDiff('s', LastHeartbeat, Now()) AS SecondsSinceHeartbeat
FROM T_TodoList_Sessions
ORDER BY LastHeartbeat DESC;
```

### Voir les sessions par TodoList
```sql
SELECT 
    TodoId,
    COUNT(*) AS ActiveUsers,
    SUM(IIF(IsEditing, 1, 0)) AS EditingUsers,
    SUM(IIF(IsEditing, 0, 1)) AS ViewingUsers
FROM T_TodoList_Sessions
WHERE CountryId = 'FR'
GROUP BY TodoId;
```

### Nettoyer manuellement les sessions expirées
```sql
DELETE FROM T_TodoList_Sessions
WHERE DateDiff('s', LastHeartbeat, Now()) > 30;
```

### Supprimer toutes les sessions (reset)
```sql
DELETE FROM T_TodoList_Sessions;
```

## ✅ Checklist de Validation

- [ ] Table `T_TodoList_Sessions` créée dans Lock DB
- [ ] Session créée quand on clique sur une TodoCard
- [ ] Heartbeat mis à jour toutes les 10 secondes
- [ ] Session supprimée quand on change de TodoCard
- [ ] Session supprimée quand on change de country
- [ ] Sessions expirées nettoyées automatiquement (30s)
- [ ] Multi-utilisateur : 2 users voient 2 sessions distinctes
- [ ] Isolation par country : FR ne voit pas BE

## 🚀 Prochaine Étape : Warnings Visuels

Une fois que le tracking fonctionne, vous pourrez ajouter :

1. **Banner de warning** dans ReconciliationPage
2. **Dialog de confirmation** avant modification
3. **Indicateur visuel** sur les TodoCards (ex: "👥 2 users")

Mais d'abord, **validez que le tracking fonctionne** avec les tests ci-dessus !
