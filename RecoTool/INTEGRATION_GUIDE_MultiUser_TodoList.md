# Guide d'Intégration : Système Multi-Utilisateur pour TodoList

## Vue d'Ensemble

Le système de notification multi-utilisateur permet de :
1. **Détecter** quand plusieurs utilisateurs consultent/modifient la même TodoList
2. **Avertir** l'utilisateur avant qu'il ne modifie un item consulté par d'autres
3. **Afficher** visuellement qui consulte/modifie quoi en temps réel

## Architecture

### Composants Créés

1. **`TodoListSessionTracker.cs`** : Service de tracking des sessions actives
   - Heartbeat automatique toutes les 10 secondes
   - Détection des sessions inactives (timeout 30s)
   - Table `T_Ref_TodoList_Sessions` dans la base référentielle

2. **`TodoListSessionWarning.xaml/.cs`** : Composant UI d'avertissement
   - Affiche les utilisateurs actifs
   - Rafraîchissement automatique toutes les 5 secondes
   - Style visuel avec icônes et couleurs

3. **`MultiUserHelper.cs`** : Helpers pour dialogs et vérifications
   - Dialog de confirmation avant modification
   - Notifications de modifications concurrentes
   - Résumés de sessions pour tooltips

## Intégration dans HomePage.xaml.cs

### 1. Ajouter le Tracker comme Membre

```csharp
public partial class HomePage : UserControl, INotifyPropertyChanged
{
    // ... existing members ...
    
    private TodoListSessionTracker _todoSessionTracker;
    
    // ... rest of code ...
}
```

### 2. Initialiser dans le Constructeur

```csharp
public HomePage(/* ... existing parameters ... */)
{
    // ... existing initialization ...
    
    // Initialize TodoList session tracker
    InitializeTodoSessionTracker();
}

private void InitializeTodoSessionTracker()
{
    try
    {
        // Get referential connection string from OfflineFirstService or config
        var referentialConnString = _offlineFirstService?.GetReferentialConnectionString();
        if (string.IsNullOrEmpty(referentialConnString))
            return;
            
        // Get current user ID (from Windows user or app config)
        var currentUserId = Environment.UserName; // or from your user management system
        
        _todoSessionTracker = new TodoListSessionTracker(referentialConnString, currentUserId);
        
        // Ensure table exists
        _ = _todoSessionTracker.EnsureTableAsync();
    }
    catch (Exception ex)
    {
        System.Diagnostics.Debug.WriteLine($"Failed to initialize TodoList session tracker: {ex.Message}");
        // Non-critical, continue without multi-user features
    }
}
```

### 3. Enregistrer la Consultation d'un Item

Quand l'utilisateur ouvre/consulte un TodoList item :

```csharp
private async Task OnTodoItemSelected(TodoListItem item)
{
    if (item == null) return;
    
    // Register viewing session
    if (_todoSessionTracker != null)
    {
        var userName = GetCurrentUserDisplayName(); // Your method to get display name
        await _todoSessionTracker.RegisterViewingAsync(item.TDL_id, userName, isEditing: false);
    }
    
    // ... rest of your selection logic ...
}
```

### 4. Vérifier Avant Modification

Avant de permettre la modification d'un item :

```csharp
private async Task<bool> OnTodoItemEdit(TodoListItem item)
{
    if (item == null) return false;
    
    // Check if other users are editing
    bool canProceed = await MultiUserHelper.CheckAndWarnBeforeEditAsync(
        _todoSessionTracker, 
        item.TDL_id, 
        item.TDL_Name);
    
    if (!canProceed)
        return false; // User cancelled
    
    // Update session to "editing" mode
    if (_todoSessionTracker != null)
    {
        var userName = GetCurrentUserDisplayName();
        await _todoSessionTracker.RegisterViewingAsync(item.TDL_id, userName, isEditing: true);
    }
    
    // ... proceed with edit ...
    return true;
}
```

### 5. Désenregistrer Quand l'Utilisateur Quitte

```csharp
private async Task OnTodoItemDeselected(TodoListItem item)
{
    if (item == null) return;
    
    // Unregister viewing session
    if (_todoSessionTracker != null)
    {
        await _todoSessionTracker.UnregisterViewingAsync(item.TDL_id);
    }
}
```

### 6. Cleanup dans Dispose/Unload

```csharp
private void HomePage_Unloaded(object sender, RoutedEventArgs e)
{
    // ... existing cleanup ...
    
    // Dispose session tracker (will clean up all sessions)
    _todoSessionTracker?.Dispose();
}
```

## Intégration du Composant Visuel dans XAML

### Option A : Dans un Dialog/Popup de TodoList

```xaml
<Window x:Class="RecoTool.Windows.TodoListDialog">
    <StackPanel>
        <!-- Warning Banner at Top -->
        <local:TodoListSessionWarning x:Name="SessionWarning" Margin="0,0,0,10"/>
        
        <!-- Rest of TodoList UI -->
        <TextBox Text="{Binding TDL_Name}" />
        <!-- ... -->
    </StackPanel>
</Window>
```

Dans le code-behind du dialog :

```csharp
public partial class TodoListDialog : Window
{
    private TodoListSessionTracker _sessionTracker;
    private int _todoId;
    
    public async Task InitializeAsync(TodoListSessionTracker sessionTracker, int todoId)
    {
        _sessionTracker = sessionTracker;
        _todoId = todoId;
        
        // Initialize warning component
        await SessionWarning.InitializeAsync(sessionTracker, todoId);
    }
    
    private void Window_Closing(object sender, CancelEventArgs e)
    {
        SessionWarning.Stop();
    }
}
```

### Option B : Dans la HomePage avec Binding

```xaml
<!-- In HomePage.xaml, add to TodoList section -->
<StackPanel>
    <local:TodoListSessionWarning x:Name="TodoSessionWarning" 
                                   Margin="0,0,0,10"
                                   Visibility="Collapsed"/>
    
    <!-- Existing TodoList DataGrid -->
    <DataGrid ItemsSource="{Binding TodoItems}" 
              SelectionChanged="TodoGrid_SelectionChanged"/>
</StackPanel>
```

Dans HomePage.xaml.cs :

```csharp
private async void TodoGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
{
    var selectedItem = (sender as DataGrid)?.SelectedItem as TodoListItem;
    
    if (selectedItem != null)
    {
        // Show warning for selected item
        TodoSessionWarning.Visibility = Visibility.Visible;
        await TodoSessionWarning.InitializeAsync(_todoSessionTracker, selectedItem.TDL_id);
    }
    else
    {
        // Hide warning when nothing selected
        TodoSessionWarning.Stop();
        TodoSessionWarning.Visibility = Visibility.Collapsed;
    }
}
```

## Fonctionnalités Avancées

### Afficher un Indicateur dans le DataGrid

Ajoutez une colonne pour montrer qui consulte chaque item :

```xaml
<DataGridTemplateColumn Header="👥" Width="40">
    <DataGridTemplateColumn.CellTemplate>
        <DataTemplate>
            <TextBlock Text="{Binding SessionSummary}" 
                       FontSize="10" 
                       Foreground="#FF8C00"
                       ToolTip="{Binding SessionTooltip}"/>
        </DataTemplate>
    </DataGridTemplateColumn.CellTemplate>
</DataGridTemplateColumn>
```

Dans votre ViewModel ou code-behind, ajoutez :

```csharp
public class TodoListItemViewModel : INotifyPropertyChanged
{
    private string _sessionSummary;
    
    public string SessionSummary
    {
        get => _sessionSummary;
        set { _sessionSummary = value; OnPropertyChanged(nameof(SessionSummary)); }
    }
    
    public async Task UpdateSessionInfoAsync(TodoListSessionTracker tracker)
    {
        SessionSummary = await MultiUserHelper.GetSessionSummaryAsync(tracker, TDL_id);
    }
}
```

### Rafraîchissement Périodique

Pour mettre à jour les indicateurs régulièrement :

```csharp
private DispatcherTimer _sessionRefreshTimer;

private void StartSessionMonitoring()
{
    _sessionRefreshTimer = new DispatcherTimer
    {
        Interval = TimeSpan.FromSeconds(10)
    };
    _sessionRefreshTimer.Tick += async (s, e) => await RefreshAllSessionIndicators();
    _sessionRefreshTimer.Start();
}

private async Task RefreshAllSessionIndicators()
{
    if (_todoSessionTracker == null) return;
    
    foreach (var item in TodoItems)
    {
        await item.UpdateSessionInfoAsync(_todoSessionTracker);
    }
}
```

## Gestion des Erreurs

Le système est conçu pour **fail gracefully** :
- Si la table de sessions n'existe pas, elle est créée automatiquement
- Si le tracking échoue, l'application continue sans notifications
- Les heartbeats sont "best effort" et ne bloquent jamais l'UI
- Les sessions expirées sont nettoyées automatiquement

## Performance

- **Heartbeat** : 10 secondes (configurable)
- **Timeout** : 30 secondes d'inactivité
- **Rafraîchissement UI** : 5 secondes (configurable)
- **Impact** : Minimal, requêtes SQL simples et indexées

## Sécurité

- Les sessions sont stockées dans la base référentielle (réseau partagé)
- Pas de données sensibles, seulement UserID/UserName et timestamps
- Cleanup automatique au Dispose de l'application

## Tests

### Test 1 : Consultation Simultanée
1. Utilisateur A ouvre un TodoList item
2. Utilisateur B ouvre le même item
3. Les deux voient un warning "X is viewing this item"

### Test 2 : Modification Concurrente
1. Utilisateur A commence à éditer un item
2. Utilisateur B essaie d'éditer le même item
3. B voit un dialog rouge "CURRENTLY BEING EDITED BY A"
4. B peut choisir de continuer ou annuler

### Test 3 : Expiration de Session
1. Utilisateur A ouvre un item puis ferme l'application brutalement
2. Après 30 secondes, le warning disparaît pour les autres utilisateurs

## Dépannage

### Les warnings ne s'affichent pas
- Vérifier que `_todoSessionTracker` est bien initialisé
- Vérifier que la table `T_Ref_TodoList_Sessions` existe
- Vérifier les logs pour les exceptions

### Les sessions ne se nettoient pas
- Vérifier que le Dispose() est appelé à la fermeture
- Vérifier que le heartbeat fonctionne (breakpoint dans HeartbeatCallback)

### Performance dégradée
- Réduire la fréquence de rafraîchissement UI (5s → 10s)
- Augmenter le timeout de session (30s → 60s)
- Vérifier que l'index sur TodoId existe
