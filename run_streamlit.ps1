<# 
    run_streamlit.ps1
    Usage :  
      - Ouvre PowerShell  
      - Exécute : .\run_streamlit.ps1
#>

# 1. Définir le chemin de ton projet
$projectPath = "C:\Users\User\Desktop\Travail\Homepedia\solo\Homepedia"

# 2. Se placer dans le dossier du projet
Set-Location -Path $projectPath

# 3. Activer l'environnement virtuel
#    Le point avant le chemin (".") permet de sourcer le script et d'appliquer ses modifications à la session courante.
. .\.venv\Scripts\Activate.ps1

# 4. Lancer l'application Streamlit
streamlit run src/app/streamlit_app.py
