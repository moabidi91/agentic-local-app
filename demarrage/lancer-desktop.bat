@echo off
setlocal EnableExtensions
title Agentic - demarrage desktop

rem =============================================================================
rem  Demarre toute la chaine et ouvre le front dans l'application de bureau
rem  (fenetre native Tauri).
rem
rem    lancer-desktop.bat            mode demo (defaut)
rem    lancer-desktop.bat demo       modele simule + API locale + application
rem    lancer-desktop.bat reel       API locale (config.toml du depot) + application,
rem                                  aucun modele lance : c'est le votre qui repond
rem
rem  Guide de configuration : demarrage\README.md
rem =============================================================================

rem --- Reglages. En mode reel, ces deux adresses doivent correspondre a la
rem     section [api] de config.toml. La fenetre Tauri se presente comme
rem     tauri://localhost et son serveur de developpement comme localhost:1420 :
rem     les deux figurent dans [api] cors_origins par defaut.
set "API_BASE=http://127.0.0.1:8765/api/v1"
set "API_HEALTH=%API_BASE%/health"
set "MOCK_PROBE=http://127.0.0.1:9000/openapi.json"
set "FRONT_PORT=1420"
set "FRONT_URL=http://localhost:%FRONT_PORT%/"
set "TITLE_MOCK=Agentic - modele simule"
set "TITLE_API=Agentic - API locale"
set "TITLE_FRONT=Agentic - application desktop"
if not defined AGENTIC_DEMO_SCENARIO set "AGENTIC_DEMO_SCENARIO=analysis"

rem -----------------------------------------------------------------------------
rem  0. Mode
rem -----------------------------------------------------------------------------
set "MODE=%~1"
if not defined MODE set "MODE=demo"
if /I "%MODE%"=="demo" goto :mode_ok
if /I "%MODE%"=="reel" goto :mode_ok
echo [ERREUR] Mode inconnu : %MODE%
echo Usage : lancer-desktop.bat [demo^|reel]
echo   demo  modele simule local, aucun jeton, aucun appel sortant ^(defaut^)
echo   reel  votre modele, configure dans config.toml a la racine du depot
goto :fail
:mode_ok

rem -----------------------------------------------------------------------------
rem  1. Dossier du backend, deduit de l'emplacement de CE script
rem -----------------------------------------------------------------------------
for %%I in ("%~dp0..") do set "BACKEND_DIR=%%~fI"
if not exist "%BACKEND_DIR%\pyproject.toml" (
  echo [ERREUR] Dossier du backend introuvable : "%BACKEND_DIR%"
  echo Ce script doit rester dans le dossier demarrage\ du depot agentic-local-app.
  goto :fail
)
echo Mode          : %MODE%
echo Backend       : %BACKEND_DIR%

rem -----------------------------------------------------------------------------
rem  2. Dossier du front : AGENTIC_FRONT_DIR, sinon ..\agentic-front
rem -----------------------------------------------------------------------------
if defined AGENTIC_FRONT_DIR goto :front_from_env
for %%I in ("%BACKEND_DIR%\..\agentic-front") do set "FRONT_DIR=%%~fI"
goto :front_check
:front_from_env
for %%I in ("%AGENTIC_FRONT_DIR%") do set "FRONT_DIR=%%~fI"
:front_check
if exist "%FRONT_DIR%\package.json" goto :front_ok
echo [ERREUR] Dossier du front introuvable : "%FRONT_DIR%"
echo Attendu : un dossier agentic-front a cote de agentic-local-app, ou la
echo variable d'environnement AGENTIC_FRONT_DIR pointant sur le depot du front.
echo Pour la definir une fois pour toutes, dans une invite de commandes :
echo   setx AGENTIC_FRONT_DIR "C:\chemin\vers\agentic-front"
echo puis rouvrez la fenetre et relancez ce script.
goto :fail
:front_ok
echo Front         : %FRONT_DIR%
echo.

rem -----------------------------------------------------------------------------
rem  3. Prerequis
rem -----------------------------------------------------------------------------
echo --- Verification des prerequis ---
if exist "%BACKEND_DIR%\.venv\Scripts\agentic-app.exe" goto :backend_venv
where uv >nul 2>nul
if errorlevel 1 goto :backend_missing
for /f "delims=" %%v in ('uv --version 2^>nul') do echo uv detecte    : %%v
set "AGENTIC=uv run agentic-app"
goto :backend_ok
:backend_venv
echo Backend       : environnement .venv du depot
set "AGENTIC=.venv\Scripts\agentic-app.exe"
goto :backend_ok
:backend_missing
echo [ERREUR] Le backend ne peut pas demarrer : ni environnement .venv dans le
echo depot, ni uv dans le PATH.
echo Installez uv ^(recommande^) : https://docs.astral.sh/uv/getting-started/installation/
echo uv installe Python ^(^>= 3.11^) et les dependances tout seul au premier lancement.
echo Python seul : https://www.python.org/downloads/ puis, dans le depot :
echo   py -m venv .venv ^&^& .venv\Scripts\pip install -e .
goto :fail
:backend_ok

where node >nul 2>nul
if errorlevel 1 (
  echo [ERREUR] Node.js n'est pas installe ou pas dans le PATH.
  echo Installez Node.js ^(18 ou plus recent^) : https://nodejs.org/
  goto :fail
)
for /f "delims=" %%v in ('node -v') do echo Node.js       : %%v

where cargo >nul 2>nul
if errorlevel 1 goto :no_cargo
for /f "delims=" %%v in ('cargo --version') do echo Rust          : %%v
goto :cargo_ok
:no_cargo
echo [INFO] Rust ^(cargo^) n'est pas detecte : la fenetre native ne peut pas etre
echo compilee. Pour l'application de bureau il faut, en plus de Node.js :
echo   - Rust + Cargo                 https://rustup.rs/
echo   - Visual Studio Build Tools, charge de travail "Developpement Desktop en C++"
echo     https://visualstudio.microsoft.com/visual-cpp-build-tools/
echo   - WebView2 Runtime ^(deja present sur la plupart des Windows 10/11 a jour^)
echo     https://developer.microsoft.com/microsoft-edge/webview2/
echo.
echo Continuer en mode navigateur ^(lancer-web.bat^) : appuyez sur O
echo Quitter pour installer Rust d'abord          : appuyez sur N
choice /C ON /M "Votre choix"
if errorlevel 2 (
  echo Arret a votre demande : installez Rust, puis relancez ce script.
  goto :fail
)
echo.
call "%~dp0lancer-web.bat" %MODE%
exit /b %errorlevel%
:cargo_ok
echo [INFO] La compilation Rust exige aussi les Visual Studio Build Tools
echo ^(charge de travail C++^) et le runtime WebView2. Sans eux, la compilation
echo s'arrete avec une erreur dans la fenetre "%TITLE_FRONT%".

echo Preparation de l'environnement Python ^(peut prendre un moment la premiere fois^)...
cd /d "%BACKEND_DIR%"
call %AGENTIC% version
if errorlevel 1 (
  echo [ERREUR] Le backend ne repond pas a la commande "version".
  echo Regardez le message ci-dessus : dependances incompletes, ou installation a refaire.
  goto :fail
)

rem Rien ne doit deja ecouter sur les ports que nous allons prendre.
call :wait_url "%API_HEALTH%" 1
if not errorlevel 1 (
  echo [ERREUR] Une API locale repond deja sur %API_BASE%.
  echo Fermez la fenetre "%TITLE_API%" restee ouverte, puis relancez ce script.
  goto :fail
)
echo.

rem -----------------------------------------------------------------------------
rem  4. Dependances du front, premiere execution uniquement
rem -----------------------------------------------------------------------------
cd /d "%FRONT_DIR%"
if exist "node_modules" goto :npm_ok
echo --- Installation des dependances du front ---
echo ^(premiere execution uniquement, peut prendre 1 a 2 minutes^)
call npm install
if errorlevel 1 (
  echo [ERREUR] npm install a echoue. Voir le message ci-dessus.
  goto :fail
)
echo Dependances installees.
:npm_ok
echo.

rem -----------------------------------------------------------------------------
rem  5. Le modele simule, en mode demo seulement
rem -----------------------------------------------------------------------------
cd /d "%BACKEND_DIR%"
if /I not "%MODE%"=="demo" goto :model_done
echo --- Modele simule ^(scenario %AGENTIC_DEMO_SCENARIO%^) ---
start "%TITLE_MOCK%" cmd /k "%AGENTIC% mock-server --host 127.0.0.1 --port 9000 --scenario-name %AGENTIC_DEMO_SCENARIO%"
echo Attente que le modele simule reponde...
call :wait_url "%MOCK_PROBE%" 40
if errorlevel 1 (
  echo [ERREUR] Le modele simule ne repond pas sur 127.0.0.1:9000.
  echo Regardez la fenetre "%TITLE_MOCK%" : le message d'erreur y est affiche.
  goto :fail
)
echo Modele simule pret.
echo.
:model_done

rem -----------------------------------------------------------------------------
rem  6. L'API locale
rem -----------------------------------------------------------------------------
echo --- API locale ---
if /I "%MODE%"=="demo" start "%TITLE_API%" cmd /k "%AGENTIC% serve --config demarrage\config-demo.toml"
if /I "%MODE%"=="reel" start "%TITLE_API%" cmd /k "%AGENTIC% serve"
echo Attente que l'API reponde sur %API_HEALTH% ...
call :wait_url "%API_HEALTH%" 40
if errorlevel 1 (
  echo [ERREUR] L'API n'a pas repondu sur %API_HEALTH%.
  echo Regardez la fenetre "%TITLE_API%" : le message d'erreur y est affiche
  echo ^(port deja pris, configuration invalide, dependance manquante^).
  goto :fail
)
echo API prete.
echo.

rem -----------------------------------------------------------------------------
rem  7. L'application de bureau
rem -----------------------------------------------------------------------------
echo --- Application de bureau ^(npm run tauri dev^) ---
cd /d "%FRONT_DIR%"
rem VITE_API_BASE_URL est posee ici : la fenetre lancee en herite, et le serveur
rem Vite que "tauri dev" demarre lui-meme (beforeDevCommand) en herite a son tour.
rem C'est ce serveur, sur le port 1420 epingle par vite.config.ts et par le
rem devUrl de src-tauri\tauri.conf.json, que la fenetre native charge.
set "VITE_API_BASE_URL=%API_BASE%"
echo VITE_API_BASE_URL=%VITE_API_BASE_URL%
start "%TITLE_FRONT%" cmd /k "npm run tauri dev"
echo Attente que le serveur de developpement reponde sur %FRONT_URL% ...
call :wait_url "%FRONT_URL%" 60
if errorlevel 1 (
  echo [ERREUR] Le serveur de developpement n'a pas repondu sur %FRONT_URL%.
  echo Regardez la fenetre "%TITLE_FRONT%" : le port %FRONT_PORT% est peut-etre deja
  echo pris, ou npm a signale une erreur.
  goto :fail
)
echo Serveur de developpement pret.
echo.

rem -----------------------------------------------------------------------------
rem  8. Recapitulatif
rem -----------------------------------------------------------------------------
echo =============================================================================
echo  Tout est demarre ^(mode %MODE%^).
echo.
if /I "%MODE%"=="demo" echo  Fenetre "%TITLE_MOCK%"        le modele simule, sur 127.0.0.1:9000
echo  Fenetre "%TITLE_API%"          l'API locale, sur %API_BASE%
echo  Fenetre "%TITLE_FRONT%"  la compilation Rust puis l'application
echo.
echo  La fenetre native s'ouvrira d'elle-meme des que la compilation Rust sera
echo  terminee. Au tout premier lancement, comptez plusieurs minutes ; les
echo  suivants sont rapides. La progression defile dans la fenetre
echo  "%TITLE_FRONT%".
echo.
if /I "%MODE%"=="demo" echo  Donnees de la demo : %BACKEND_DIR%\data-demo
if /I "%MODE%"=="reel" echo  Donnees : dossier [app] data_dir de config.toml
echo.
echo  Pour tout arreter : fermez la fenetre de l'application, puis chaque fenetre
echo  ci-dessus ^(ou Ctrl+C dedans^). Cette fenetre-ci peut etre fermee des maintenant.
echo =============================================================================
echo.
pause
endlocal
exit /b 0

rem -----------------------------------------------------------------------------
rem  Sous-programmes
rem -----------------------------------------------------------------------------
:wait_url
rem %1 = URL a interroger, %2 = nombre de tentatives, une toutes les 500 ms
powershell -NoProfile -Command "$ok=$false; for ($i=0; $i -lt %~2; $i++) { try { Invoke-WebRequest -Uri '%~1' -UseBasicParsing -TimeoutSec 1 | Out-Null; $ok=$true; break } catch { Start-Sleep -Milliseconds 500 } }; if (-not $ok) { exit 1 }"
exit /b %errorlevel%

:fail
echo.
echo Arret : rien n'a ete laisse en marche.
call :stop_all
echo.
pause
endlocal
exit /b 1

:stop_all
call :kill_title "%TITLE_MOCK%"
call :kill_title "%TITLE_API%"
call :kill_title "%TITLE_FRONT%"
exit /b 0

:kill_title
rem %1 = titre exact de la fenetre. Les deux formes couvrent le cas ou la
rem console prefixe le titre (fenetre elevee) et celui ou elle ne le fait pas.
taskkill /FI "WINDOWTITLE eq %~1" /T /F >nul 2>nul
taskkill /FI "WINDOWTITLE eq %~1*" /T /F >nul 2>nul
exit /b 0
