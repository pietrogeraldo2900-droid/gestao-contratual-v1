[CmdletBinding()]
param(
    [switch]$RunChecks,
    [switch]$RunTests
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Path $MyInvocation.MyCommand.Path -Parent
$projectRoot = $scriptDir
while (-not (Test-Path -LiteralPath (Join-Path -Path $projectRoot -ChildPath "run_web.py"))) {
    $parent = Split-Path -Path $projectRoot -Parent
    if ([string]::IsNullOrWhiteSpace($parent) -or $parent -eq $projectRoot) {
        throw "Nao foi possivel localizar a raiz do projeto (run_web.py) a partir de '$scriptDir'."
    }
    $projectRoot = $parent
}

Write-Host ""
Write-Host "=== RESUMO TECNICO DA INTEGRACAO (PASSO 1) ===" -ForegroundColor Cyan
Write-Host "Projeto: $projectRoot"
Write-Host ""

Write-Host "1) Shell/base estabilizado (templates/base.html)" -ForegroundColor Yellow
Write-Host "   - Base reorganizada para evitar conflito de render."
Write-Host "   - Blocos Jinja padronizados: title, extra_head, content, extra_scripts."
Write-Host "   - Assets via static com url_for (sem dependencia de runtime externo)."
Write-Host ""

Write-Host "2) UI aprovada mantida nas telas autenticadas" -ForegroundColor Yellow
Write-Host "   - templates/dashboard.html, templates/profile.html, templates/settings.html"
Write-Host "   - Layout server-rendered preservado."
Write-Host ""

Write-Host "3) Isolamento de CSS publico x autenticado" -ForegroundColor Yellow
Write-Host "   - static/css/app.css: regras de header/nav escopadas para body.public-shell."
Write-Host "   - static/css/dashboard-redesign.css: protecao explicita do hero (.mk-hero)."
Write-Host "   - Resultado: reduz vazamento visual entre /login e area autenticada."
Write-Host ""

Write-Host "4) Diagnostico local mais claro (ambiente sem banco)" -ForegroundColor Yellow
Write-Host "   - app/routes/web_app.py: mensagem de login/cadastro deixa explicito quando DB_ENABLED=0."
Write-Host "   - Log informativo no startup quando auth local esta desabilitado."
Write-Host ""

Write-Host "5) Seguranca JWT local/teste (warning removido)" -ForegroundColor Yellow
Write-Host "   - config/settings.py: fallback de secret agora usa chave 32+ bytes."
Write-Host "   - Evita InsecureKeyLengthWarning em dev/test."
Write-Host ""

Write-Host "6) Compatibilidade de testes pytest" -ForegroundColor Yellow
Write-Host "   - tests/conftest.py adiciona raiz do projeto ao sys.path."
Write-Host "   - Ajuda a rodar tanto 'pytest -q' quanto 'python -m pytest -q' (com pytest instalado)."
Write-Host ""

Write-Host "7) Zip de auditoria limpo" -ForegroundColor Yellow
Write-Host "   - scripts/gerar_zip_auditoria.ps1"
Write-Host "   - Exclui runtime/lixo e tambem data/drafts/**, mantendo apenas .gitkeep."
Write-Host ""

Write-Host "=== COMO RODAR LOCAL (BANCO + APP) ===" -ForegroundColor Cyan
Write-Host "docker compose up -d db"
Write-Host '$env:DB_ENABLED="1"'
Write-Host '$env:DB_HOST="localhost"'
Write-Host '$env:DB_PORT="5432"'
Write-Host '$env:DB_NAME="sisg"'
Write-Host '$env:DB_USER="sisg"'
Write-Host '$env:DB_PASSWORD="sisg_local"'
Write-Host '$env:DB_SSLMODE="disable"'
Write-Host '$env:WEB_APP_SECRET="sisg-local-dev-secret-key-2026-32-bytes"'
Write-Host '$env:AUTH_JWT_SECRET="sisg-local-dev-secret-key-2026-32-bytes"'
Write-Host "python run_web.py"
Write-Host ""

if ($RunChecks) {
    Write-Host "=== CHECKS RAPIDOS ===" -ForegroundColor Cyan
    Push-Location $projectRoot
    try {
        Write-Host "[check] settings atuais..."
        python -c "from config.settings import load_settings; s=load_settings(); print('DB_ENABLED=', s.db_enabled); print('DATABASE_URL=', s.database_url); print('JWT_SECRET_LEN=', len(s.auth_jwt_secret))"

        Write-Host ""
        Write-Host "[check] container postgres (sisg-postgres)..."
        try {
            docker ps --filter "name=sisg-postgres" --format "table {{.Names}}`t{{.Status}}"
        } catch {
            Write-Host "docker indisponivel neste terminal."
        }
    }
    finally {
        Pop-Location
    }
    Write-Host ""
}

if ($RunTests) {
    Write-Host "=== TESTES WEB ESSENCIAIS ===" -ForegroundColor Cyan
    Push-Location $projectRoot
    try {
        python -m unittest tests.test_web_auth_ui -v
        python -m unittest tests.test_web_dashboard -v
    }
    finally {
        Pop-Location
    }
    Write-Host ""
}

Write-Host "Script concluido." -ForegroundColor Green
