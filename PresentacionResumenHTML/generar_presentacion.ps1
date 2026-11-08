$ErrorActionPreference = 'Stop'

$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$htmlDirectory = Join-Path $scriptDirectory 'HTMLS'
$exportsDirectory = Join-Path (Split-Path -Parent $scriptDirectory) 'NoteBooksHTML'
$officialNotebooks = @(
    '0100_ExtraccionInformacion',
    '0300_CleanTransformParquet_8',
    '0310_AuditoriaDesenlace',
    '0320_DiagnosticoFanout',
    '0330_BaseAnaliticaV3_3',
    '0150_ClimaBogota_2',
    '0160_ClimaAlineado_3',
    '0210_AnalisisExploratorioV3',
    '0340_VerificacionSupuestos',
    '0350_RutaVigenteAlAdmitir',
    '0411_Preparacion_Modelado',
    '0412_Modelos_Base',
    '0413_RandomForest',
    '0414_XGBoost',
    '0415_LightGBM',
    '0416_Comparacion_PruebaFinal',
    '0420_Interpretabilidad_4',
    '0417_Sensibilidad',
    '0418_EfectoSabado',
    '0430_AsignacionZonas',
    '0440_MoranResiduos'
)

if (-not (Test-Path -LiteralPath $exportsDirectory -PathType Container)) {
    throw "No se encontró la carpeta de exportaciones: $exportsDirectory"
}
if (-not (Test-Path -LiteralPath $htmlDirectory -PathType Container)) {
    [void](New-Item -ItemType Directory -Path $htmlDirectory)
}

function Get-Phase([string] $name) {
    if ($name -match '^(0100|0110)_') { return '01. Extracción' }
    if ($name -match '^(0150|0160)_') { return '02. Contexto climático' }
    if ($name -match '^(0200|0210)_') { return '03. Exploración' }
    if ($name -match '^(0300|0305|0310|0320|0330)_') { return '04. Calidad y base analítica' }
    if ($name -match '^(0340|0345|0350)_') { return '05. Supuestos y auditorías' }
    if ($name -match '^04(11|12|13|14|15|16|17|18|19)_') { return '06. Modelado y evaluación' }
    if ($name -match '^0420_') { return '07. Interpretabilidad' }
    if ($name -match '^(0430|0440)_') { return '08. Análisis espacial' }
    if ($name -match '^0500_') { return '09. Privacidad' }
    return 'Otros'
}

function Get-CellKind($command) {
    $text = [string]$command.command
    if ($text -match '^\s*%md\b') { return 'markdown' }
    return 'code'
}

$notebooks = @()
foreach ($file in (Get-ChildItem -LiteralPath $exportsDirectory -Filter '*.html' -File | Sort-Object Name)) {
    $html = [IO.File]::ReadAllText($file.FullName)
    $match = [regex]::Match($html, "var __DATABRICKS_NOTEBOOK_MODEL = '([^']+)'")
    if (-not $match.Success) {
        throw "El HTML no contiene el modelo de notebook esperado: $($file.Name)"
    }

    $encodedModel = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($match.Groups[1].Value))
    $modelJson = [uri]::UnescapeDataString($encodedModel)
    $model = $modelJson | ConvertFrom-Json
    if (-not $model.name -or -not $model.commands) {
        throw "El modelo de notebook no tiene nombre o celdas: $($file.Name)"
    }

    $cells = @()
    $codeOrdinal = 0
    foreach ($command in $model.commands) {
        $kind = Get-CellKind $command
        if ($kind -eq 'code') { $codeOrdinal++ }
        $resultType = if ($command.results) { [string]$command.results.type } else { '' }
        $displayType = if ($command.displayType) { [string]$command.displayType } else { '' }
        $cells += [PSCustomObject]@{
            kind = $kind
            codeOrdinal = if ($kind -eq 'code') { $codeOrdinal } else { 0 }
            command = [string]$command.command
            state = [string]$command.state
            resultType = $resultType
            displayType = $displayType
            hasError = [bool]($command.error -or $command.errorSummary)
        }
    }

    $notebooks += [PSCustomObject]@{
        id = [string]$model.name
        file = $file.Name
        name = [string]$model.name
        phase = Get-Phase ([string]$model.name)
        language = [string]$model.language
        version = if ($officialNotebooks -contains [string]$model.name) {
            'En el flujo principal documentado'
        } elseif ([string]$model.name -match 'ARCHIVAR') {
            'Archivado: referencia histórica'
        } else {
            'Variante o antecedente: contrastar antes de usar'
        }
        cells = $cells
    }
}

if ($notebooks.Count -eq 0) {
    throw "No se encontraron HTML exportados en $exportsDirectory"
}
$codeCount = @($notebooks | ForEach-Object { $_.cells } | Where-Object kind -eq 'code').Count
$markdownCount = @($notebooks | ForEach-Object { $_.cells } | Where-Object kind -eq 'markdown').Count
$json = ConvertTo-Json -InputObject $notebooks -Depth 100 -Compress
$json = $json.Replace('<', '\u003c').Replace([string][char]0x2028, '\u2028').Replace([string][char]0x2029, '\u2029')

$page = @'
<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="Presentación didáctica de los notebooks de Databricks, sus celdas, resultados y dependencias.">
<title>Del dato a la decisión | Notebooks Databricks</title>
<style>
:root{--bg:#f4f7f9;--paper:#fff;--soft:#e5edf1;--line:#96c2db;--blue:#68a3c7;--ink:#1a252c;--muted:#526874;--code:#0f171c;--ok:#246447;--warn:#795812;--shadow:0 12px 32px rgba(26,37,44,.07)}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.65 Arial,Helvetica,sans-serif}
button,input{font:inherit}button{cursor:pointer}.shell{max-width:1180px;margin:auto;padding:28px 22px 64px}
header{background:var(--paper);border:1px solid var(--line);border-radius:16px;padding:30px 34px;box-shadow:var(--shadow)}
.eyebrow{margin:0 0 8px;color:#356986;text-transform:uppercase;letter-spacing:.11em;font-size:11px;font-weight:700}
h1{font-size:clamp(27px,4vw,40px);line-height:1.15;margin:0 0 12px;letter-spacing:-.03em}
h2{font-size:23px;margin:0 0 9px;line-height:1.3}h3{font-size:17px;margin:0 0 8px}p{margin:0 0 12px}
.lede{max-width:850px;color:var(--muted);font-size:16px}.byline{font-size:12px;color:var(--muted);margin:14px 0 0}
.stats{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin:18px 0}
.stat{background:var(--paper);border:1px solid #d5e4ec;border-radius:12px;padding:15px 17px}.stat strong{display:block;font-size:24px;line-height:1.2}.stat span{font-size:12px;color:var(--muted)}
.notice{border-left:4px solid var(--blue);background:#edf5f9;padding:14px 17px;border-radius:0 10px 10px 0;margin:16px 0;color:#314b59}
.notice.warning{border-color:#d0aa51;background:#fcf7e9}.section{margin-top:22px;background:var(--paper);border:1px solid #d5e4ec;border-radius:14px;padding:22px;box-shadow:0 5px 16px rgba(26,37,44,.035)}
.flow{display:flex;flex-direction:column;align-items:stretch;max-width:760px;margin:20px auto 8px;gap:8px}.flow-step{position:relative;padding:13px 17px;border:1px solid var(--line);border-radius:10px;background:#fff}
.flow-step strong{display:block}.flow-step small{display:block;color:var(--muted);margin-top:3px}.flow-arrow{text-align:center;color:#4f819d;font-size:20px;line-height:1}.flow-branch{border-style:dashed;background:#f7fbfd}
.explorer-layout{display:grid;grid-template-columns:220px minmax(0,1fr);gap:22px;align-items:start;margin-top:18px}
.phase-sidebar{position:sticky;top:14px;border:1px solid #d5e4ec;background:#f8fbfc;border-radius:12px;padding:13px}
.phase-sidebar h3{font-size:13px;color:var(--muted);margin:2px 8px 10px}
.phase-nav{display:flex;flex-direction:column;gap:5px}.phase-button,.notebook-button,.toolbar button{border:1px solid transparent;background:transparent;color:var(--ink);border-radius:8px;padding:9px 11px;font-weight:600}
.phase-button{text-align:left;width:100%;line-height:1.35}.phase-button.active{background:#d7eaf4;border-color:#b8d7e7;color:#173c51}.phase-button:hover,.notebook-button:hover,.toolbar button:hover{background:#e8f2f6}
.phase-button:focus-visible,.notebook-button:focus-visible,.toolbar button:focus-visible,.search:focus-visible{outline:3px solid #68a3c7;outline-offset:2px}
.explorer-content{min-width:0}.selector-head{display:flex;justify-content:space-between;align-items:center;gap:14px;margin:14px 0 8px}.notebook-nav{display:flex;gap:5px;flex-wrap:nowrap;overflow-x:auto;margin:10px 0 18px;padding:3px 2px 11px;border-bottom:1px solid #d5e4ec;scrollbar-color:var(--line) transparent}
.notebook-button{flex:0 0 auto;white-space:nowrap;border-bottom:2px solid transparent;border-radius:8px 8px 0 0}
.notebook-button.active{background:#edf5f9;border-color:#b8d7e7;border-bottom-color:#68a3c7;color:#173c51}.notebook-button,.toolbar button{border:1px solid transparent}
.toolbar{display:flex;gap:8px;flex-wrap:wrap}.toolbar button{font-size:12px;padding:7px 10px}
.search{width:100%;max-width:420px;padding:10px 12px;border:1px solid #bdd2dd;border-radius:9px;background:#fff;color:var(--ink)}
.meta-row{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0 18px}.badge{display:inline-flex;align-items:center;border:1px solid #d2e1e9;background:#f3f8fa;color:#3f5b69;padding:4px 9px;border-radius:999px;font-size:11px}
.badge.ok{color:var(--ok);background:#edf7f0;border-color:#bfdec9}.badge.warn{color:var(--warn);background:#fcf7e9;border-color:#ead9a9}
.cell-card{border:1px solid #d5e4ec;border-radius:12px;margin:12px 0;overflow:hidden}.cell-heading{display:flex;align-items:center;justify-content:space-between;gap:14px;padding:12px 15px;background:#f0f5f7;border-bottom:1px solid #d5e4ec}
.cell-title{font-weight:700}.cell-content{padding:15px}.cell-context{border-left:3px solid var(--line);padding-left:12px;color:var(--muted);margin-bottom:13px}
.explanation{background:#f7fafb;border-radius:9px;padding:12px 14px;margin-bottom:12px}.explanation strong,.output strong{display:block;margin-bottom:3px}
.output{background:#edf5f9;border-left:3px solid var(--blue);border-radius:0 9px 9px 0;padding:11px 13px;color:#35515f}
pre{margin:0;background:var(--code);color:#e5edf1;padding:14px;border-radius:9px;overflow:auto;max-height:520px;white-space:pre;font:12px/1.6 "Courier New",Courier,monospace}
.md-card{border:1px solid #d9e7ed;background:#fbfdfe;border-radius:10px;padding:13px 15px;margin:10px 0;color:#405966}.md-card h3{margin:0 0 6px}
.empty{padding:28px;text-align:center;color:var(--muted);border:1px dashed var(--line);border-radius:12px}
footer{color:var(--muted);font-size:12px;text-align:center;margin-top:24px}
@media(max-width:760px){.explorer-layout{grid-template-columns:1fr;gap:12px}.phase-sidebar{position:static}.phase-nav{flex-direction:row;overflow-x:auto;padding-bottom:7px}.phase-button{width:auto;flex:0 0 auto;white-space:nowrap}.phase-sidebar h3{margin-bottom:5px}}
@media(max-width:680px){.shell{padding:14px 12px 36px}header,.section{padding:19px}.stats{grid-template-columns:1fr}.selector-head{align-items:flex-start;flex-direction:column}.cell-heading{align-items:flex-start;flex-direction:column}}
</style>
</head>
<body>
<main class="shell">
<header>
  <p class="eyebrow">Presentación interactiva · Databricks · Bogotá</p>
  <h1>Del dato a la decisión</h1>
  <p class="lede">Recorrido celda por celda por la extracción, preparación, análisis y modelado del tiempo de última milla y el riesgo de incumplir el SLA. Cada cuaderno se explica dentro de su etapa y sus dependencias.</p>
  <p class="byline">Trabajo de grado · Maestría en Ciencia de Datos · Servienvios</p>
</header>
<div class="stats">
  <div class="stat"><strong>__TOTAL_NOTEBOOKS__</strong><span>exportaciones HTML válidas revisadas</span></div>
  <div class="stat"><strong>__CODE_COUNT__</strong><span>celdas de código explicadas</span></div>
  <div class="stat"><strong>__MARKDOWN_COUNT__</strong><span>celdas de contexto preservadas</span></div>
</div>
<div class="notice warning"><strong>Privacidad.</strong> Los HTML de ejecución pueden contener filas de guías, rutas y otros datos del trabajo. Esta presentación muestra el código y el tipo/estado del resultado, pero omite los valores de salida. No publiques los HTML originales sin autorización.</div>
<section class="section" id="overview">
  <p class="eyebrow">00 · Arquitectura</p><h2>Cómo se conectan las etapas</h2>
  <p>La ruta principal transforma registros operativos en una base por guía, prepara validaciones temporales y compara modelos antes de la única prueba final. El clima y el análisis espacial son líneas complementarias.</p>
  <div class="flow" aria-label="Flujo vertical de notebooks">
    <div class="flow-step"><strong>1 · Extraer y conservar fuentes</strong><small>0100 obtiene guías, movimientos, documentos y paramétricas; los Parquet son el insumo de Databricks.</small></div>
    <div class="flow-arrow" aria-hidden="true">↓</div>
    <div class="flow-step"><strong>2 · Limpiar y auditar</strong><small>0300 depura y seudonimiza; 0310 verifica el desenlace; 0320 detecta multiplicación de filas.</small></div>
    <div class="flow-arrow" aria-hidden="true">↓</div>
    <div class="flow-step"><strong>3 · Construir la unidad analítica</strong><small>0330 consolida una fila por guía y define T0, tiempos observados y SLA en días hábiles.</small></div>
    <div class="flow-step flow-branch"><strong>Rama complementaria · Contexto climático</strong><small>0150 obtiene clima horario; 0160 alinea únicamente información previa a T0.</small></div>
    <div class="flow-arrow" aria-hidden="true">↓</div>
    <div class="flow-step"><strong>4 · Caracterizar y comprobar supuestos</strong><small>0210 explora; 0340, 0345 y 0350 auditan desenlaces, evaluación y variantes de ruta.</small></div>
    <div class="flow-arrow" aria-hidden="true">↓</div>
    <div class="flow-step"><strong>5 · Preparar y validar modelos</strong><small>0411 genera particiones temporales; 0412–0415 entrenan modelos; 0416 decide y ejecuta la prueba final una sola vez.</small></div>
    <div class="flow-arrow" aria-hidden="true">↓</div>
    <div class="flow-step"><strong>6 · Interpretar y analizar el territorio</strong><small>0420 explica predicciones; 0430 asigna zonas; 0440 evalúa autocorrelación espacial de residuos.</small></div>
  </div>
  <div class="notice"><strong>Lectura del resultado.</strong> El objetivo no es solo acertar una predicción: se contrasta el tiempo esperado desde T0 y la probabilidad de superar el SLA, respetando el orden temporal para evitar usar información futura.</div>
  <div class="notice warning"><strong>Exportaciones faltantes.</strong> El inventario recibido no contiene HTML de 0419_ArchivoParticiones ni 0500_SeudonimizacionHMAC. Se muestran en el flujo conceptual, pero no se inventa una explicación celda por celda.</div>
</section>
<section class="section" id="explorer">
  <p class="eyebrow">Explorador de cuadernos</p><h2>Elige una etapa y abre un notebook</h2>
  <p class="lede">Selecciona una etapa en el menú lateral; sus notebooks aparecen como pestañas. Usa el buscador para localizar una celda o cuaderno.</p>
  <div class="explorer-layout">
  <aside class="phase-sidebar"><h3>Etapas principales</h3><nav id="phase-nav" class="phase-nav" aria-label="Etapas principales"></nav></aside>
  <div class="explorer-content">
  <div class="selector-head"><input id="search" class="search" type="search" placeholder="Buscar notebook, celda o código…" aria-label="Buscar notebook, celda o código"><div class="toolbar"><button id="expand-all" type="button">Expandir códigos</button><button id="collapse-all" type="button">Ocultar códigos</button></div></div>
  <nav id="notebook-nav" class="notebook-nav" aria-label="Notebooks de la etapa" role="tablist"></nav>
  <div id="notebook-view" role="tabpanel" aria-live="polite"></div>
  </div>
  </div>
</section>
<footer>Fuentes: exportaciones de NoteBooksHTML y modelos de notebook contenidos en ellas. La explicación es una guía técnica; la validez analítica depende de la ejecución y de las decisiones documentadas.</footer>
</main>
<script id="notebook-data" type="application/json">__DATA_JSON__</script>
<script>
const notebooks=JSON.parse(document.getElementById('notebook-data').textContent);
const phases=[...new Set(notebooks.map(n=>n.phase))];
let activePhase=phases[0],activeNotebook=null,query='';
const phaseNav=document.getElementById('phase-nav'), notebookNav=document.getElementById('notebook-nav'), view=document.getElementById('notebook-view');
function esc(value){return String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function md(value){return esc(value).replace(/^###\s+(.+)$/gm,'<h3>$1</h3>').replace(/^##\s+(.+)$/gm,'<h3>$1</h3>').replace(/\*\*(.+?)\*\*/g,'<strong>$1</strong>').replace(/`([^`]+)`/g,'<code>$1</code>').replace(/\n/g,'<br>');}
function meaningful(code){return code.split(/\r?\n/).filter(line=>line.trim()&&!/^\s*#/.test(line)).join('\n').trim();}
function explain(code,context){
  const body=meaningful(code), lower=body.toLowerCase(), ctx=meaningful(context||'');
  if(!body)return 'La celda contiene comentarios o una referencia de configuración; documenta el propósito o deja un ejemplo sin ejecutar.';
  if(/^\s*%pip\s+install/m.test(body)||/^\s*!pip\s+install/m.test(body))return 'Instala o declara dependencias necesarias para ejecutar el análisis en el clúster. La instalación debe ocurrir antes de importar esas bibliotecas.';
  if(/^\s*(from\s+\S+\s+import|import\s+)/m.test(body)&&!/\n\s*(df|spark|model|X_|y_)\w*\s*=/.test(body))return 'Carga bibliotecas y funciones reutilizables para el resto del cuaderno. Revisa las importaciones para saber qué motor, transformaciones o métricas estarán disponibles.';
  const defs=[...body.matchAll(/^\s*(?:async\s+)?(?:def|class)\s+([A-Za-z_]\w*)/gm)].map(x=>x[1]);
  if(defs.length)return `Define ${defs.map(x=>'“'+x+'”').join(', ')} para encapsular una operación que luego se puede reutilizar. Sigue sus argumentos, validaciones y valor de retorno para entender el contrato de la función.`;
  if(/\b(hmac|sha256|hashlib|seudonim|digest)\b/i.test(body))return 'Protege identificadores mediante una transformación criptográfica o seudónimo. El propósito es permitir vinculaciones analíticas sin exponer directamente el identificador original; la clave/semilla debe gestionarse fuera del código.';
  if(/\b(shap|permutation_importance|feature_importances_|partial_dependence)\b/i.test(body))return 'Cuantifica cómo las variables contribuyen a las predicciones. La importancia describe el comportamiento del modelo, no demuestra causalidad.';
  if(/\b(moran|geopandas|geodataframe|geometry|spatial|espacial|queen|rook)\b/i.test(body))return 'Construye o evalúa relaciones geográficas entre zonas. En el análisis de residuos, esto ayuda a detectar patrones espaciales que el modelo no explicó.';
  if(/\b(lightgbm|xgboost|randomforest|random_forest|gradientboosting|\.fit\s*\()/i.test(body))return 'Configura o ajusta un estimador con los datos preparados. Comprueba qué partición recibe, qué variables usa y que la validación respete el tiempo.';
  if(/\b(roc_auc|average_precision|precision_score|recall_score|mean_absolute_error|root_mean_squared_error|brier_score|log_loss|confusion_matrix)\b/i.test(body))return 'Calcula métricas para evaluar predicciones. Interprétalas según la tarea (tiempo continuo o incumplimiento) y el conjunto temporal evaluado; no son resultados de entrenamiento por sí solos.';
  if(/\b(read_parquet|read_csv|read_json|spark\.read|open\s*\(|load_model|read_table)\b/i.test(body))return 'Lee datos, artefactos o configuraciones de entrada. Esta es la dependencia de datos de la celda; verifica que el origen corresponda a la versión prevista del flujo.';
  if(/\b(to_parquet|to_csv|write\.|save_model|write_table|\.save\s*\()/i.test(body))return 'Persiste un resultado o artefacto para que otra etapa lo consuma. El nombre y la ruta de salida definen el contrato entre notebooks.';
  if(/\b(join|merge|union|concat)\s*\(/i.test(body))return 'Relaciona conjuntos de datos. Revisa la clave, la cardinalidad y el tipo de unión: una unión uno-a-muchos puede multiplicar filas y alterar la unidad de análisis.';
  if(/\b(groupby|group_by|groupBy|agg\s*\(|aggregate\s*\()/i.test(body))return 'Resume observaciones por grupo para producir conteos, estadísticas o variables agregadas. La granularidad del agrupamiento determina qué representa cada fila del resultado.';
  if(/\b(dropna|fillna|drop_duplicates|dropDuplicates|withColumn|with_columns|cast\s*\(|replace\s*\()/i.test(body))return 'Limpia o transforma columnas para estandarizar tipos, valores faltantes y variables derivadas. La transformación debe preservar la definición de una guía como unidad analítica.';
  if(/\b(to_datetime|datetime|timedelta|business.?day|día.?hábil|sla|t0|timezone)\b/i.test(body)||/pandas\.tseries/i.test(body))return 'Prepara fechas, duraciones o plazos. En este proyecto importa distinguir el instante T0, la entrega efectiva y el cómputo del SLA en días hábiles.';
  if(/\b(train_test_split|time.?series.?split|validaci[oó]n|partici[oó]n|cutoff|holdout)\b/i.test(body)||/\b(fecha|periodo|mes)\b.*(?:>=|<=|<|>)/i.test(body))return 'Define o aplica una partición de evaluación. La separación temporal evita que registros futuros informen el entrenamiento de períodos anteriores.';
  if(/\b(assert|raise\s+(?:value|runtime|assertion)error|isna\(\)|duplicated\(\)|nunique\(\))\b/i.test(body))return 'Ejecuta una comprobación de calidad o una condición de integridad. Si falla, debe detener o señalar el flujo antes de que un dato inconsistente llegue a etapas posteriores.';
  if(/\b(plt\.|sns\.|plot\s*\(|hist\s*\(|scatter\s*\(|barplot|heatmap|display\s*\()/i.test(body))return 'Presenta una tabla, métrica o visualización para inspeccionar la distribución o el resultado de la etapa. La salida sirve como diagnóstico, no reemplaza la validación cuantitativa.';
  if(/\b(select|from|where|with\s+\w+\s+as|create\s+table|describe)\b/i.test(body))return 'Ejecuta una consulta o inspección tabular. Identifica las fuentes, filtros y columnas para entender qué población queda representada en el resultado.';
  const first=body.split(/\r?\n/).find(x=>x.trim())||'';
  const clue=first.trim().slice(0,150);
  return `Ejecuta una transformación o comprobación de esta etapa. La instrucción principal comienza con “${clue}”; lee las asignaciones y llamadas posteriores para seguir el dato hasta su resultado.`;
}
function outputLabel(cell){
  if(cell.hasError||cell.state==='error')return 'La exportación registra un error de ejecución; revisa el HTML original en Databricks para el detalle.';
  if(!cell.state||cell.state==='')return 'La exportación no registra estado de ejecución para esta celda.';
  if(cell.state!=='finished')return `Estado registrado: ${cell.state}.`;
  const type=cell.displayType||cell.resultType;
  const names={table:'tabla',chart:'gráfico',html:'contenido visual',image:'imagen',listResults:'resultado de celda',text:'texto'};
  const label=names[type]||type||'salida de ejecución';
  return `Ejecución registrada como completada; tipo de salida: ${label}. Se omiten valores y filas para proteger la información operativa.`;
}
function renderPhases(){
  phaseNav.innerHTML=phases.map((p,i)=>`<button type="button" class="phase-button ${p===activePhase?'active':''}" aria-pressed="${p===activePhase}" aria-current="${p===activePhase?'step':'false'}" data-phase="${esc(p)}">${String(i+1).padStart(2,'0')}. ${esc(p.replace(/^\d+\.\s*/,''))}</button>`).join('');
  phaseNav.querySelectorAll('button').forEach(b=>b.addEventListener('click',()=>{activePhase=b.dataset.phase;activeNotebook=null;render();}));
}
function renderNotebookTabs(){
  const list=notebooks.filter(n=>n.phase===activePhase&&matches(n));
  if(!list.length){activeNotebook=null;notebookNav.innerHTML='';view.removeAttribute('aria-labelledby');view.innerHTML='<div class="empty">No hay notebooks coincidentes en esta etapa. Prueba otra búsqueda o etapa.</div>';return;}
  if(!activeNotebook||!list.some(n=>n.id===activeNotebook))activeNotebook=list[0].id;
  notebookNav.innerHTML=list.map((n,index)=>`<button type="button" id="notebook-tab-${index}" role="tab" aria-controls="notebook-view" class="notebook-button ${n.id===activeNotebook?'active':''}" aria-selected="${n.id===activeNotebook}" aria-pressed="${n.id===activeNotebook}" data-id="${esc(n.id)}">${esc(n.name)}</button>`).join('');
  notebookNav.querySelectorAll('button').forEach(b=>b.addEventListener('click',()=>{activeNotebook=b.dataset.id;render();}));
  notebookNav.querySelectorAll('button').forEach((button,index,buttons)=>button.addEventListener('keydown',event=>{
    let target=index;
    if(event.key==='ArrowRight')target=(index+1)%buttons.length;
    else if(event.key==='ArrowLeft')target=(index-1+buttons.length)%buttons.length;
    else if(event.key==='Home')target=0;
    else if(event.key==='End')target=buttons.length-1;
    else return;
    event.preventDefault();activeNotebook=buttons[target].dataset.id;render();notebookNav.querySelectorAll('button')[target].focus();
  }));
  renderNotebook(list.find(n=>n.id===activeNotebook));
}
function matches(n){
  if(!query)return true;
  const q=query.toLowerCase();
  return n.name.toLowerCase().includes(q)||n.cells.some(c=>c.command.toLowerCase().includes(q));
}
function renderNotebook(n){
  view.setAttribute('aria-labelledby',`notebook-tab-${notebooks.filter(item=>item.phase===activePhase&&matches(item)).findIndex(item=>item.id===n.id)}`);
  let context='',codeNumber=0,body='';
  for(const cell of n.cells){
    if(cell.kind==='markdown'){
      context=cell.command.replace(/^\s*%md\s*/,'').trim();
      if(context)body+=`<div class="md-card">${md(context)}</div>`;
      continue;
    }
    codeNumber++;
    const code=cell.command;
    const contextHtml=context?`<div class="cell-context"><strong>Sección del notebook:</strong><br>${md(context.slice(0,1800))}</div>`:'';
    const error=cell.hasError||cell.state==='error';
    const badge=error?'<span class="badge warn">Revisar error</span>':cell.state==='finished'?'<span class="badge ok">Ejecución registrada</span>':'<span class="badge">Sin estado registrado</span>';
    body+=`<article class="cell-card" data-cell-search="${esc(code.toLowerCase())}">
      <div class="cell-heading"><span class="cell-title">Celda ${codeNumber} · Código</span>${badge}</div>
      <div class="cell-content">${contextHtml}
        <div class="explanation"><strong>¿Qué hace y por qué?</strong>${esc(explain(code,context))}</div>
        <details><summary>Ver código ejecutado</summary><pre><code>${esc(code)}</code></pre></details>
        <div class="output" style="margin-top:12px"><strong>Salida / evidencia disponible</strong>${esc(outputLabel(cell))}</div>
      </div>
    </article>`;
  }
  const codeCells=n.cells.filter(c=>c.kind==='code').length,mdCells=n.cells.filter(c=>c.kind==='markdown').length;
  const versionClass=n.version.startsWith('En el flujo')?'ok':'warn';
  view.innerHTML=`<div class="meta-row"><span class="badge">${esc(n.phase)}</span><span class="badge">${codeCells} celdas de código</span><span class="badge">${mdCells} celdas markdown</span><span class="badge">${esc(n.language||'lenguaje no indicado')}</span><span class="badge ${versionClass}">${esc(n.version)}</span></div><h3>${esc(n.name)}</h3><p class="byline">Exportación revisada: ${esc(n.file)} · Los bloques de código están cerrados inicialmente para facilitar la lectura.</p>${body||'<div class="empty">Este notebook no contiene celdas de código clasificables.</div>'}`;
  view.querySelectorAll('details').forEach(d=>d.open=false);
}
function render(){renderPhases();renderNotebookTabs();}
document.getElementById('search').addEventListener('input',e=>{query=e.target.value.trim();renderNotebookTabs();});
document.getElementById('expand-all').addEventListener('click',()=>view.querySelectorAll('details').forEach(d=>d.open=true));
document.getElementById('collapse-all').addEventListener('click',()=>view.querySelectorAll('details').forEach(d=>d.open=false));
render();
</script>
</body>
</html>
'@

$page = $page.Replace('__TOTAL_NOTEBOOKS__', [string]$notebooks.Count).
    Replace('__CODE_COUNT__', [string]$codeCount).
    Replace('__MARKDOWN_COUNT__', [string]$markdownCount).
    Replace('__DATA_JSON__', $json)
$outputPath = Join-Path $htmlDirectory 'index.html'
[IO.File]::WriteAllText($outputPath, $page, [Text.UTF8Encoding]::new($false))
Write-Output "Presentación creada: $outputPath"
Write-Output ("Exportaciones revisadas: {0}; celdas de código: {1}; celdas markdown: {2}" -f $notebooks.Count, $codeCount, $markdownCount)
