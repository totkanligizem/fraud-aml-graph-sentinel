const data = window.__AML_DASHBOARD_DATA__;

if (!data) {
  throw new Error("Dashboard data bundle is missing. Run `make dashboard-build`.");
}

const state = {
  selectedDataset: "overview",
};

const fmtInt = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const fmtPct0 = new Intl.NumberFormat("en-US", { style: "percent", maximumFractionDigits: 0 });
const fmtPct1 = new Intl.NumberFormat("en-US", { style: "percent", minimumFractionDigits: 1, maximumFractionDigits: 1 });
const fmtPct2 = new Intl.NumberFormat("en-US", { style: "percent", minimumFractionDigits: 2, maximumFractionDigits: 2 });
const fmtCompact = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });
const fmtDecimal3 = new Intl.NumberFormat("en-US", { minimumFractionDigits: 3, maximumFractionDigits: 3 });
const fmtMagnitudeCompact = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});

function $(id) {
  return document.getElementById(id);
}

function datasetLabel(datasetId) {
  if (datasetId === "overview") return "Overview";
  return datasetId
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function formatDateShort(isoLike) {
  if (!isoLike) return "-";
  const date = new Date(`${isoLike}T00:00:00Z`);
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" });
}

function formatUtcTimestamp(value) {
  if (!value) return "-";
  const date = new Date(value);
  return date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZone: "UTC",
    hour12: false,
  }) + " UTC";
}

function formatBytes(value) {
  const bytes = Number(value || 0);
  if (bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let size = bytes;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(size >= 10 || unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
}

function getSelectedDatasetRow() {
  if (state.selectedDataset === "overview") return null;
  return data.dataset_breakdown.find((item) => item.dataset_id === state.selectedDataset) || null;
}

function getTrendSeries() {
  return data.daily_series[state.selectedDataset] || data.daily_series.overview || [];
}

function getScoreBuckets() {
  if (state.selectedDataset === "overview") {
    return data.score_buckets.overview || [];
  }
  return data.score_buckets[state.selectedDataset] || [];
}

function getQueueHighlights() {
  if (state.selectedDataset === "overview") {
    return data.queue_highlights.overview || [];
  }
  return data.queue_highlights[state.selectedDataset] || [];
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function renderFilters() {
  const datasets = ["overview", ...data.dataset_breakdown.map((item) => item.dataset_id)];
  $("dataset-switcher").innerHTML = datasets
    .map(
      (datasetId) => `
        <button class="dataset-chip ${datasetId === state.selectedDataset ? "is-active" : ""}" data-dataset="${datasetId}">
          ${escapeHtml(datasetLabel(datasetId))}
        </button>
      `
    )
    .join("");

  $("dataset-switcher").querySelectorAll(".dataset-chip").forEach((button) => {
    button.addEventListener("click", () => {
      state.selectedDataset = button.dataset.dataset;
      render();
    });
  });
}

function renderHero() {
  const quality = data.quality;
  const assurancePct = fmtPct0.format(quality.assurance_ratio);
  const ring = $("assurance-ring");
  ring.style.background = `
    radial-gradient(circle at center, rgba(255, 248, 239, 0.86) 0 43%, transparent 44%),
    conic-gradient(from -90deg, var(--teal-500) 0deg, var(--teal-300) ${quality.assurance_ratio * 360}deg, rgba(170, 101, 53, 0.35) ${quality.assurance_ratio * 360}deg, rgba(170, 101, 53, 0.35) 360deg)
  `;
  $("assurance-value").textContent = assurancePct;
  $("snapshot-time").textContent = formatUtcTimestamp(data.snapshot_generated_at_utc);
  $("region-text").textContent = `${data.project.location} / ${data.project.dataset_id}`;
  $("theme-text").textContent = data.project.theme;
  const statusPill = $("status-pill");
  const hasDefects = (quality.total_defects || 0) > 0;
  const hasUnresolved = (quality.unresolved_checks || 0) > 0;
  const toneClass = hasDefects ? "status-fail" : hasUnresolved ? "status-warn" : "status-ok";
  statusPill.className = `status-pill ${toneClass}`;
  statusPill.textContent = hasDefects
    ? `${quality.failed_checks} failed checks`
    : hasUnresolved
      ? `${quality.passed_checks}/${quality.total_checks} passed · ${quality.unresolved_checks} unresolved`
      : `${quality.passed_checks}/${quality.total_checks} checks passed`;

  $("insight-ribbon").innerHTML = data.insights
    .map((insight) => `<div class="insight-card">${escapeHtml(insight)}</div>`)
    .join("");
}

function renderSignalBand() {
  const analyst = data.analyst || {};
  const refreshItems = [
    `Dashboard ${formatUtcTimestamp(data.freshness.dashboard_generated_at_utc)}`,
    `Checkpoint ${formatUtcTimestamp(data.freshness.checkpoint_snapshot_generated_at_utc)}`,
    `BigQuery ${formatUtcTimestamp(data.freshness.bigquery_validation_generated_at_utc)}`,
  ];
  $("refresh-spine").innerHTML = refreshItems.map((item) => `<span class="signal-pill">${escapeHtml(item)}</span>`).join("");

  const controlItems = [
    `${data.quality.passed_checks}/${data.quality.total_checks} checks zero-defect`,
    data.quality.unresolved_checks > 0 ? `${fmtInt.format(data.quality.unresolved_checks)} unresolved remote checks` : "Remote quality fully resolved",
    `${fmtInt.format(data.kpis.queue_count)} active investigation queues`,
    `Threshold ${fmtDecimal3.format(data.kpis.threshold)} with AP ${fmtDecimal3.format(data.kpis.average_precision)}`,
    `Drift status ${String(data.drift?.status || "n/a").toUpperCase()}`,
  ];
  $("control-posture").innerHTML = controlItems.map((item) => `<span class="signal-pill">${escapeHtml(item)}</span>`).join("");

  const llmItems = [
    analyst.available ? `${analyst.model} in ${analyst.location}` : "Vertex runtime not yet available",
    analyst.available ? `${fmtInt.format(analyst.response_count || 0)} validated responses` : "No validated responses",
    analyst.available ? `Status ${analyst.status}` : "Status not_run",
  ];
  $("llm-surface").innerHTML = llmItems.map((item) => `<span class="signal-pill">${escapeHtml(item)}</span>`).join("");

  const publicationItems = [
    `${fmtInt.format((data.evidence_items || []).length)} evidence artifacts`,
    `${fmtInt.format(data.dataset_breakdown.length)} active datasets`,
    `${fmtInt.format(data.graph.cluster_summary.cluster_count)} graph clusters mirrored`,
  ];
  $("publication-chain").innerHTML = publicationItems
    .map((item) => `<span class="signal-pill">${escapeHtml(item)}</span>`)
    .join("");
}

function renderKpis() {
  const selectedRow = getSelectedDatasetRow();
  const kpis = [
    {
      label: selectedRow ? `${datasetLabel(selectedRow.dataset_id)} rows` : "Total transactions",
      value: selectedRow ? fmtInt.format(selectedRow.transaction_rows) : fmtInt.format(data.kpis.total_transactions),
      foot: selectedRow
        ? `${fmtPct1.format(selectedRow.share_of_volume)} of mart volume`
        : `${fmtInt.format(data.kpis.total_transactions)} validated mart rows`,
      trend: selectedRow ? `${fmtPct1.format(selectedRow.fraud_rate)} fraud rate` : `${fmtPct1.format(data.kpis.scoring_coverage)} scoring coverage`,
    },
    {
      label: selectedRow ? "Scored coverage" : "Scored events",
      value: selectedRow ? fmtPct1.format(selectedRow.scoring_coverage) : fmtInt.format(data.kpis.scored_rows),
      foot: selectedRow
        ? `${fmtInt.format(selectedRow.scored_rows)} scored rows`
        : `${fmtPct1.format(data.kpis.scoring_coverage)} of transaction mart`,
      trend: selectedRow ? `${fmtDecimal3.format(selectedRow.avg_queue_score)} avg queue score` : `${fmtPct1.format(data.kpis.high_risk_share)} high-risk score share`,
    },
    {
      label: selectedRow ? "Fraud or AML labels" : "Investigation queues",
      value: selectedRow
        ? fmtInt.format((selectedRow.fraud_rows || 0) + (selectedRow.aml_rows || 0))
        : fmtInt.format(data.kpis.queue_count),
      foot: selectedRow
        ? `${fmtInt.format(selectedRow.queue_rows)} queued records`
        : `${fmtInt.format(data.ranking.queues_with_positive_labels)} queues contain positives`,
      trend: selectedRow
        ? `${fmtPct2.format(selectedRow.aml_rate)} AML rate`
        : `P@${data.kpis.top_k}: ${fmtPct2.format(data.kpis.mean_precision_at_k)}`,
    },
    {
      label: selectedRow ? "Average amount" : "Graph clusters",
      value: selectedRow ? fmtMagnitudeCompact.format(selectedRow.avg_amount || 0) : fmtInt.format(data.kpis.graph_clusters),
      foot: selectedRow
        ? `${formatDateShort(selectedRow.first_event_date)} to ${formatDateShort(selectedRow.last_event_date)}`
        : `${fmtPct1.format(data.kpis.suspicious_edge_ratio)} suspicious edge share`,
      trend: selectedRow
        ? `${fmtDecimal3.format(selectedRow.max_queue_score)} max queue score`
        : `${fmtInt.format(data.graph.cluster_summary.suspicious_edge_count)} suspicious edges`,
    },
    {
      label: "Validation assurance",
      value: fmtPct0.format(data.kpis.assurance_ratio),
      foot: `${data.quality.passed_checks}/${data.quality.total_checks} checks at zero`,
      trend: `${fmtInt.format(data.quality.total_defects)} open defects`,
    },
    {
      label: "Model threshold",
      value: fmtDecimal3.format(data.kpis.threshold),
      foot: `AP ${fmtDecimal3.format(data.kpis.average_precision)} / PR-AUC ${fmtDecimal3.format(data.kpis.pr_auc_trapz)}`,
      trend: `${datasetLabel(data.kpis.highlighted_dataset.dataset_id)} has the highest queue pressure`,
    },
  ];

  $("kpi-grid").innerHTML = kpis
    .map(
      (item) => `
        <article class="kpi-card">
          <div class="kpi-label">${escapeHtml(item.label)}</div>
          <span class="kpi-value">${escapeHtml(item.value)}</span>
          <div class="kpi-foot">${escapeHtml(item.foot)}</div>
          <div class="kpi-trend">${escapeHtml(item.trend)}</div>
        </article>
      `
    )
    .join("");

  const briefLines = selectedRow
    ? [
        `${datasetLabel(selectedRow.dataset_id)} contributes ${fmtPct1.format(selectedRow.share_of_volume)} of total mart volume.`,
        `Scoring coverage is ${fmtPct1.format(selectedRow.scoring_coverage)} with mean queue score ${fmtDecimal3.format(selectedRow.avg_queue_score)}.`,
        `Current label density sits at ${fmtPct2.format((selectedRow.fraud_rows + selectedRow.aml_rows) / Math.max(selectedRow.transaction_rows, 1))}.`,
      ]
    : [
        `Total scored coverage is ${fmtPct1.format(data.kpis.scoring_coverage)} across ${fmtInt.format(data.kpis.total_transactions)} validated transactions.`,
        `Investigation quality remains stable at P@${data.kpis.top_k} ${fmtPct2.format(data.kpis.mean_precision_at_k)} and NDCG ${fmtPct2.format(data.kpis.mean_ndcg_at_k)}.`,
        `Graph suspicious-edge share is ${fmtPct1.format(data.kpis.suspicious_edge_ratio)} across ${fmtInt.format(data.kpis.graph_clusters)} clusters.`,
      ];

  $("kpi-brief").innerHTML = briefLines
    .map((line) => `<span class="kpi-brief-pill">${escapeHtml(line)}</span>`)
    .join("");
}

function renderCompletion() {
  const cards = [
    {
      label: "Working MVP",
      value: data.project.completion.mvp,
      copy: "End-to-end ingestion, warehouse, scoring, ranking, graph and BigQuery validation are operational.",
      className: "",
    },
    {
      label: "Full vision",
      value: data.project.completion.vision,
      copy: "Dashboard, executive reporting and analyst copilot remain the main delivery frontier.",
      className: "vision",
    },
  ];
  $("completion-stack").innerHTML = cards
    .map(
      (card) => `
        <div class="completion-card">
          <div class="row-top">
            <div>
              <div class="dataset-name">${escapeHtml(card.label)}</div>
              <div class="mini-copy">${escapeHtml(card.copy)}</div>
            </div>
            <div class="queue-value">${card.value}%</div>
          </div>
          <div class="progress-rail">
            <div class="progress-fill ${card.className}" style="width:${card.value}%"></div>
          </div>
        </div>
      `
    )
    .join("");

  $("quality-ledger").innerHTML = [
    { label: "Defect count", value: fmtInt.format(data.quality.total_defects) },
    { label: "Unresolved checks", value: fmtInt.format(data.quality.unresolved_checks || 0) },
    { label: "BigQuery parity", value: data.quality.unresolved_checks > 0 ? "Snapshot fallback" : "Exact" },
    { label: "Report layer", value: "TR + EN PDF" },
  ]
    .map(
      (item) => `
        <div class="metric-row">
          <span class="stat-label">${escapeHtml(item.label)}</span>
          <span class="metric-value">${escapeHtml(item.value)}</span>
        </div>
      `
    )
    .join("");
}

function buildTrendPath(points, xAccessor, yAccessor, width, height, padding) {
  const innerWidth = width - padding.left - padding.right;
  const innerHeight = height - padding.top - padding.bottom;
  const maxX = Math.max(points.length - 1, 1);
  const maxY = Math.max(...points.map(yAccessor), 1);

  return points
    .map((point, index) => {
      const x = padding.left + (xAccessor(point, index) / maxX) * innerWidth;
      const y = padding.top + innerHeight - (yAccessor(point) / maxY) * innerHeight;
      return `${index === 0 ? "M" : "L"}${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
}

function buildAreaPath(points, yAccessor, width, height, padding) {
  const innerWidth = width - padding.left - padding.right;
  const innerHeight = height - padding.top - padding.bottom;
  const maxX = Math.max(points.length - 1, 1);
  const maxY = Math.max(...points.map(yAccessor), 1);
  const top = points
    .map((point, index) => {
      const x = padding.left + (index / maxX) * innerWidth;
      const y = padding.top + innerHeight - (yAccessor(point) / maxY) * innerHeight;
      return `${index === 0 ? "M" : "L"}${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
  const lastX = padding.left + innerWidth;
  const baselineY = padding.top + innerHeight;
  return `${top} L${lastX.toFixed(2)},${baselineY.toFixed(2)} L${padding.left},${baselineY.toFixed(2)} Z`;
}

function renderTrend() {
  const series = getTrendSeries();
  const svg = $("trend-chart");
  const width = 920;
  const height = 360;
  const padding = { top: 24, right: 30, bottom: 42, left: 30 };
  if (!Array.isArray(series) || series.length === 0) {
    svg.innerHTML = "";
    $("trend-note").textContent = "Current lens has no daily trend points.";
    $("trend-summary").innerHTML = "";
    return;
  }
  const maxTxn = Math.max(...series.map((item) => item.txn_count), 1);
  const maxRate = Math.max(...series.map((item) => ((item.fraud_count || 0) / Math.max(item.txn_count, 1)) * 1000), 0.01);
  const maxAml = Math.max(...series.map((item) => item.aml_count || 0), 1);
  const innerWidth = width - padding.left - padding.right;
  const innerHeight = height - padding.top - padding.bottom;
  const overviewLabel = state.selectedDataset === "overview" ? "overview" : datasetLabel(state.selectedDataset);
  $("trend-note").textContent = `Current lens: ${overviewLabel}. Volume area and fraud incidence per 1,000 events.`;

  const grid = Array.from({ length: 5 }, (_, index) => {
    const y = padding.top + (innerHeight / 4) * index;
    return `<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" stroke="rgba(33,62,65,0.10)" stroke-dasharray="4 8" />`;
  }).join("");

  const areaPath = buildAreaPath(series, (item) => item.txn_count, width, height, padding);
  const fraudLine = buildTrendPath(series, (_, index) => index, (item) => ((item.fraud_count || 0) / Math.max(item.txn_count, 1)) * 1000, width, height, padding);
  const labelIndexes = [...new Set([0, Math.floor(series.length / 2), series.length - 1])].sort((a, b) => a - b);
  const labels = labelIndexes
    .map((sourceIndex) => {
      const item = series[sourceIndex];
      const isFirst = sourceIndex === 0;
      const isLast = sourceIndex === series.length - 1;
      const x = isFirst
        ? padding.left + 2
        : isLast
          ? width - padding.right - 2
          : padding.left + (sourceIndex / Math.max(series.length - 1, 1)) * innerWidth;
      const anchor = isFirst ? "start" : isLast ? "end" : "middle";
      return `<text x="${x}" y="${height - 12}" text-anchor="${anchor}" fill="var(--muted)" font-size="12">${formatDateShort(item?.event_date)}</text>`;
    })
    .join("");

  const amlPoints = series
    .filter((item) => (item.aml_count || 0) > 0)
    .map((item, index) => {
      const seriesIndex = series.indexOf(item);
      const x = padding.left + (seriesIndex / Math.max(series.length - 1, 1)) * innerWidth;
      const y = padding.top + innerHeight - ((item.aml_count || 0) / maxAml) * innerHeight;
      return `<circle cx="${x}" cy="${y}" r="5" fill="var(--olive-500)" opacity="0.92"></circle>`;
    })
    .join("");

  svg.innerHTML = `
    <defs>
      <linearGradient id="txnArea" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="rgba(31,127,120,0.50)" />
        <stop offset="100%" stop-color="rgba(31,127,120,0.04)" />
      </linearGradient>
    </defs>
    ${grid}
    <path d="${areaPath}" fill="url(#txnArea)"></path>
    <path d="${buildTrendPath(series, (_, index) => index, (item) => item.txn_count, width, height, padding)}" fill="none" stroke="var(--teal-500)" stroke-width="3.5" stroke-linecap="round"></path>
    <path d="${fraudLine}" fill="none" stroke="var(--copper-600)" stroke-width="3" stroke-linecap="round"></path>
    ${amlPoints}
    ${labels}
  `;

  const totals = series.reduce(
    (acc, item) => {
      acc.txn += item.txn_count || 0;
      acc.fraud += item.fraud_count || 0;
      acc.aml += item.aml_count || 0;
      acc.amount += item.avg_amount || 0;
      return acc;
    },
    { txn: 0, fraud: 0, aml: 0, amount: 0 }
  );
  const avgAmount = series.length ? totals.amount / series.length : 0;
  const fraudRate = totals.fraud / Math.max(totals.txn, 1);
  const amlRate = totals.aml / Math.max(totals.txn, 1);

  $("trend-summary").innerHTML = [
    { label: "Days in lens", value: fmtInt.format(series.length), copy: `${formatDateShort(series[0]?.event_date)} to ${formatDateShort(series[series.length - 1]?.event_date)}` },
    { label: "Events in view", value: fmtCompact.format(totals.txn), copy: `${fmtInt.format(totals.txn)} cumulative transactions` },
    { label: "Fraud incidence", value: fmtPct2.format(fraudRate), copy: `${fmtInt.format(totals.fraud)} labeled fraud events` },
    { label: "AML incidence", value: fmtPct2.format(amlRate), copy: `${fmtInt.format(totals.aml)} labeled AML events / avg amount ${fmtMagnitudeCompact.format(avgAmount)}` },
  ]
    .map(
      (item) => `
        <div class="mini-stat">
          <div class="mini-label">${escapeHtml(item.label)}</div>
          <span class="mini-value">${escapeHtml(item.value)}</span>
          <div class="mini-copy">${escapeHtml(item.copy)}</div>
        </div>
      `
    )
    .join("");
}

function renderQuality() {
  const groups = [
    { title: "Core contract", rows: data.quality.core },
    { title: "Graph contract", rows: data.quality.graph },
  ];
  $("metric-checklist").innerHTML = groups
    .map(
      (group) => `
        <div class="metric-group">
          <div class="dataset-name">${escapeHtml(group.title)}</div>
          ${group.rows
            .map(
              (row) => `
                <div class="metric-row">
                  <span class="stat-label">${escapeHtml(row.name)}</span>
                  <span class="metric-value ${row.status === "failed" ? "metric-failed" : row.status === "unresolved" ? "metric-unresolved" : "metric-passed"}">
                    ${escapeHtml(row.status === "unresolved" ? "pending" : String(row.value))}
                  </span>
                </div>
              `
            )
            .join("")}
        </div>
      `
    )
    .join("");
}

function renderDatasetTable() {
  $("dataset-table").innerHTML = data.dataset_breakdown
    .map((row) => {
      const active = state.selectedDataset === row.dataset_id;
      const labelRate = (row.fraud_rows + row.aml_rows) / Math.max(row.transaction_rows, 1);
      return `
        <article class="dataset-row ${active ? "is-active" : ""}">
          <div class="row-top">
            <div>
              <div class="dataset-name">${escapeHtml(datasetLabel(row.dataset_id))}</div>
              <div class="dataset-meta">${fmtInt.format(row.transaction_rows)} rows from ${formatDateShort(row.first_event_date)} to ${formatDateShort(row.last_event_date)}</div>
            </div>
            <span class="dataset-badge">${fmtPct1.format(row.share_of_volume)} of mart</span>
          </div>
          <div class="row-grid">
            <div class="row-metric">
              <span class="stat-label">Label density</span>
              <span class="row-value">${fmtPct2.format(labelRate)}</span>
            </div>
            <div class="row-metric">
              <span class="stat-label">Fraud / AML</span>
              <span class="row-value">${fmtInt.format(row.fraud_rows)} / ${fmtInt.format(row.aml_rows)}</span>
            </div>
            <div class="row-metric">
              <span class="stat-label">Avg amount</span>
              <span class="row-value">${fmtMagnitudeCompact.format(row.avg_amount || 0)}</span>
            </div>
          </div>
          <div class="bar-shell"><div class="bar-fill teal" style="width:${row.scoring_coverage * 100}%"></div></div>
          <div class="queue-foot">
            <span>Scored coverage ${fmtPct1.format(row.scoring_coverage)}</span>
            <span>Mean queue score ${fmtDecimal3.format(row.avg_queue_score)}</span>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderBuckets() {
  const buckets = getScoreBuckets();
  if (!buckets.length) {
    $("bucket-bars").innerHTML = `
      <div class="bucket-item">
        <div class="bucket-top">
          <span class="bucket-label">No scored records</span>
          <span class="metric-value">0</span>
        </div>
        <div class="queue-foot">
          <span>This lens is monitored in the warehouse but not passed through the current fraud scoring model.</span>
        </div>
      </div>
    `;
    $("queue-highlights").innerHTML = `
      <article class="queue-item">
        <div class="queue-top">
          <div>
            <div class="queue-name">Queue not generated</div>
            <div class="queue-copy">The selected lens currently has no fraud scoring and therefore no alert queue surface.</div>
          </div>
          <span class="queue-tag">empty</span>
        </div>
      </article>
    `;
    return;
  }
  const maxCount = Math.max(...buckets.map((item) => item.row_count), 1);
  $("bucket-bars").innerHTML = buckets
    .map((item, index) => {
      const palette = ["teal", "copper", "olive", "rose", "teal"][index % 5];
      return `
        <div class="bucket-item">
          <div class="bucket-top">
            <span class="bucket-label">${escapeHtml(item.bucket)}</span>
            <span class="metric-value">${fmtInt.format(item.row_count)}</span>
          </div>
          <div class="bar-shell"><div class="bar-fill ${palette}" style="width:${(item.row_count / maxCount) * 100}%"></div></div>
          <div class="queue-foot">
            <span>${fmtPct1.format(item.row_count / Math.max((getSelectedDatasetRow()?.scored_rows) || data.kpis.scored_rows, 1))} of scored events</span>
            <span>distribution band</span>
          </div>
        </div>
      `;
    })
    .join("");

  const highlights = [...getQueueHighlights()].sort(
    (left, right) => (right.avg_score - left.avg_score) || (right.positive_rows - left.positive_rows)
  );
  const visibleHighlights = highlights.slice(0, 4);
  const highlightsHtml = visibleHighlights
    .map(
      (item) => `
        <article class="queue-item">
          <div class="queue-top">
            <div>
              <div class="queue-name">${escapeHtml(item.queue_id)}</div>
              <div class="queue-copy">${formatDateShort(item.event_date)} / ${fmtInt.format(item.queue_rows)} events</div>
            </div>
            <span class="queue-tag">Top queue</span>
          </div>
          <div class="queue-grid">
            <div class="queue-metric">
              <span class="stat-label">Avg score</span>
              <span class="row-value">${fmtDecimal3.format(item.avg_score)}</span>
            </div>
            <div class="queue-metric">
              <span class="stat-label">Max score</span>
              <span class="row-value">${fmtDecimal3.format(item.max_score)}</span>
            </div>
            <div class="queue-metric">
              <span class="stat-label">Positive labels</span>
              <span class="row-value">${fmtInt.format(item.positive_rows)}</span>
            </div>
          </div>
        </article>
      `
    )
    .join("");
  const overflowNote =
    highlights.length > visibleHighlights.length
      ? `
        <div class="queue-foot queue-foot-caption">
          <span>Showing top ${fmtInt.format(visibleHighlights.length)} of ${fmtInt.format(highlights.length)} queues in this lens.</span>
        </div>
      `
      : "";
  $("queue-highlights").innerHTML = `${highlightsHtml}${overflowNote}`;
}

function renderRanking() {
  const topK = data.ranking.top_k;
  const estimatedSurface = Math.round(data.ranking.mean_precision_at_k * data.ranking.queue_count * topK);
  $("ranking-stats").innerHTML = [
    { label: `Mean P@${topK}`, value: fmtPct2.format(data.ranking.mean_precision_at_k), copy: "Average precision at investigation cutoff" },
    { label: `Mean NDCG@${topK}`, value: fmtPct2.format(data.ranking.mean_ndcg_at_k), copy: "Ranking order quality across queues" },
    { label: "Positive queues", value: fmtInt.format(data.ranking.queues_with_positive_labels), copy: `${fmtInt.format(data.ranking.queue_count)} total daily queues` },
    { label: "Top-k surfaced labels", value: fmtInt.format(estimatedSurface), copy: "Approximate positives exposed by the current cutoff" },
  ]
    .map(
      (item) => `
        <article class="stat-card">
          <div class="stat-label">${escapeHtml(item.label)}</div>
          <span class="stat-value">${escapeHtml(item.value)}</span>
          <div class="metric-copy">${escapeHtml(item.copy)}</div>
        </article>
      `
    )
    .join("");
}

function renderDrift() {
  const drift = data.drift || {};
  if (!drift.available) {
    $("drift-stats").innerHTML = `
      <article class="stat-card">
        <div class="stat-label">Status</div>
        <span class="stat-value">n/a</span>
        <div class="metric-copy">${escapeHtml(drift.note || "Not enough scored history for drift metrics.")}</div>
      </article>
    `;
    return;
  }

  const windowLabel = `${formatDateShort(drift.current_window?.start_date)} to ${formatDateShort(drift.current_window?.end_date)}`;
  const queueJaccard = drift.queue_jaccard_top20 == null ? "n/a" : fmtPct2.format(drift.queue_jaccard_top20);
  $("drift-stats").innerHTML = [
    {
      label: "Status",
      value: String(drift.status || "unknown").toUpperCase(),
      copy: "Combined PSI/KS and queue-overlap signal.",
    },
    {
      label: "Score PSI",
      value: fmtDecimal3.format(drift.score_psi || 0),
      copy: "Population Stability Index over score buckets.",
    },
    {
      label: "Score KS",
      value: fmtDecimal3.format(drift.score_ks || 0),
      copy: "Cumulative distribution shift across score buckets.",
    },
    {
      label: "Top-20 queue Jaccard",
      value: queueJaccard,
      copy: `Current window ${windowLabel}`,
    },
  ]
    .map(
      (item) => `
        <article class="stat-card">
          <div class="stat-label">${escapeHtml(item.label)}</div>
          <span class="stat-value">${escapeHtml(item.value)}</span>
          <div class="metric-copy">${escapeHtml(item.copy)}</div>
        </article>
      `
    )
    .join("");
}

function renderPipeline() {
  $("pipeline-list").innerHTML = data.pipeline_steps
    .map(
      (step, index) => `
        <article class="pipeline-item">
          <div class="pipeline-index">${index + 1}</div>
          <div>
            <div class="dataset-name">${escapeHtml(step.label)}</div>
            <div class="mini-copy">${fmtInt.format(step.value)} current records or groups</div>
          </div>
          <div class="pipeline-status">${escapeHtml(step.status)}</div>
        </article>
      `
    )
    .join("");
}

function renderFeatures() {
  const topFeatureMarkup = data.top_features
    .map((feature, index) => {
      const width = Math.max(Math.abs(feature.weight) * 100, 6);
      const palette = feature.weight >= 0 ? "teal" : "copper";
      return `
        <div class="feature-item">
          <div class="row-top">
            <div>
              <div class="dataset-name">${escapeHtml(feature.feature)}</div>
              <div class="mini-copy">Weight direction ${feature.weight >= 0 ? "raises" : "suppresses"} fraud score</div>
            </div>
            <div class="feature-weight">${feature.weight >= 0 ? "+" : ""}${feature.weight.toFixed(4)}</div>
          </div>
          <div class="bar-shell"><div class="bar-fill ${palette}" style="width:${Math.min(width, 100)}%"></div></div>
        </div>
      `;
    })
    .join("");

  $("feature-list").innerHTML = `
    <div class="stat-card">
      <div class="stat-label">Average precision</div>
      <span class="stat-value">${fmtDecimal3.format(data.model.metrics.average_precision)}</span>
      <div class="metric-copy">Cost-optimized threshold ${fmtDecimal3.format(data.model.cost_optimized_threshold.threshold)}</div>
    </div>
    ${topFeatureMarkup}
  `;
}

function renderClusters() {
  const maxAmount = Math.max(...data.graph_panels.top_clusters.map((item) => item.amount_sum), 1);
  $("cluster-list").innerHTML = data.graph_panels.top_clusters
    .map(
      (item) => `
        <article class="cluster-item">
          <div class="cluster-top">
            <div>
              <div class="cluster-name">${escapeHtml(item.cluster_id)}</div>
              <div class="mini-copy">${fmtInt.format(item.party_count)} parties / ${fmtInt.format(item.edge_count)} edges</div>
            </div>
            <span class="metric-pill">risk ${fmtDecimal3.format(item.max_edge_risk_score)}</span>
          </div>
          <div class="bar-shell"><div class="bar-fill copper" style="width:${(item.amount_sum / maxAmount) * 100}%"></div></div>
          <div class="queue-foot">
            <span>${fmtInt.format(item.txn_count)} transactions</span>
            <span>${fmtMagnitudeCompact.format(item.amount_sum)}</span>
          </div>
        </article>
      `
    )
    .join("");
}

function renderNodes() {
  const maxRisk = Math.max(...data.graph_panels.top_nodes.map((item) => item.risk_score), 1);
  $("node-list").innerHTML = data.graph_panels.top_nodes
    .map(
      (item) => `
        <article class="node-item">
          <div class="node-top">
            <div>
              <div class="node-name">${escapeHtml(item.party_id)}</div>
              <div class="mini-copy">${fmtInt.format(item.total_txn_count)} transfers / ${fmtInt.format(item.distinct_counterparty_count)} counterparties</div>
            </div>
            <span class="metric-pill">risk ${fmtDecimal3.format(item.risk_score)}</span>
          </div>
          <div class="bar-shell"><div class="bar-fill teal" style="width:${(item.risk_score / maxRisk) * 100}%"></div></div>
          <div class="node-grid">
            <div class="node-metric">
              <span class="stat-label">Amount</span>
              <span class="node-value">${fmtMagnitudeCompact.format(item.total_amount_sum)}</span>
            </div>
            <div class="node-metric">
              <span class="stat-label">High-risk events</span>
              <span class="node-value">${fmtInt.format(item.high_risk_event_count)}</span>
            </div>
            <div class="node-metric">
              <span class="stat-label">Risk score</span>
              <span class="node-value">${fmtDecimal3.format(item.risk_score)}</span>
            </div>
          </div>
        </article>
      `
    )
    .join("");
}

function renderAnalyst() {
  const analyst = data.analyst || {};
  const fallbackCount = Array.isArray(analyst.responses)
    ? analyst.responses.filter((item) => String(item.model || "").includes("deterministic_fallback")).length
    : 0;
  $("analyst-note").textContent =
    (analyst.note || "Vertex analyst output is not available.")
    + (fallbackCount > 0 ? ` ${fmtInt.format(fallbackCount)} case(s) used deterministic fallback for schema-safe continuity.` : "");

  const metaItems = [
    { label: "Model", value: analyst.model || "not run" },
    { label: "Region", value: analyst.location || data.project.location },
    { label: "Responses", value: fmtInt.format(analyst.response_count || 0) },
    { label: "Errors", value: fmtInt.format(analyst.error_count || 0) },
    { label: "Fallback", value: fmtInt.format(fallbackCount) },
  ];

  $("analyst-meta").innerHTML = metaItems
    .map(
      (item) => `
        <article class="analyst-meta-card">
          <div class="stat-label">${escapeHtml(item.label)}</div>
          <div class="analyst-meta-value">${escapeHtml(item.value)}</div>
        </article>
      `
    )
    .join("");

  if (!analyst.available || !Array.isArray(analyst.responses) || analyst.responses.length === 0) {
    $("analyst-cases").innerHTML = `
      <article class="analyst-case analyst-empty">
        <div class="dataset-name">No validated analyst output</div>
        <div class="metric-copy">Run \`make agent-vertex-validate\` to publish the latest Gemini case summary into the dashboard.</div>
      </article>
    `;
    return;
  }

  $("analyst-cases").innerHTML = analyst.responses
    .map(
      (item) => `
        <article class="analyst-case">
          <div class="analyst-case-top">
            <div>
              <div class="dataset-name">${escapeHtml(datasetLabel(item.dataset_id))}</div>
              <div class="queue-copy">${escapeHtml(item.queue_id)}</div>
            </div>
            <div class="analyst-pill-row">
              <span class="metric-pill">${escapeHtml(item.model || analyst.model || "model n/a")}</span>
              <span class="metric-pill priority-${escapeHtml(String(item.overall_priority || "").toLowerCase())}">${escapeHtml(item.overall_priority || "unknown")} priority</span>
              <span class="metric-pill">${fmtInt.format(item.response_chars || 0)} chars</span>
            </div>
          </div>
          <p class="analyst-overview">${escapeHtml(item.case_overview || "-")}</p>
          <div class="analyst-columns">
            <div class="analyst-column">
              <div class="stat-label">Observed signals</div>
              <ul class="analyst-list">
                ${(item.observed_signals || []).map((signal) => `<li>${escapeHtml(signal)}</li>`).join("")}
              </ul>
            </div>
            <div class="analyst-column">
              <div class="stat-label">Recommended actions</div>
              <ul class="analyst-list">
                ${(item.recommended_actions || []).map((action) => `<li>${escapeHtml(action)}</li>`).join("")}
              </ul>
            </div>
          </div>
        </article>
      `
    )
    .join("");
}

function renderEvidence() {
  const items = data.evidence_items || (data.evidence_paths || []).map((path) => ({
    path,
    label: path.split("/").slice(-1)[0],
    kind: "artifact",
    exists: true,
    size_bytes: 0,
    modified_at_utc: null,
  }));

  $("evidence-grid").innerHTML = items
    .map(
      (item) => `
        <article class="evidence-item">
          <div class="evidence-top">
            <div>
              <div class="evidence-name">${escapeHtml(item.label)}</div>
              <div class="evidence-copy">Versioned artifact backing the current dashboard state.</div>
            </div>
            <span class="evidence-tag">${escapeHtml(item.kind)}</span>
          </div>
          <div class="evidence-metadata">
            <span class="metric-pill">${escapeHtml(formatBytes(item.size_bytes))}</span>
            <span class="metric-pill">${escapeHtml(item.modified_at_utc ? formatUtcTimestamp(item.modified_at_utc) : "timestamp unavailable")}</span>
          </div>
          <span class="evidence-path">${escapeHtml(item.path)}</span>
        </article>
      `
    )
    .join("");
}

function render() {
  renderFilters();
  renderHero();
  renderSignalBand();
  renderKpis();
  renderCompletion();
  renderTrend();
  renderQuality();
  renderDatasetTable();
  renderBuckets();
  renderRanking();
  renderDrift();
  renderPipeline();
  renderFeatures();
  renderClusters();
  renderNodes();
  renderAnalyst();
  renderEvidence();
}

render();
