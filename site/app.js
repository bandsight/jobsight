const jobsEl = document.querySelector("#jobs");
const statsEl = document.querySelector("#stats");
const searchEl = document.querySelector("#search");
const statusEl = document.querySelector("#status");
const template = document.querySelector("#job-template");

let payload = { summary: {}, jobs: [] };

function normalise(value) {
  return String(value || "").toLowerCase();
}

function formatNumber(value) {
  return Number(value || 0).toLocaleString("en-AU");
}

function salaryText(job) {
  if (job.advertised_salary_text) return job.advertised_salary_text;
  const min = job.advertised_salary_min;
  const max = job.advertised_salary_max;
  if (!min && !max) return "";
  const range = min === max || !max ? `$${formatNumber(min || max)}` : `$${formatNumber(min)}-${formatNumber(max)}`;
  return job.advertised_salary_period ? `${range} per ${job.advertised_salary_period}` : range;
}

function renderStats(jobs) {
  const summary = payload.summary || {};
  const parts = [
    `${formatNumber(jobs.length)} shown`,
    `${formatNumber(summary.current_jobs)} latest known`,
    `${formatNumber(summary.observations)} observations`,
    `${formatNumber(summary.sources_configured)} sources`,
    `${formatNumber(summary.councils_with_current_jobs)} councils`,
  ];
  statsEl.innerHTML = "";
  for (const part of parts) {
    const node = document.createElement("span");
    node.className = "stat";
    node.textContent = part;
    statsEl.append(node);
  }
}

function matches(job) {
  const query = normalise(searchEl.value).trim();
  const status = statusEl.value;
  if (status && job.classification_status !== status) return false;
  if (!query) return true;
  const haystack = normalise([
    job.title,
    job.council_name,
    job.short_name,
    job.classification_status,
    job.advertised_salary_text,
    job.location_text,
    job.description_excerpt,
  ].join(" "));
  return query.split(/\s+/).every((token) => haystack.includes(token));
}

function render() {
  const jobs = payload.jobs.filter(matches);
  renderStats(jobs);
  jobsEl.innerHTML = "";
  for (const job of jobs) {
    const node = template.content.firstElementChild.cloneNode(true);
    node.querySelector("h2").textContent = job.title || "Untitled role";
    const band = job.band ? `Band ${job.band}` : job.classification_status || "unclassified";
    node.querySelector(".meta").textContent = `${job.short_name || job.council_name} - ${band}${job.closing_date ? ` - closes ${job.closing_date}` : ""}`;
    node.querySelector(".salary").textContent = salaryText(job);
    const excerpt = node.querySelector(".excerpt");
    excerpt.textContent = job.description_excerpt || "";
    excerpt.hidden = !job.description_excerpt;
    const link = node.querySelector(".open");
    link.href = job.url;
    jobsEl.append(node);
  }
}

async function init() {
  if (window.JOBSIGHT_DATA) {
    payload = window.JOBSIGHT_DATA;
  } else if (window.fetch) {
    const response = await fetch("./data/current-jobs.json", { cache: "no-store" });
    payload = await response.json();
  } else {
    throw new Error("No embedded data or fetch support");
  }
  render();
}

searchEl.addEventListener("input", render);
statusEl.addEventListener("change", render);
init().catch((error) => {
  jobsEl.textContent = `Could not load job data: ${error.message}`;
});
