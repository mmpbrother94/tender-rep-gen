const form = document.getElementById("upload-form");
const submitButton = document.getElementById("submit-button");
const statusPanel = document.getElementById("status");
const resultPanel = document.getElementById("result");
const resultMeta = document.getElementById("result-meta");
const synopsisLink = document.getElementById("synopsis-link");
const bidLink = document.getElementById("bid-link");
const synopsisPreviewBody = document.getElementById("synopsis-preview-body");
const evaluationPreviewBody = document.getElementById("evaluation-preview-body");
const scorePercentage = document.getElementById("score-percentage");
const scoreCategory = document.getElementById("score-category");
const scoreDecision = document.getElementById("score-decision");
const accuracyPercentage = document.getElementById("accuracy-percentage");
const accuracyNote = document.getElementById("accuracy-note");
const coveragePercentage = document.getElementById("coverage-percentage");
const coverageNote = document.getElementById("coverage-note");
const ocrStatus = document.getElementById("ocr-status");
const ocrNote = document.getElementById("ocr-note");
const processingLoader = document.getElementById("processing-loader");
const loaderCopy = document.getElementById("loader-copy");
const previewTabs = document.querySelectorAll(".preview-tab");
const previewPanels = document.querySelectorAll(".preview-panel");
const defaultSubmitLabel = submitButton.textContent;
const activeJobStorageKey = "tender-workbook-active-job";
const maxStartAttempts = 3;
const maxPollNetworkErrors = 12;
const pollDelayMs = 2500;
const retryBaseDelayMs = 2000;
const maxSavedJobAgeMs = 12 * 60 * 60 * 1000;

const processingMessages = [
  "Reading the document structure and identifying the tender sections.",
  "Extracting fields, clause references, and workbook values.",
  "Running OCR on uploaded images and image-based PDF pages when normal text extraction is weak.",
  "Writing the synopsis and bid evaluation workbooks.",
];

let loaderTimer = null;

function showStatus(message, isError = false) {
  statusPanel.textContent = message;
  statusPanel.classList.remove("hidden", "error");
  if (isError) {
    statusPanel.classList.add("error");
  }
}

function renderMeta(payload) {
  resultMeta.innerHTML = "";
  const chips = [
    `Source: ${payload.source_name ?? "Unknown"}`,
    `Employer: ${payload.employer ?? "Not Available"}`,
    `Report Date: ${payload.report_date ?? "Not Available"}`,
  ];

  chips.forEach((label) => {
    const chip = document.createElement("span");
    chip.textContent = label;
    resultMeta.appendChild(chip);
  });
}

function renderInsights(payload) {
  const estimatedAccuracy = Number(payload.estimated_accuracy_percentage ?? 0);
  const coverage = Number(payload.field_coverage_percentage ?? 0);
  accuracyPercentage.textContent = `${estimatedAccuracy.toFixed(2)}%`;
  coveragePercentage.textContent = `${coverage.toFixed(2)}%`;
  accuracyNote.textContent =
    payload.confidence_note ??
    "Estimated from match strength and OCR usage. It is not a guaranteed factual accuracy score.";
  coverageNote.textContent = `${Number(payload.average_field_confidence_percentage ?? 0).toFixed(2)}% average confidence on populated fields.`;
  ocrStatus.textContent = payload.ocr_used ? "Applied" : (payload.ocr_enabled ? "Ready" : "Unavailable");
  ocrNote.textContent = payload.ocr_summary ?? "OCR fallback status not reported.";
}

function renderSynopsis(rows) {
  synopsisPreviewBody.innerHTML = "";
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    [
      row.row_number,
      row.label,
      row.section,
      row.clause,
      row.page,
      row.value,
      row.remark || "",
    ].forEach((value) => {
      const td = document.createElement("td");
      td.textContent = value;
      tr.appendChild(td);
    });
    synopsisPreviewBody.appendChild(tr);
  });
}

function renderEvaluation(rows) {
  evaluationPreviewBody.innerHTML = "";
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    [
      row.point,
      `${Number(row.allocation_percent ?? 0).toFixed(2)}%`,
      `${Number(row.weight_percent ?? 0).toFixed(2)}%`,
      `${Number(row.weighted_percent ?? 0).toFixed(2)}%`,
      row.selected_band,
      row.rationale,
    ].forEach((value) => {
      const td = document.createElement("td");
      td.textContent = value;
      tr.appendChild(td);
    });
    evaluationPreviewBody.appendChild(tr);
  });
}

function renderScore(summary) {
  scorePercentage.textContent = `${Number(summary.total_percentage ?? 0).toFixed(2)}%`;
  scoreCategory.textContent = summary.category ?? "Not Available";
  scoreDecision.textContent = summary.decision ?? "Not Available";
}

function renderGenerationResult(payload) {
  clearActiveJob();
  showStatus(payload.message || "Workbooks generated.");
  renderMeta(payload);
  renderInsights(payload);
  renderSynopsis(payload.synopsis_preview_rows || []);
  renderEvaluation(payload.bid_evaluation_preview_rows || []);
  renderScore(payload.bid_evaluation_summary || {});
  synopsisLink.href = payload.outputs?.synopsis?.download_url || "#";
  synopsisLink.download = payload.outputs?.synopsis?.file_name || "Synopsis.xlsx";
  bidLink.href = payload.outputs?.bid_evaluation?.download_url || "#";
  bidLink.download = payload.outputs?.bid_evaluation?.file_name || "Bid_Evaluation.xlsx";
  activatePanel("synopsis-preview");
  resultPanel.classList.remove("hidden");
}

function activatePanel(panelId) {
  previewPanels.forEach((panel) => {
    panel.classList.toggle("hidden", panel.id !== panelId);
  });
  previewTabs.forEach((button) => {
    button.classList.toggle("active", button.dataset.panel === panelId);
  });
}

function startProcessing(fileName) {
  let messageIndex = 0;
  submitButton.disabled = true;
  submitButton.textContent = "Processing...";
  processingLoader.classList.remove("hidden");
  processingLoader.setAttribute("aria-hidden", "false");
  loaderCopy.textContent = `Starting ${fileName}. ${processingMessages[messageIndex]}`;
  loaderTimer = window.setInterval(() => {
    messageIndex = (messageIndex + 1) % processingMessages.length;
    loaderCopy.textContent = processingMessages[messageIndex];
  }, 2600);
}

function stopProcessing() {
  window.clearInterval(loaderTimer);
  loaderTimer = null;
  processingLoader.classList.add("hidden");
  processingLoader.setAttribute("aria-hidden", "true");
  submitButton.disabled = false;
  submitButton.textContent = defaultSubmitLabel;
}

function delay(milliseconds) {
  return new Promise((resolve) => window.setTimeout(resolve, milliseconds));
}

function createRetryableError(message) {
  const error = new Error(message);
  error.retryable = true;
  return error;
}

function isRetryableError(error) {
  if (error?.retryable) {
    return true;
  }
  if (error instanceof TypeError) {
    return true;
  }
  const message = String(error?.message || "").toLowerCase();
  return message.includes("failed to fetch") || message.includes("network");
}

function buildSubmissionId() {
  if (window.crypto?.randomUUID) {
    return window.crypto.randomUUID();
  }
  return `upload-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function readActiveJob() {
  try {
    const rawValue = window.localStorage.getItem(activeJobStorageKey);
    if (!rawValue) {
      return null;
    }
    const parsed = JSON.parse(rawValue);
    const savedAt = parsed?.savedAt ? Date.parse(parsed.savedAt) : NaN;
    if (Number.isFinite(savedAt) && (Date.now() - savedAt) > maxSavedJobAgeMs) {
      window.localStorage.removeItem(activeJobStorageKey);
      return null;
    }
    return parsed;
  } catch (error) {
    window.localStorage.removeItem(activeJobStorageKey);
    return null;
  }
}

function saveActiveJob(jobId, fileName) {
  window.localStorage.setItem(
    activeJobStorageKey,
    JSON.stringify({
      jobId,
      fileName,
      savedAt: new Date().toISOString(),
    }),
  );
}

function clearActiveJob() {
  window.localStorage.removeItem(activeJobStorageKey);
}

async function readResponsePayload(response) {
  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    return response.json();
  }

  const text = (await response.text()).trim();
  return text ? { detail: text } : {};
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const payload = await readResponsePayload(response);
  return { response, payload };
}

async function startGeneration(formData, requestId, fileName) {
  let lastError = null;

  for (let attempt = 1; attempt <= maxStartAttempts; attempt += 1) {
    try {
      const { response, payload } = await fetchJson("/generate", {
        method: "POST",
        body: formData,
        headers: {
          "X-Upload-Id": requestId,
        },
      });

      if (!response.ok) {
        const message = payload.detail || "Generation failed.";
        if (response.status >= 500) {
          throw createRetryableError(message);
        }
        throw new Error(message);
      }

      return payload;
    } catch (error) {
      lastError = error;
      if (!isRetryableError(error) || attempt === maxStartAttempts) {
        break;
      }

      showStatus(`Connection dropped while uploading ${fileName}. Retrying automatically (${attempt + 1}/${maxStartAttempts}).`);
      await delay(retryBaseDelayMs * attempt);
    }
  }

  throw new Error(
    `Unable to reach the server. Check that the deployment is online and retry. ${lastError?.message || ""}`.trim(),
  );
}

async function pollGeneration(jobId) {
  let consecutiveNetworkErrors = 0;

  while (true) {
    await delay(pollDelayMs);

    try {
      const { response, payload } = await fetchJson(`/jobs/${jobId}`);
      if (!response.ok) {
        if (response.status === 404) {
          clearActiveJob();
          throw new Error("The previous generation job is no longer available on the server. Upload the document again.");
        }
        const message = payload.detail || "Unable to read job status.";
        if (response.status >= 500) {
          throw createRetryableError(message);
        }
        clearActiveJob();
        throw new Error(message);
      }

      consecutiveNetworkErrors = 0;
      if (payload.status === "completed") {
        clearActiveJob();
        return payload;
      }
      if (payload.status === "failed") {
        clearActiveJob();
        throw new Error(payload.detail || "Generation failed.");
      }
      showStatus(payload.message || "Processing is in progress.");
    } catch (error) {
      if (!isRetryableError(error)) {
        throw error;
      }

      consecutiveNetworkErrors += 1;
      if (consecutiveNetworkErrors >= maxPollNetworkErrors) {
        throw new Error(
          "The connection kept dropping during processing. Check the hosting service logs and retry the document once the worker is healthy.",
        );
      }

      showStatus(
        `Connection interrupted while checking progress. Retrying automatically (${consecutiveNetworkErrors}/${maxPollNetworkErrors}).`,
      );
    }
  }
}

async function resumeSavedJob() {
  const activeJob = readActiveJob();
  if (!activeJob?.jobId) {
    return;
  }

  resultPanel.classList.add("hidden");
  startProcessing(activeJob.fileName || "document");
  showStatus(`Resuming processing for ${activeJob.fileName || "the uploaded document"}.`);

  try {
    const payload = await pollGeneration(activeJob.jobId);
    renderGenerationResult(payload);
  } catch (error) {
    showStatus(error.message || "Unable to resume the previous job.", true);
  } finally {
    stopProcessing();
  }
}

previewTabs.forEach((button) => {
  button.addEventListener("click", () => activatePanel(button.dataset.panel));
});

window.addEventListener("load", () => {
  void resumeSavedJob();
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const fileInput = document.getElementById("document");
  if (!fileInput.files.length) {
    showStatus("Choose a tender document first.", true);
    return;
  }

  resultPanel.classList.add("hidden");
  startProcessing(fileInput.files[0].name);
  showStatus("Generating synopsis and bid evaluation workbooks. Large scanned PDFs and image uploads can take longer because OCR may be used.");

  try {
    const formData = new FormData();
    const file = fileInput.files[0];
    const requestId = buildSubmissionId();
    formData.append("document", file);

    clearActiveJob();
    const startPayload = await startGeneration(formData, requestId, file.name);
    if (startPayload.job_id) {
      saveActiveJob(startPayload.job_id, file.name);
    }

    showStatus(startPayload.message || "Upload received. Processing has started.");
    const payload = startPayload.status === "completed"
      ? startPayload
      : await pollGeneration(startPayload.job_id);
    renderGenerationResult(payload);
  } catch (error) {
    showStatus(error.message || "Something went wrong.", true);
  } finally {
    stopProcessing();
  }
});
