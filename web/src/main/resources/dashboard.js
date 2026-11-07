const state = {
  rows: [],
  selected: [],
  drawRequest: 0,
  replay: false,
  replayTime: null,
  timeline: [],
  replayTimer: null,
  speed: 1,
  chartModel: null,
  symbols: [],
  visibleSymbols: [],
  symbolQuery: "",
};

const colors = ["#50d2a0", "#f4bd63", "#88a9ff", "#f486a8", "#c78cff", "#69d2e7", "#f2f28b", "#ff9566"];
const formatNumber = (value) => Number(value).toFixed(4);
const formatAxisValue = (value, scaleMax) => {
  const decimals = scaleMax < 1 ? 2 : scaleMax < 10 ? 1 : 0;
  return Number(value.toFixed(decimals)).toString();
};
const formatTimestamp = (value) => {
  const date = new Date(value);
  const time = `${date.getHours()}:${String(date.getMinutes()).padStart(2, "0")}`;
  const day = String(date.getDate()).padStart(2, "0");
  const month = String(date.getMonth() + 1).padStart(2, "0");
  return `${time} ${day}-${month}-${date.getFullYear()}`;
};
const element = (id) => document.querySelector(`#${id}`);

function syncSelector(symbols) {
  const select = element("symbols");
  const query = state.symbolQuery.trim().toLowerCase();
  const visibleSymbols = symbols.filter((symbol) => symbol.toLowerCase().includes(query));
  state.visibleSymbols = visibleSymbols;
  const signature = visibleSymbols.join("|");
  if (select.dataset.signature !== signature) {
    select.innerHTML = visibleSymbols.map((symbol) => `<option>${symbol}</option>`).join("");
    select.dataset.signature = signature;
  }
  Array.from(select.options).forEach((option) => {
    option.selected = state.selected.includes(option.value);
  });
  if (!select.dataset.bound) {
    select.onchange = (event) => {
      const visibleSelection = Array.from(event.target.selectedOptions).map((option) => option.value);
      state.selected = [
        ...state.selected.filter((symbol) => !state.visibleSymbols.includes(symbol)),
        ...visibleSelection,
      ];
      draw();
    };
    select.dataset.bound = "true";
  }
  const search = element("symbolSearch");
  if (!search.dataset.bound) {
    search.oninput = (event) => {
      state.symbolQuery = event.target.value;
      syncSelector(state.symbols);
    };
    search.dataset.bound = "true";
  }
}

function updateReplayControls() {
  const timeline = element("replayTimeline");
  element("replayToggle").textContent = state.replay ? "Pause time analysis" : "Play time analysis";
  timeline.disabled = !state.timeline.length;
  timeline.max = Math.max(0, state.timeline.length - 1);
  timeline.value = state.replayTime ? Math.max(0, state.timeline.indexOf(state.replayTime)) : 0;
  element("replayTime").textContent =
    state.replay && state.replayTime ? `Replay | ${formatTimestamp(state.replayTime)}` : "Live mode";
}

function setReplayTime(index) {
  if (!state.timeline.length) return;
  state.replayTime = state.timeline[Math.max(0, Math.min(state.timeline.length - 1, Number(index)))];
  draw();
}

function advanceReplay() {
  if (!state.replay || !state.timeline.length) return;
  const next = state.timeline.indexOf(state.replayTime) + 1;
  if (next >= state.timeline.length) {
    state.replay = false;
    clearInterval(state.replayTimer);
    state.replayTimer = null;
    state.replayTime = null;
  } else {
    state.replayTime = state.timeline[next];
  }
  draw();
}

function toggleReplay() {
  state.replay = !state.replay;
  if (state.replay) {
    state.replayTime = state.timeline[0] || null;
    clearInterval(state.replayTimer);
    state.replayTimer = setInterval(advanceReplay, Math.max(100, 1000 / state.speed));
  } else {
    clearInterval(state.replayTimer);
    state.replayTimer = null;
    state.replayTime = null;
  }
  updateReplayControls();
  draw();
}

function hideChartTooltip() {
  element("chartTooltip").classList.remove("visible");
}

function showChartTooltip(event) {
  const model = state.chartModel;
  const canvas = element("chart");
  if (!model || !model.histories.length) return;
  const bounds = canvas.getBoundingClientRect();
  const canvasX = ((event.clientX - bounds.left) / bounds.width) * canvas.width;
  const canvasY = ((event.clientY - bounds.top) / bounds.height) * canvas.height;
  if (
    canvasX < model.margin.left ||
    canvasX > canvas.width - model.margin.right ||
    canvasY < model.margin.top ||
    canvasY > canvas.height - model.margin.bottom
  ) {
    hideChartTooltip();
    return;
  }
  const index = Math.max(
    0,
    Math.min(
      model.pointCount - 1,
      Math.round(((canvasX - model.margin.left) / model.plotWidth) * (model.pointCount - 1)),
    ),
  );
  const tooltip = element("chartTooltip");
  const rows = model.histories
    .map((history) => ({ history, row: history.byTime.get(model.timeline[index]) }))
    .filter(({ row }) => row)
    .map(
      ({ history, row }) =>
        `<span style="color:${colors[model.histories.indexOf(history) % colors.length]}">${history.symbol} EMA 38 ${formatNumber(row.ema38)}</span><span style="color:${colors[model.histories.indexOf(history) % colors.length]}">${history.symbol} EMA 100 ${formatNumber(row.ema100)}</span>`,
    )
    .join("");
  tooltip.innerHTML = `<strong>${formatTimestamp(model.timeline[index])}</strong>${rows}`;
  tooltip.style.left = `${(canvasX / canvas.width) * bounds.width}px`;
  tooltip.style.top = `${(canvasY / canvas.height) * bounds.height}px`;
  tooltip.classList.remove("flip-left");
  tooltip.classList.add("visible");
  const frameBounds = canvas.parentElement.getBoundingClientRect();
  if (event.clientX + tooltip.offsetWidth + 10 > frameBounds.right) {
    tooltip.classList.add("flip-left");
  }
}

function bindChartHover() {
  const canvas = element("chart");
  if (canvas.dataset.hoverBound) return;
  canvas.addEventListener("pointermove", showChartTooltip);
  canvas.addEventListener("pointerleave", hideChartTooltip);
  canvas.dataset.hoverBound = "true";
}

async function refresh() {
  try {
    const [latestResponse, symbolsResponse] = await Promise.all([
      fetch("/api/trends/latest?limit=250"),
      fetch("/api/symbols"),
    ]);
    if (!latestResponse.ok || !symbolsResponse.ok) throw new Error("database unavailable");
    state.rows = await latestResponse.json();
    const symbols = await symbolsResponse.json();
    state.symbols = symbols;
    state.selected = state.selected.filter((symbol) => symbols.includes(symbol));
    if (!state.selected.length && symbols.length) state.selected = [symbols[0]];

    syncSelector(symbols);
    element("replayToggle").onclick = toggleReplay;
    element("replaySpeed").onchange = (event) => {
      state.speed = Number(event.target.value);
      if (state.replay) {
        clearInterval(state.replayTimer);
        state.replayTimer = setInterval(advanceReplay, Math.max(100, 1000 / state.speed));
      }
    };
    element("replayTimeline").oninput = (event) => setReplayTime(event.target.value);
    element("rows").innerHTML = state.rows
      .slice(0, 100)
      .map(
        (row) => `
      <tr><td>${row.symbol}</td><td>${formatTimestamp(row.windowEnd)}</td><td>${formatNumber(row.lastPrice)}</td>
      <td>${formatNumber(row.ema38)}</td><td>${formatNumber(row.ema100)}</td><td>${formatTimestamp(row.receivedAt)}</td></tr>
    `,
      )
      .join("");
    element("status").textContent = state.replay
      ? `REPLAY | ${state.selected.length} plotted`
      : `LIVE | ${symbols.length} symbols | ${state.selected.length} plotted`;
    element("status").className = "status";
    draw();
  } catch (error) {
    element("status").textContent = "DATABASE OFFLINE";
    element("status").className = "status error";
  }
}

async function draw() {
  const requestId = ++state.drawRequest;
  const selected = state.selected.slice();
  const canvas = element("chart");
  canvas.classList.toggle("replay-chart", state.replay);
  const context = canvas.getContext("2d");
  const margin = { left: 78, right: 24, top: 42, bottom: 62 };
  const plotWidth = canvas.width - margin.left - margin.right;
  const plotHeight = canvas.height - margin.top - margin.bottom;
  context.clearRect(0, 0, canvas.width, canvas.height);
  if (!selected.length) {
    element("quotes").textContent = "Select a stock";
    return;
  }

  const responses = await Promise.all(
    selected.map((symbol) => fetch(`/api/trends/${encodeURIComponent(symbol)}?limit=1000`)),
  );
  if (responses.some((response) => !response.ok)) return;
  const allHistories = await Promise.all(
    responses.map(async (response, index) => ({
      symbol: selected[index],
      data: (await response.json()).filter((row) => !(row.lastPrice === 0 && row.ema38 === 0 && row.ema100 === 0)),
    })),
  );
  if (requestId !== state.drawRequest || selected.join("|") !== state.selected.join("|")) return;

  state.timeline = [...new Set(allHistories.flatMap((history) => history.data.map((row) => row.windowEnd)))].sort(
    (first, second) => Date.parse(first) - Date.parse(second),
  );
  if (state.replay && (!state.replayTime || !state.timeline.includes(state.replayTime))) {
    state.replayTime = state.timeline[0] || null;
  }
  const histories = allHistories.map((history) => ({
    symbol: history.symbol,
    data:
      state.replay && state.replayTime
        ? history.data.filter((row) => Date.parse(row.windowEnd) <= Date.parse(state.replayTime))
        : history.data,
  }));
  updateReplayControls();

  const usable = histories.filter((history) => history.data.length);
  if (!usable.length) return;
  const timeline = [...new Set(usable.flatMap((history) => history.data.map((row) => row.windowEnd)))].sort(
    (first, second) => Date.parse(first) - Date.parse(second),
  );
  const plotted = usable.map((history) => ({
    ...history,
    byTime: new Map(history.data.map((row) => [row.windowEnd, row])),
  }));
  const values = usable.flatMap((history) => history.data.flatMap((row) => [row.ema38, row.ema100]));
  const dataMax = Math.max(...values);
  const min = 0;
  const max = dataMax < 1 ? Math.max(dataMax * 1.08, 0.1) : Math.max(dataMax * 1.08, 1);
  const span = max - min;
  const pointCount = timeline.length;
  const x = (index) => margin.left + (index / Math.max(1, pointCount - 1)) * plotWidth;
  const y = (value) => margin.top + ((max - value) / span) * plotHeight;
  state.chartModel = { histories: plotted, timeline, margin, plotWidth, plotHeight, pointCount };
  bindChartHover();

  context.font = "12px system-ui, sans-serif";
  context.lineWidth = 1;
  context.strokeStyle = "#d7dee6";
  context.fillStyle = "#687582";
  context.textAlign = "right";
  for (let tick = 0; tick <= 4; tick++) {
    const value = max * (1 - tick / 4);
    const yPosition = y(value);
    context.beginPath();
    context.moveTo(margin.left, yPosition);
    context.lineTo(canvas.width - margin.right, yPosition);
    context.stroke();
    context.fillText(formatAxisValue(value, max), margin.left - 10, yPosition + 4);
  }

  context.textAlign = "center";
  const labelSpacing = plotWidth / Math.max(1, pointCount - 1);
  const labelStep = Math.max(1, Math.ceil(64 / labelSpacing));
  for (let index = 0; index < pointCount; index += labelStep) {
    const xPosition = x(index);
    const label = new Date(timeline[index]).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    context.beginPath();
    context.moveTo(xPosition, margin.top);
    context.lineTo(xPosition, canvas.height - margin.bottom);
    context.stroke();
    context.fillText(label, xPosition, canvas.height - margin.bottom + 24);
  }
  if ((pointCount - 1) % labelStep !== 0 && pointCount > 1) {
    const lastIndex = pointCount - 1;
    const xPosition = x(lastIndex);
    const label = new Date(timeline[lastIndex]).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    context.beginPath();
    context.moveTo(xPosition, margin.top);
    context.lineTo(xPosition, canvas.height - margin.bottom);
    context.stroke();
    context.fillText(label, xPosition, canvas.height - margin.bottom + 24);
  }

  const notchStep = pointCount <= 60 ? 1 : Math.ceil(pointCount / 60);
  context.strokeStyle = "#aeb9c4";
  context.lineWidth = 1;
  for (let index = 0; index < pointCount; index += notchStep) {
    const xPosition = x(index);
    context.beginPath();
    context.moveTo(xPosition, canvas.height - margin.bottom);
    context.lineTo(xPosition, canvas.height - margin.bottom + 7);
    context.stroke();
  }

  context.strokeStyle = "#687582";
  context.lineWidth = 1.5;
  context.beginPath();
  context.moveTo(margin.left, margin.top);
  context.lineTo(margin.left, canvas.height - margin.bottom);
  context.lineTo(canvas.width - margin.right, canvas.height - margin.bottom);
  context.stroke();
  context.textAlign = "left";
  context.fillText("EMA value", 8, margin.top - 16);
  context.textAlign = "right";
  context.fillText("Window end (local time)", canvas.width - margin.right, canvas.height - 8);

  plotted.forEach((history, historyIndex) => {
    const color = colors[historyIndex % colors.length];
    const legendX = margin.left + historyIndex * 150;
    context.fillStyle = color;
    context.fillRect(legendX, 18, 18, 3);
    context.textAlign = "left";
    context.fillText(`${history.symbol} EMA`, legendX + 24, 23);
    [
      ["ema38", false],
      ["ema100", true],
    ].forEach(([key, dashed]) => {
      context.strokeStyle = color;
      context.lineWidth = dashed ? 1.5 : 2.5;
      context.setLineDash(dashed ? [7, 5] : []);
      context.beginPath();
      let pathOpen = false;
      timeline.forEach((windowEnd, index) => {
        const row = history.byTime.get(windowEnd);
        if (!row) {
          if (pathOpen) {
            context.stroke();
            context.beginPath();
            pathOpen = false;
          }
          return;
        }
        if (pathOpen) {
          context.lineTo(x(index), y(row[key]));
        } else {
          context.moveTo(x(index), y(row[key]));
          pathOpen = true;
        }
      });
      if (pathOpen) context.stroke();
      context.setLineDash([]);
      timeline.forEach((windowEnd, index) => {
        const row = history.byTime.get(windowEnd);
        if (!row) return;
        context.fillStyle = color;
        context.beginPath();
        context.arc(x(index), y(row[key]), 2.5, 0, 2 * Math.PI);
        context.fill();
      });
    });
  });

  element("quotes").innerHTML = plotted
    .map((history, index) => {
      const latest = history.data[history.data.length - 1];
      return `<div class="quote"><strong style="color:${colors[index % colors.length]}">${history.symbol}</strong>
      <span>Price ${formatNumber(latest.lastPrice)}</span><span>EMA 38 ${formatNumber(latest.ema38)}</span><span>EMA 100 ${formatNumber(latest.ema100)}</span></div>`;
    })
    .join("");
}

refresh();
setInterval(refresh, 2000);
