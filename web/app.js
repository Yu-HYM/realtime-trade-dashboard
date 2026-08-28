/* =========================================================
   电商实时交易大屏 · 前端逻辑（真实数据版）
   - 数据来源：FastAPI /api/realtime/*（背后为 Redis 实时指标，
     链路：Kafka → Spark Structured Streaming → Redis → FastAPI）
   - ECharts 折线图（各品类 GMV 趋势）+ 柱状图（最新 5 分钟窗口 GMV）
   - KPI 数字滚动动画 / 订单增量事件流 / 品类成交榜
   - 时间范围切换（15M / 30M / 1H / ALL，按窗口数截取）
   ========================================================= */
(function () {
  "use strict";

  /* 前后端分离：大屏跑在 8502，API 跑在同主机 8000，自动拼接基址 */
  const API =
    window.location.protocol + "//" + window.location.hostname + ":8000/api/realtime";

  /* ---------- 色板 ---------- */
  const C = {
    cyan: "#23f0ff",
    purple: "#7b61ff",
    amber: "#ffb020",
    green: "#45ff8b",
    red: "#ff5b6c",
    text: "#e7eaef",
    sub: "#9098a8",
    border: "#1f2230",
    surface: "#101218",
  };

  /* 品类配色（与 Redis rt:gmv:cat:* 品类对齐，未知品类自动分配） */
  const colorMap = new Map([
    ["手机数码", C.cyan],
    ["家用电器", C.purple],
    ["服饰鞋包", C.amber],
    ["食品生鲜", C.green],
    ["美妆个护", C.red],
  ]);
  const FALLBACK = [C.cyan, C.purple, C.amber, C.green, C.red];
  const colorOf = (name) => {
    if (!colorMap.has(name)) {
      colorMap.set(name, FALLBACK[colorMap.size % FALLBACK.length]);
    }
    return colorMap.get(name);
  };

  /* ---------- 工具函数 ---------- */
  const fmtInt = (n) => Math.round(n).toLocaleString("en-US");
  const fmtMoney = (n) =>
    "¥" + n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const fmtCompact = (n) =>
    n == null ? "-" :
    n >= 1e8 ? (n / 1e8).toFixed(2) + "亿" :
    n >= 1e4 ? (n / 1e4).toFixed(1) + "万" : fmtInt(n);
  const pad = (n) => String(n).padStart(2, "0");
  const fmtKpi = (v, d) =>
    d.prefix + v.toLocaleString("en-US", {
      minimumFractionDigits: d.decimals, maximumFractionDigits: d.decimals,
    });

  async function fetchJSON(url) {
    const r = await fetch(url, { cache: "no-store" });
    if (!r.ok) throw new Error("HTTP " + r.status);
    return r.json();
  }

  /* =======================================================
     KPI（数据源：/api/realtime/overview）
     ======================================================= */
  const KPI_DEFS = [
    { key: "gmv",    label: "实时 GMV",   color: C.cyan,   prefix: "¥", decimals: 2, sub: "今日累计成交额" },
    { key: "orders", label: "实时订单数", color: C.purple, prefix: "",  decimals: 0, sub: "去重订单总数" },
    { key: "dau",    label: "实时 DAU",   color: C.green,  prefix: "",  decimals: 0, sub: "活跃用户数（set 去重）" },
    { key: "aov",    label: "笔单价",     color: C.amber,  prefix: "¥", decimals: 1, sub: "GMV ÷ 订单数" },
  ];
  const kpiState = { gmv: null, orders: null, dau: null, aov: null };
  const kpiTrend = { gmv: null, orders: null, dau: null, aov: null };

  function buildKpi() {
    const root = document.getElementById("kpiRow");
    root.innerHTML = "";
    KPI_DEFS.forEach((d) => {
      const el = document.createElement("div");
      el.className = "kpi-card";
      el.style.setProperty("--kc", d.color);
      el.innerHTML =
        '<div class="kpi-top">' +
        '<span class="kpi-label">' + d.label + "</span>" +
        '<span class="kpi-trend">—</span>' +
        "</div>" +
        '<div class="kpi-value" id="kpi-' + d.key + '">—</div>' +
        '<div class="kpi-sub">' + d.sub + "</div>";
      root.appendChild(el);
    });
  }

  function renderKpi(instant) {
    KPI_DEFS.forEach((d) => {
      const el = document.getElementById("kpi-" + d.key);
      const badge = el.closest(".kpi-card").querySelector(".kpi-trend");
      const target = kpiState[d.key];

      /* 涨跌徽标：较上次轮询的真实变化 */
      const t = kpiTrend[d.key];
      if (t == null) {
        badge.textContent = "—";
        badge.className = "kpi-trend";
      } else {
        const up = t >= 0;
        badge.className = "kpi-trend " + (up ? "up" : "down");
        badge.textContent = (up ? "▲ " : "▼ ") + Math.abs(t).toFixed(2) + "%";
      }

      if (target == null) { el.textContent = "—"; return; }
      if (instant) {
        el.textContent = fmtKpi(target, d);
        el.dataset.cur = target;
        return;
      }
      const start = el.dataset.cur != null ? parseFloat(el.dataset.cur) : 0;
      animateValue(el, start, target, 700, d.prefix, d.decimals);
      el.dataset.cur = target;
    });
  }

  function animateValue(el, from, to, dur, prefix, decimals) {
    const t0 = performance.now();
    function step(now) {
      const p = Math.min(1, (now - t0) / dur);
      const eased = 1 - Math.pow(1 - p, 3);
      const v = from + (to - from) * eased;
      el.textContent = prefix + v.toLocaleString("en-US", {
        minimumFractionDigits: decimals, maximumFractionDigits: decimals,
      });
      if (p < 1) requestAnimationFrame(step);
    }
    requestAnimationFrame(step);
  }

  const calcTrend = (prev, curr) => {
    if (prev == null || prev === 0 || curr == null) return null;
    return ((curr - prev) / prev) * 100;
  };

  /* =======================================================
     折线图：各品类 GMV 趋势（数据源：/api/realtime/trend）
     ======================================================= */
  let lineChart, barChart;
  let rawTrend = { times: [], series: {} };   // series: {品类: [窗口 GMV]}
  let liveCats = [];                          // 当前有数据的品类

  const RANGE_POINTS = { "15M": 15, "30M": 30, "1H": 60, "ALL": Infinity };
  let currentRange = "1H";

  function buildLineOption(xs, cats) {
    const n = xs.length;
    return {
      backgroundColor: "transparent",
      grid: { left: 48, right: 16, top: 36, bottom: 28 },
      tooltip: {
        trigger: "axis",
        backgroundColor: C.surface,
        borderColor: C.border,
        textStyle: { color: C.text },
        valueFormatter: (v) => (v == null ? "-" : "¥" + fmtCompact(v)),
      },
      legend: {
        data: cats,
        top: 0, right: 0,
        textStyle: { color: C.sub, fontSize: 11 },
        itemWidth: 14, itemHeight: 8,
      },
      xAxis: {
        type: "category", boundaryGap: false, data: xs,
        axisLine: { lineStyle: { color: C.border } },
        axisLabel: { color: C.sub, fontSize: 11 },
        axisTick: { show: false },
      },
      yAxis: {
        type: "value",
        splitLine: { lineStyle: { color: C.border, type: "dashed" } },
        axisLabel: { color: C.sub, fontSize: 11, formatter: (v) => fmtCompact(v) },
      },
      series: cats.map((cat) => {
        const color = colorOf(cat);
        return {
          name: cat,
          type: "line",
          smooth: true,
          showSymbol: false,
          connectNulls: true,
          lineStyle: { width: 2, color: color },
          itemStyle: { color: color },
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: color + "55" },
              { offset: 1, color: color + "00" },
            ]),
          },
          data: rawTrend.series[cat].slice(-n),
        };
      }),
    };
  }

  function renderLine() {
    const all = rawTrend.times;
    const keep = RANGE_POINTS[currentRange] === Infinity
      ? all.length
      : Math.min(RANGE_POINTS[currentRange], all.length);
    const xs = all.slice(-keep);
    const cats = Object.keys(rawTrend.series);
    if (!lineChart || !cats.length) return;
    lineChart.setOption(buildLineOption(xs, cats), { replaceMerge: ["series"] });
  }

  function initLine() {
    lineChart = echarts.init(document.getElementById("lineChart"), null, { renderer: "canvas" });
    lineChart.setOption(buildLineOption([], []));
  }

  /* =======================================================
     柱状图：最新 5 分钟窗口各品类 GMV
     ======================================================= */
  function buildBarOption(xs, vals) {
    return {
      backgroundColor: "transparent",
      grid: { left: 56, right: 12, top: 16, bottom: 28 },
      tooltip: {
        trigger: "axis",
        backgroundColor: C.surface, borderColor: C.border, textStyle: { color: C.text },
        valueFormatter: (v) => fmtMoney(v),
      },
      xAxis: {
        type: "category", data: xs,
        axisLine: { lineStyle: { color: C.border } },
        axisLabel: { color: C.sub, fontSize: 10 },
        axisTick: { show: false },
      },
      yAxis: {
        type: "value",
        splitLine: { lineStyle: { color: C.border, type: "dashed" } },
        axisLabel: { color: C.sub, fontSize: 11, formatter: (v) => fmtCompact(v) },
      },
      series: [{
        type: "bar",
        data: vals,
        barWidth: "52%",
        itemStyle: {
          borderRadius: [4, 4, 0, 0],
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: C.purple },
            { offset: 1, color: C.cyan },
          ]),
        },
      }],
    };
  }

  function renderBar() {
    if (!barChart || !liveCats.length) return;
    const pairs = liveCats.map((cat) => {
      const arr = rawTrend.series[cat];
      /* 找最后一个非空窗口值 */
      let last = null;
      for (let i = arr.length - 1; i >= 0; i--) {
        if (arr[i] != null) { last = arr[i]; break; }
      }
      return [cat, last == null ? 0 : last];
    }).sort((a, b) => b[1] - a[1]);
    barChart.setOption(
      buildBarOption(pairs.map((p) => p[0]), pairs.map((p) => p[1])),
      { replaceMerge: ["series"] }
    );
  }

  function initBar() {
    barChart = echarts.init(document.getElementById("barChart"), null, { renderer: "canvas" });
    barChart.setOption(buildBarOption([], []));
  }

  /* =======================================================
     成交榜：最新窗口 GMV + 相邻窗口涨跌幅
     ======================================================= */
  function renderRank() {
    const root = document.getElementById("rankTable");
    if (!liveCats.length) {
      root.innerHTML = '<div class="rank-row" style="justify-content:center;color:' + C.sub + '">等待窗口数据产出（约 1 分钟）…</div>';
      return;
    }
    const rows = liveCats.map((cat) => {
      const arr = rawTrend.series[cat];
      let last = null, prev = null;
      for (let i = arr.length - 1; i >= 0; i--) {
        if (arr[i] != null) {
          if (last == null) { last = arr[i]; }
          else { prev = arr[i]; break; }
        }
      }
      const trend = calcTrend(prev, last);
      return { name: cat, color: colorOf(cat), gmv: last == null ? 0 : last, trend: trend };
    });
    const total = rows.reduce((s, r) => s + r.gmv, 0);
    const sorted = [...rows].sort((a, b) => b.gmv - a.gmv);
    root.innerHTML = sorted
      .map((r, i) => {
        const share = total > 0 ? ((r.gmv / total) * 100).toFixed(1) : "0.0";
        const tHtml = r.trend == null
          ? '<span class="rank-trend">—</span>'
          : '<span class="rank-trend ' + (r.trend >= 0 ? "up" : "down") + '">' +
            (r.trend >= 0 ? "▲" : "▼") + Math.abs(r.trend).toFixed(1) + "%</span>";
        return (
          '<div class="rank-row">' +
          '<span class="rank-no">' + (i + 1) + "</span>" +
          '<span class="rank-cat"><span class="dot" style="color:' + r.color + ';background:' + r.color + '"></span>' +
          '<span class="name">' + r.name + "</span></span>" +
          '<span class="rank-gmv">' + fmtMoney(r.gmv) + "</span>" +
          tHtml +
          '<span class="rank-share"><i style="width:' + share + '%"></i></span>' +
          "</div>"
        );
      })
      .join("");
  }

  /* =======================================================
     事件流（真实驱动：基于 overview 订单/DAU 增量生成）
     ======================================================= */
  const stream = document.getElementById("eventStream");

  function pushEvent(type, color, text) {
    const now = new Date();
    const item = document.createElement("div");
    item.className = "event-item";
    item.style.setProperty("--ec", color);
    item.innerHTML =
      '<span class="event-dot"></span>' +
      '<span class="event-text"><b>' + type + "</b> <span class=\"e-cat\">· " + text + "</span></span>" +
      '<span class="event-time">' + pad(now.getHours()) + ":" + pad(now.getMinutes()) + ":" + pad(now.getSeconds()) + "</span>";
    stream.prepend(item);
    while (stream.childElementCount > 30) stream.removeChild(stream.lastElementChild);
  }

  const PAY_TEXTS = [
    (c) => "用户完成「" + c + "」订单支付",
    (c) => "「" + c + "」新订单支付成功",
    (c) => "「" + c + "」订单结算完成",
  ];

  function pickLiveCat() {
    if (!liveCats.length) return "未知品类";
    return liveCats[Math.floor(Math.random() * liveCats.length)];
  }

  function pushRealEvents(prev, curr) {
    const dOrders = curr.order_total - prev.order_total;
    if (dOrders > 0) {
      const n = Math.min(dOrders, 3);
      for (let i = 0; i < n; i++) {
        const cat = pickLiveCat();
        const tpl = PAY_TEXTS[Math.floor(Math.random() * PAY_TEXTS.length)];
        pushEvent("支付成功", C.green, tpl(cat));
      }
    }
    const dDau = curr.dau - prev.dau;
    if (dDau > 0) {
      pushEvent("新用户", C.cyan, dDau + " 位新用户进入今日活跃");
    }
  }

  /* =======================================================
     时钟 / 链路状态 / 范围切换 / 手动刷新
     ======================================================= */
  function tickClock() {
    const d = new Date();
    document.getElementById("clockTime").textContent =
      pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
    document.getElementById("clockDate").textContent =
      d.getFullYear() + "/" + pad(d.getMonth() + 1) + "/" + pad(d.getDate());
  }

  let online = null;
  function setOnline(ok) {
    if (ok === online) return;
    online = ok;
    const el = document.getElementById("linkStatus");
    el.textContent = ok
      ? "Kafka · Spark · Redis · FastAPI 实时链路"
      : "后端不可用，正在重连…";
    el.style.color = ok ? "" : C.red;
  }

  function bindRange() {
    const sw = document.getElementById("rangeSwitcher");
    sw.addEventListener("click", (ev) => {
      const btn = ev.target.closest(".range-btn");
      if (!btn) return;
      sw.querySelectorAll(".range-btn").forEach((b) => {
        b.classList.remove("active");
        b.removeAttribute("aria-selected");
      });
      btn.classList.add("active");
      btn.setAttribute("aria-selected", "true");
      currentRange = btn.dataset.range;
      renderLine();
    });
  }

  function bindRefresh() {
    const btn = document.getElementById("refreshBtn");
    btn.addEventListener("click", async () => {
      const label = btn.querySelector(".deploy-label");
      const old = label.textContent;
      label.textContent = "刷新中…";
      btn.style.pointerEvents = "none";
      await refreshAll();
      label.textContent = "已更新 ✓";
      pushEvent("手动刷新", C.amber, "已从 Redis 拉取最新指标");
      setTimeout(() => {
        label.textContent = old;
        btn.style.pointerEvents = "";
      }, 1400);
    });
  }

  /* =======================================================
     数据刷新（轮询真实接口）
     ======================================================= */
  let lastOverview = null;

  async function refreshOverview() {
    try {
      const d = await fetchJSON(API + "/overview");
      const prev = lastOverview;
      kpiState.gmv = d.gmv_total;
      kpiState.orders = d.order_total;
      kpiState.dau = d.dau;
      kpiState.aov = d.order_total > 0 ? d.gmv_total / d.order_total : 0;
      kpiTrend.gmv = calcTrend(prev ? prev.gmv_total : null, d.gmv_total);
      kpiTrend.orders = calcTrend(prev ? prev.order_total : null, d.order_total);
      kpiTrend.dau = calcTrend(prev ? prev.dau : null, d.dau);
      kpiTrend.aov = calcTrend(
        prev && prev.order_total > 0 ? prev.gmv_total / prev.order_total : null,
        kpiState.aov
      );
      renderKpi(false);
      setOnline(true);
      if (prev) pushRealEvents(prev, d);
      lastOverview = d;
    } catch (e) {
      setOnline(false);
    }
  }

  async function refreshTrend() {
    try {
      const d = await fetchJSON(API + "/trend");
      /* 汇总全部窗口时间轴，各品类按时间对齐（缺窗补 null） */
      const timeSet = new Set();
      Object.values(d).forEach((v) => v.times.forEach((t) => timeSet.add(t)));
      const times = [...timeSet].sort();
      const series = {};
      Object.entries(d).forEach(([cat, v]) => {
        const m = new Map(v.times.map((t, i) => [t, v.values[i]]));
        series[cat] = times.map((t) => (m.has(t) ? m.get(t) : null));
      });
      rawTrend = { times, series };
      liveCats = Object.keys(series);
      renderLine();
      renderBar();
      renderRank();
    } catch (e) {
      /* 静默：连接状态由 overview 轮询负责提示 */
    }
  }

  async function refreshAll() {
    await Promise.all([refreshOverview(), refreshTrend()]);
  }

  /* =======================================================
     初始化 & 实时循环
     ======================================================= */
  function init() {
    buildKpi();
    initLine();
    initBar();
    renderRank();
    bindRange();
    bindRefresh();
    tickClock();
    pushEvent("系统", C.sub, "大屏已启动，等待实时数据流入…");

    refreshAll();
    setInterval(tickClock, 1000);
    setInterval(refreshOverview, 5000);   /* KPI 5 秒一刷（与指标写入节奏匹配） */
    setInterval(refreshTrend, 15000);     /* 窗口 1 分钟一滚动，15 秒轮询足够灵敏 */

    window.addEventListener("resize", () => {
      lineChart && lineChart.resize();
      barChart && barChart.resize();
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();