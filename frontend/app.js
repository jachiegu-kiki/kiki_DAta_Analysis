/* ════════════════════════════════════════════════════════════════════
   app.js · 入口文件（v6 · 2026-04-30 拆分後）
   ────────────────────────────────────────────────────────────────────
   依賴鏈（必須按 index.html 中的順序載入）：
     1) core.js     — 全局狀態 / AUTH / utils
     2) filters.js  — 篩選器引擎 / 級聯 / 移動端手風琴
     3) render.js   — 各看板渲染函數
     4) app.js      — 本檔，負責「取數」+「ETL 觸發」+「啟動」+「Tooltip 移動端點擊」

   第一性原理：
   - 拆分以「修改頻率 × 概念內聚度」為標尺，不以行數平均切。
   - 所有函數保持頂層全局，因 HTML onclick="xxx()" 直接調用，不引入 module
     既保留零構建（pure static）的部署形態，也避免一次性大改造。
   ════════════════════════════════════════════════════════════════════ */


/* ═══ FETCH — 核心：支持多维度筛选 ═══ */
async function fetchReport(execDate, filterParams) {
  var url = API + '/api/v1/dashboard/daily-report';
  var qs = [];
  if (execDate) qs.push('execution_date=' + execDate);
  var fp = filterParams || {};
  if (fp.advisors && fp.advisors.length) qs.push('advisors=' + encodeURIComponent(fp.advisors.join(',')));
  if (fp.filter_line && fp.filter_line.length) qs.push('filter_line=' + encodeURIComponent(fp.filter_line.join(',')));
  if (fp.filter_sub_line && fp.filter_sub_line.length) qs.push('filter_sub_line=' + encodeURIComponent(fp.filter_sub_line.join(',')));
  if (fp.filter_group_sys && fp.filter_group_sys.length) qs.push('filter_group_sys=' + encodeURIComponent(fp.filter_group_sys.join(',')));
  if (fp.filter_biz_block && fp.filter_biz_block.length) qs.push('filter_biz_block=' + encodeURIComponent(fp.filter_biz_block.join(',')));
  if (fp.filter_group_l1 && fp.filter_group_l1.length) qs.push('filter_group_l1=' + encodeURIComponent(fp.filter_group_l1.join(',')));
  if (fp.filter_group_advisor && fp.filter_group_advisor.length) qs.push('filter_group_advisor=' + encodeURIComponent(fp.filter_group_advisor.join(',')));
  if (fp.filter_biz_type && fp.filter_biz_type.length) qs.push('filter_biz_type=' + encodeURIComponent(fp.filter_biz_type.join(',')));
  if (qs.length) url += '?' + qs.join('&');

  try {
    var res = await fetch(url, { credentials: 'same-origin' });
    if (res.status === 401) { gotoLogin(); return; }
    var data = await res.json();
    data.potential = data.potential || data.fund_warning || {total_unarchived:0, total_unconfirmed:0, departments:[]};
    (data.potential.departments || []).forEach(function(d) { if (!d.contracts) d.contracts = []; });

    RAW = data;

    // 首次加载：保存元数据
    if (!origExecDate) {
      origExecDate = data.header.execution_date;
      ADL = data.advisor_dept_links || {};
      ALL_DEPTS = data.all_depts || [];
      ALL_ADVS = data.all_advisors || [];
      FO = data.filter_options || {};
      buildOptions();
    }

    $('dash').style.display = 'block';
    $('demo-text').textContent = '数据来源：FastAPI + PostgreSQL · 报表日期 ' + data.header.execution_date;
    renderAll();
  } catch(e) { console.error('fetch error:', e); }
}


/* ═══ ETL 觸發（v4：require_system_admin） ═══ */
async function fetchEtlStatus() {
  try {
    var res = await fetch(API + '/api/v1/etl/status', { credentials: 'same-origin' });
    if (!res.ok) return null;
    return await res.json();
  } catch(e) { return null; }
}

async function triggerETL() {
  var btn = $('btn-etl'), txt = $('etl-txt');
  if (btn.classList.contains('running')) return;
  // v4: 前端第二道防線（後端 require_system_admin 才是真實權限）
  if (!CU || !CU.is_system_admin) {
    alert('此操作僅限系統管理員');
    return;
  }

  // ─── 1. 預檢後端狀態 ───
  var status = await fetchEtlStatus();
  if (status && status.running) {
    var startedSh = fmtShanghai(status.started_at) || '未知時間';
    var by = status.triggered_by ? '\n觸發人：' + status.triggered_by : '';
    alert('⏳ ETL 正在後端執行中，請勿重複觸發。\n\n本次開始時間：' + startedSh + by
        + '\n\n（請等待當前任務結束後再操作）');
    return;
  }

  // ─── 2. 顯示上次完成時間 + 二次確認 ───
  var lastSh = status ? fmtShanghai(status.completed_at) : null;
  var lastLine;
  if (lastSh) {
    var statusTag = '';
    if (status && status.last_status === 'failed')  statusTag = '（上次失敗）';
    if (status && status.last_status === 'timeout') statusTag = '（上次超時）';
    var elapsed = (status && status.elapsed_seconds != null)
                ? '，耗時 ' + Number(status.elapsed_seconds).toFixed(1) + ' 秒' : '';
    lastLine = '上次成功完成：' + lastSh + elapsed + statusTag;
  } else if (status && status.last_status && status.last_status !== 'success') {
    var lastErr = status.last_error ? '\n失敗原因：' + status.last_error.slice(0, 200) : '';
    lastLine = '尚無成功完成記錄（上次狀態：' + status.last_status + '）' + lastErr;
  } else {
    lastLine = '尚無同步記錄（首次執行）';
  }

  if (!confirm('确认执行数据同步（ETL）？\n\n' + lastLine
             + '\n\n此操作将从 Excel 数据源重新清洗并写入数据库，'
             + '預計耗時 1-5 分鐘，期間請勿關閉頁面或重複點擊。')) {
    return;
  }

  // ─── 3. 執行 ───
  btn.classList.add('running');
  txt.textContent = '同步中...';
  try {
    var res = await fetch(API + '/api/v1/etl/trigger', {
      method: 'POST',
      credentials: 'same-origin'
    });
    var data = await res.json();
    if (res.ok) {
      txt.textContent = '✓ 完成';
      setTimeout(function(){ txt.textContent = '同步数据'; btn.classList.remove('running'); }, 2500);
      await fetchReport(origExecDate);
    } else if (res.status === 409) {
      // v4: 競態防御 — 預檢後到 POST 之間有人搶先觸發
      var d = data && data.detail;
      var msg = '⏳ ETL 已被其他請求觸發，請稍後重試';
      if (d && typeof d === 'object' && d.started_at) {
        msg += '\n\n本次開始時間：' + (fmtShanghai(d.started_at) || d.started_at);
        if (d.triggered_by) msg += '\n觸發人：' + d.triggered_by;
      }
      alert(msg);
      txt.textContent = '同步数据'; btn.classList.remove('running');
    } else {
      var detail = data && data.detail;
      var errMsg = (typeof detail === 'string') ? detail
                 : (detail && detail.message)   ? detail.message
                 : '未知错误';
      alert('ETL 执行失败: ' + errMsg);
      txt.textContent = '同步数据'; btn.classList.remove('running');
    }
  } catch(e) {
    alert('网络错误，请检查服务状态');
    txt.textContent = '同步数据'; btn.classList.remove('running');
  }
}


/* ═══ v6 移動端 Tooltip 點擊支持
   第一性原理：CSS :hover 在 touch 設備行為不可靠（首次 tap 觸發 hover，再 tap 才觸發 click）。
   逆向思維：與其讓桌面/移動共用一套交互，不如讓桌面繼續用 :hover（CSS 已實現），
            移動端疊加一層極輕的 JS 切換 .tooltip-open class（CSS 中對該 class 應用同 hover 樣式）。
   奧卡姆剃刀：不檢測設備類型；桌面 tap 也能 toggle，但因有 :hover 已展示，雙保險不衝突。
   ═══ */
function setupTooltips() {
  var tags = document.querySelectorAll('.sec-tag[data-tooltip]');
  for (var i = 0; i < tags.length; i++) {
    tags[i].addEventListener('click', function(e) {
      e.stopPropagation();
      var alreadyOpen = this.classList.contains('tooltip-open');
      // 先關閉所有，再切換當前 — 同一時刻最多一個展開
      var all = document.querySelectorAll('.sec-tag.tooltip-open');
      for (var j = 0; j < all.length; j++) all[j].classList.remove('tooltip-open');
      if (!alreadyOpen) this.classList.add('tooltip-open');
    });
  }
  // 點擊頁面其他地方關閉
  document.addEventListener('click', function() {
    var all = document.querySelectorAll('.sec-tag.tooltip-open');
    for (var j = 0; j < all.length; j++) all[j].classList.remove('tooltip-open');
  });
}


/* ═══ 啟動：先拿當前用戶，再載入報表 ═══ */
(async function init() {
  CU = await loadMe();
  if (!CU) return;  // 401 已被 loadMe 跳轉處理
  // 角色網關：ADVISOR → body 加 class，CSS 自動隱藏潛在簽約 / 顧問排行 / 鎖顧問篩選器
  if (CU.role === 'ADVISOR') {
    document.body.classList.add('role-advisor');
  }
  // v4: 非系统管理员（gw_role != 'admin'）隐藏『同步数据』按钮
  // 后端 /api/v1/etl/trigger 仍独立做 require_system_admin 防御（defense-in-depth）
  if (!CU.is_system_admin) {
    var etlBtn = document.getElementById('btn-etl');
    if (etlBtn) etlBtn.style.display = 'none';
  }
  // v5.1 手机端 UX：将筛选器重组为手风琴结构（桌面端不生效）
  setupMobileAccordion();
  // v6: Tooltip 移動端點擊支持
  setupTooltips();
  await fetchReport();
})();
